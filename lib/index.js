'use strict';

export * from 'pouchdb-mapreduce-no-ddocs'; // re-export everything the original file put on exports

import * as mapReduceAll from 'pouchdb-mapreduce-no-ddocs';
import { toIndexableString } from 'pouchdb-collate';
import * as utils from './pouch-utils';
import lunr from 'lunr';
// var uniq = require('uniq'); // unused in file
const Promise = utils.PromiseLib;
import stringify from 'json-stable-stringify';

const indexes = {};

const TYPE_TOKEN_COUNT = 'a';
const TYPE_DOC_INFO = 'b';

function add(left, right) {
  return left + right;
}

// get all the tokens found in the given text (non-unique)
function getTokenStream(text, pipeline, is_query = false) {
  // Lunr 0.x -> 2.x Tokens are now instances of lunr.Token, not just strings,
  // so the uniq call on this array was not really working.
  let trimmer;
  const stemmers = [];
  const stop_word_filters = [];
  for (let i = 0, len = pipeline._stack.length; i < len; i++) {
    if (pipeline._stack[i].label.indexOf('trimmer') >= 0) {
      trimmer = pipeline._stack[i];
    } else if (pipeline._stack[i].label.indexOf('stemmer') >= 0) {
      stemmers.push(pipeline._stack[i]);
    } else if (pipeline._stack[i].label.indexOf('stopWordFilter') >= 0) {
      stop_word_filters.push(pipeline._stack[i]);
    }
  }

  const trimmed_tokens = lunr.tokenizer(text).map(function (token) {
    // Don't modify query tokens with an asterisk (*).
    if (is_query && token.toString().indexOf('*') >= 0) {
      return token.toString();
    } else {
      // Trim and remove stop words.
      trimmer(token);
      for (let i = 0, len = stop_word_filters.length; i < len; i++) {
        stop_word_filters[i](token);
      }
      return token.toString();
    }
  });

  const results_2 = trimmed_tokens.map(function (s) {
    const token = new lunr.Token(s);
    for (let i = 0, len = stemmers.length; i < len; i++) {
      stemmers[i](token);
    }
    return token.toString();
  });

  if (!is_query) {
    // Combine with non-stemmed tokens (don't duplicate).
    for (let i = 0, len = trimmed_tokens.length; i < len; i++) {
      if (results_2.indexOf(trimmed_tokens[i]) === -1) {
        results_2.push(trimmed_tokens[i]);
      }
    }
  }

  return results_2;
}

// given an object containing the field name and/or
// a deepField definition plus the doc, return the text for
// indexing
function getText(fieldBoost, doc) {
  if (fieldBoost.getText) return fieldBoost.getText(doc);
  let text;
  if (!fieldBoost.deepField) {
    text = doc[fieldBoost.field];
  } else { // "Enhance."
    text = doc;
    for (let i = 0, len = fieldBoost.deepField.length; i < len; i++) {
      if (Array.isArray(text)) {
        text = text.map(handleNestedObjectArrayItem(fieldBoost, fieldBoost.deepField.slice(i)));
      } else {
        text = text && text[fieldBoost.deepField[i]];
      }
    }
  }
  if (text) {
    if (Array.isArray(text)) {
      text = text.join(' ');
    } else if (typeof text !== 'string') {
      text = text.toString();
    }
  }
  return text;
}

function handleNestedObjectArrayItem(fieldBoost, deepField) {
  return function (one) {
    return getText(utils.extend({}, fieldBoost, {
      deepField: deepField
    }), one);
  };
}

// map function that gets passed to map/reduce
// emits two types of key/values - one for each token
// and one for the field-len-norm
function createMapFunction(fieldBoosts, index, filter, db) {
  return function (doc, emit) {

    if (isFiltered(doc, filter, db)) {
      return;
    }

    const docInfo = [];

    for (let i = 0, len = fieldBoosts.length; i < len; i++) {
      const fieldBoost = fieldBoosts[i];

      const text = fieldBoost.getText ? fieldBoost.getText(doc) : getText(fieldBoost, doc);

      let fieldLenNorm;
      if (text) {
        const terms = getTokenStream(text, index.pipeline);
        for (let j = 0, jLen = terms.length; j < jLen; j++) {
          const term = terms[j];
          // avoid emitting the value if there's only one field;
          // it takes up unnecessary space on disk
          const value = fieldBoosts.length > 1 ? i : undefined;
          emit(TYPE_TOKEN_COUNT + term, value);
        }
        fieldLenNorm = Math.sqrt(terms.length);
      } else { // no tokens
        fieldLenNorm = 0;
      }
      docInfo.push(fieldLenNorm);
    }

    emit(TYPE_DOC_INFO + doc._id, docInfo);
  };
}

export const search = utils.toPromise(function (opts, callback) {
  const pouch = this;
  opts = utils.extend(true, {}, opts);
  const q = opts.query || opts.q;
  const mm = 'mm' in opts ? (parseFloat(opts.mm) / 100) : 1; // e.g. '75%'
  let fields = opts.fields;
  const highlighting = opts.highlighting;
  const includeDocs = opts.include_docs;
  const destroy = opts.destroy;
  const stale = opts.stale;
  const limit = opts.limit;
  const build = opts.build;
  const lunrOptions = build ? opts.lunrOptions : null;
  const skip = opts.skip || 0;
  const language = opts.language || 'en';
  const filter = opts.filter;
  const getText = opts.getText || {};

  if (Array.isArray(fields)) {
    const fieldsMap = {};
    fields.forEach(function (field) {
      fieldsMap[field] = 1; // default boost
    });
    fields = fieldsMap;
  }

  const fieldBoosts = Object.keys(fields).map(function (field) {
    const deepField = field.indexOf('.') !== -1 && field.split('.');
    return {
      field: field,
      getText: getText[field],
      deepField: deepField,
      boost: fields[field]
    };
  });

  let index = indexes[language];
  let indexPipeline;
  if (!index) {
    index = indexes[language] = lunr(function() {
      /* istanbul ignore next */
      lunrOptions && lunrOptions.bind(this)(lunr);
      indexPipeline = this.pipeline;
      if (Array.isArray(language)) {
        this.use(lunr['multiLanguage'].apply(this, language));
      } else if (language !== 'en') {
        this.use(lunr[language]);
      }
    });
    index.searchPipeline = indexPipeline; //index.pipeline;
    index.pipeline = indexPipeline;
  }

  // the index we save as a separate database is uniquely identified
  // by the fields the user want to index (boost doesn't matter)
  // plus the tokenizer

  const indexParams = {
    language: language,
    fields: fieldBoosts.map(function (x) {
      return x.field;
    }).sort()
  };

  const persistedIndexName = 'search-' + utils.MD5(stringify(indexParams));

  const mapFun = createMapFunction(fieldBoosts, index, filter, pouch);

  const queryOpts = {
    saveAs: persistedIndexName
  };
  if (destroy) {
    queryOpts.destroy = true;
    return pouch.query(mapFun, queryOpts, callback);
  } else if (build) {
    delete queryOpts.stale; // update immediately
    queryOpts.limit = 0;
    pouch.query(mapFun, queryOpts).then(function () {
      callback(null, {ok: true});
    }).catch(callback);
    return;
  }

  // it shouldn't matter if the user types the same
  // token more than once, in fact I think even Lucene does this
  // special cases like boingo boingo and mother mother are rare
  const queryTerms = Array.from(new Set(getTokenStream(q, index.searchPipeline, true)));
  if (!queryTerms.length) {
    return callback(null, {total_rows: 0, rows: []});
  }
  queryOpts.keys = queryTerms.map(function (queryTerm) {
    return TYPE_TOKEN_COUNT + queryTerm;
  });

  if (typeof stale === 'string') {
    queryOpts.stale = stale;
  }

  // Wildcards
  const wildcardTerms = queryTerms.filter(function(queryTerm) {
    const sections = queryTerm.split('*');
    // Term needs to contain something other than '*'.
    return sections.length > 1 && sections.filter(function(s) {
      return s.length > 0;
    }).length > 0;
  });
  const hasWildCard = wildcardTerms.length > 0;

  if (hasWildCard) {
    let total_rows = 0;
    const docIdsToFieldsToQueryTerms = {};
    delete queryOpts.keys;
    // For v0 let's not bother setting a limit.
    return pouch.query(mapFun, queryOpts).then(function (res) {
      return res.rows.filter(function(d) {
        const text = d.key.substring(1),
              term = wildcardTerms[0],
              sections = term.split("*");
        return matchWildcard(term, sections, text);
      });
    // Copied from step 3 below.
    }).then(function (rows) {
      total_rows = rows.length;
      // filter before fetching docs or applying highlighting
      // for a slight optimization, since for now we've only fetched ids/scores
      return (typeof limit === 'number' && limit >= 0) ?
        rows.slice(skip, skip + limit) : skip > 0 ? rows.slice(skip) : rows;
    }).then(function (rows) {
      if (includeDocs) {
        return applyIncludeDocs(pouch, rows);
      }
      return rows;
    }).then(function (rows) {
      if (highlighting) {
        return applyHighlighting(pouch, opts, rows, fieldBoosts, docIdsToFieldsToQueryTerms);
      }
      return rows;

    }).then(function (rows) {
      callback(null, {total_rows: total_rows, rows: rows});
    });
  }

  // search algorithm, basically classic TF-IDF
  let prefix_rows = [];
  pouch._search_query(mapFun, {
    startkey: TYPE_TOKEN_COUNT + queryTerms[queryTerms.length - 1],
    endkey: TYPE_TOKEN_COUNT + queryTerms[queryTerms.length - 1] + '\uffff',
    stale: queryOpts.stale,
    saveAs: queryOpts.saveAs
  }).then(function (res) {
    prefix_rows = res.rows;

    // step 1
    return pouch._search_query(mapFun, queryOpts);
  }).then(function (res) {

    // Only append prefix results that we don't already have results for.
    const foo = res.rows.map(function (row) {
      return row.key + row.id + row.value;
    });
    const bar = prefix_rows.map(function (row) {
      return TYPE_TOKEN_COUNT + queryTerms[queryTerms.length - 1] + row.id + row.value;
    });
    for (let i = 0, len = bar.length; i < len; i++) {
      if (foo.indexOf(bar[i]) === -1) {
        res.rows.push(prefix_rows[i]);
      }
    }

    if (!res.rows.length) {
      return callback(null, {total_rows: 0, rows: []});
    }
    let total_rows = 0;
    const docIdsToFieldsToQueryTerms = {};
    const termDFs = {};

    res.rows.forEach(function (row) {
      const term = row.key.substring(1);
      const field = row.value || 0;

      // calculate termDFs
      if (!(term in termDFs)) {
        termDFs[term] = 1;
      } else {
        termDFs[term]++;
      }

      // calculate docIdsToFieldsToQueryTerms
      if (!(row.id in docIdsToFieldsToQueryTerms)) {
        const arr = docIdsToFieldsToQueryTerms[row.id] = [];
        for (let i = 0; i < fieldBoosts.length; i++) {
          arr[i] = {};
        }
      }

      const docTerms = docIdsToFieldsToQueryTerms[row.id][field];
      if (!(term in docTerms)) {
        docTerms[term] = 1;
      } else {
        docTerms[term]++;
      }
    });

    // apply the minimum should match (mm)
    if (queryTerms.length > 1) {
      Object.keys(docIdsToFieldsToQueryTerms).forEach(function (docId) {
        const allMatchingTerms = {};
        const fieldsToQueryTerms = docIdsToFieldsToQueryTerms[docId];
        Object.keys(fieldsToQueryTerms).forEach(function (field) {
          Object.keys(fieldsToQueryTerms[field]).forEach(function (term) {
            allMatchingTerms[term] = true;
          });
        });
        const numMatchingTerms = Object.keys(allMatchingTerms).length;
        const matchingRatio = numMatchingTerms / queryTerms.length;
        if ((Math.floor(matchingRatio * 100) / 100) < mm) {
          delete docIdsToFieldsToQueryTerms[docId]; // ignore this doc
        }
      });
    }

    if (!Object.keys(docIdsToFieldsToQueryTerms).length) {
      return callback(null, {total_rows: 0, rows: []});
    }

    const keys = Object.keys(docIdsToFieldsToQueryTerms).map(function (docId) {
      return TYPE_DOC_INFO + docId;
    });

    const queryOpts2 = {
      saveAs: persistedIndexName,
      // We are going to bypass .query and use allDocs directly because .query is so slow.
      // The reason it's so slow is because it uses prefix search on each key individually,
      // which is not necessary for us because we know the entire key value.
      _raw_keys: true,
      // The key is serialized version of [TYPE_DOC_INFO + docId, docId]
      keys: keys.map(function(k) { return toIndexableString([k, k.substring(1)]); }),
      stale: stale
    };

    // step 2
    return pouch.query(mapFun, queryOpts2).then(function (res) {

      const docIdsToFieldsToNorms = {};
      res.rows.forEach(function (row) {
        if (!(row.id in docIdsToFieldsToNorms)) {
          docIdsToFieldsToNorms[row.id] = row.value || 0;
        }
      });
      // step 3
      // now we have all information, so calculate scores
      const rows = calculateDocumentScores(queryTerms, termDFs,
        docIdsToFieldsToQueryTerms, docIdsToFieldsToNorms, fieldBoosts);
      return rows;
    }).then(function (rows) {
      total_rows = rows.length;
      // filter before fetching docs or applying highlighting
      // for a slight optimization, since for now we've only fetched ids/scores
      return (typeof limit === 'number' && limit >= 0) ?
        rows.slice(skip, skip + limit) : skip > 0 ? rows.slice(skip) : rows;
    }).then(function (rows) {
      if (includeDocs) {
        return applyIncludeDocs(pouch, rows);
      }
      return rows;
    }).then(function (rows) {
      if (highlighting) {
        return applyHighlighting(pouch, opts, rows, fieldBoosts, docIdsToFieldsToQueryTerms);
      }
      return rows;

    }).then(function (rows) {
      callback(null, {total_rows: total_rows, rows: rows});
    });
  }).catch(callback);
});


// returns a sorted list of scored results, like:
// [{id: {...}, score: 0.2}, {id: {...}, score: 0.1}];
function calculateDocumentScores(queryTerms, termDFs, docIdsToFieldsToQueryTerms,
                                 docIdsToFieldsToNorms, fieldBoosts) {

  const results = Object.keys(docIdsToFieldsToQueryTerms).map(function (docId) {

    const fieldsToQueryTerms = docIdsToFieldsToQueryTerms[docId];
    const fieldsToNorms = docIdsToFieldsToNorms[docId];

    const queryScores = queryTerms.map(function (queryTerm) {
      return fieldsToQueryTerms.map(function (queryTermsToCounts, fieldIdx) {
        const fieldNorm = fieldsToNorms[fieldIdx];
        if (!(queryTerm in queryTermsToCounts)) {
          return 0;
        }
        const termDF = termDFs[queryTerm];
        const termTF = queryTermsToCounts[queryTerm];
        const docScore = termTF / termDF; // TF-IDF for doc
        const queryScore = 1 / termDF; // TF-IDF for query, count assumed to be 1
        const boost = fieldBoosts[fieldIdx].boost;
        return docScore * queryScore * boost / fieldNorm; // see cosine sim equation
      }).reduce(add, 0);
    });

    let maxQueryScore = 0;
    queryScores.forEach(function (queryScore) {
      if (queryScore > maxQueryScore) {
        maxQueryScore = queryScore;
      }
    });

    return {
      id: docId,
      score: maxQueryScore
    };
  });

  results.sort(function (a, b) {
    return a.score < b.score ? 1 : (a.score > b.score ? -1 : 0);
  });

  return results;
}

function applyIncludeDocs(pouch, rows) {
  return Promise.all(rows.map(function (row) {
    return pouch.get(row.id);
  })).then(function (docs) {
    docs.forEach(function (doc, i) {
      rows[i].doc = doc;
    });
  }).then(function () {
    return rows;
  });
}

// create a convenient object showing highlighting results
function applyHighlighting(pouch, opts, rows, fieldBoosts,
                           docIdsToFieldsToQueryTerms) {

  const pre = opts.highlighting_pre || '<strong>';
  const post = opts.highlighting_post || '</strong>';

  return Promise.all(rows.map(function (row) {

    return Promise.resolve().then(function () {
      if (row.doc) {
        return row.doc;
      }
      return pouch.get(row.id);
    }).then(function (doc) {
      row.highlighting = {};
      docIdsToFieldsToQueryTerms[row.id].forEach(function (queryTerms, i) {
        const fieldBoost = fieldBoosts[i];
        const fieldName = fieldBoost.field;
        let text = getText(fieldBoost, doc);
        Object.keys(queryTerms).forEach(function (queryTerm) {
          const regex = new RegExp('(' + queryTerm + '[a-z]*)', 'gi');
          const replacement = pre + '$1' + post;
          text = text.replace(regex, replacement);
          row.highlighting[fieldName] = text;
        });
      });
    });
  })).then(function () {
    return rows;
  });
}

// return true if filtered, false otherwise
function isFiltered(doc, filter, db) {
  try {
    return !!(filter && !filter(doc));
  } catch (e) {
    db.emit('error', e);
    return true;
  }
}

function matchWildcard(term, sections, text) {
  // Supports *oobar foo*ar fooba*
  if (term[0] === '*' && term[term.length - 1] === '*' && sections.length === 3) {
    const matchable = sections[1];
    let hasMatch = false;
    for (let i = 0, len = text.length - matchable.length; i < len; i++) {
      if (text.substring(i, i + matchable.length) === matchable) {
        hasMatch = true;
        break;
      }
    }
    return hasMatch;
  } else if (sections.length > 2) {
    return false;
  } else {
    const front = sections[0];
    const back  = sections[1];
    const matchFront = text.substring(0, front.length) === front;
    const matchBack = text.substring(text.length - back.length) === back;
    return matchFront && matchBack;
  }
}
