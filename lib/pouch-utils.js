'use strict';

/* istanbul ignore next */
import pouchdbPromise from 'pouchdb-promise';
import inheritsDefault from 'inherits';
import md5 from 'md5';
import pouchdbExtend from 'pouchdb-extend';

const Promise = (pouchdbPromise && pouchdbPromise.default) ? pouchdbPromise.default : pouchdbPromise;

/* istanbul ignore next */
export function getArguments(fun) {
  return function () {
    var len = arguments.length;
    var args = new Array(len);
    var i = -1;
    while (++i < len) {
      args[i] = arguments[i];
    }
    return fun.call(this, args);
  };
}

/* istanbul ignore next */
export function once(fun) {
  var called = false;
  return getArguments(function (args) {
    if (called) {
      console.trace();
      throw new Error('once called  more than once');
    } else {
      called = true;
      fun.apply(this, args);
    }
  });
}

/* istanbul ignore next */
export function toPromise(func) {
  // create the function we will be returning
  return getArguments(function (args) {
    var self = this;
    var tempCB = (typeof args[args.length - 1] === 'function') ? args.pop() : false;
    // if the last argument is a function, assume its a callback
    var usedCB;
    if (tempCB) {
      // if it was a callback, create a new callback which calls it,
      // but do so async so we don't trap any errors
      usedCB = function (err, resp) {
        process.nextTick(function () {
          tempCB(err, resp);
        });
      };
    }
    var promise = new Promise(function (fulfill, reject) {
      try {
        var callback = once(function (err, mesg) {
          if (err) {
            reject(err);
          } else {
            fulfill(mesg);
          }
        });
        // create a callback for this invocation
        // apply the function in the orig context
        args.push(callback);
        func.apply(self, args);
      } catch (e) {
        reject(e);
      }
    });
    // if there is a callback, call it back
    if (usedCB) {
      promise.then(function (result) {
        usedCB(null, result);
      }, usedCB);
    }
    promise.cancel = function () {
      return this;
    };
    return promise;
  });
}

export const inherits = inheritsDefault;
export { Promise as PromiseLib }; // named export as PromiseLib to avoid shadowing global Promise
export const MD5 = md5;
export const extend = pouchdbExtend;
