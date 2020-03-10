'use strict';

const BufferQueue = require('sq-toolkit/buffer-queue');
const Exception = require('sq-toolkit/exception');
const Error = require('./constants/error');

class BqBufferQueue extends BufferQueue {

    /**
     * @param {Object}         opts
     * @param {String}         opts.name         Name to be used on alerter
     * @param {Function}       opts.iterator     Function to be iterated
     * @param {Number=}        opts.maxItems     Max count of items that can have until flush
     * @param {Number=}        opts.maxTime      Max time in ms that items can live in the buffer until flush
     * @param {Boolean=}       opts.itemPromises
     * @param {WinstonLogger=} opts.logger
     * @param {Number=}        opts.loggerStart
     * @param {Number=}        opts.loggerEach
     */
    constructor(opts) {
        super(opts);
    }

    /**
     * Re declaring flush method to handle bq errors
     * @return {Promise}
     */
    flush() {
        let pairs = this.queue.splice(0, this.maxItems);
        let items;
        if (this.itemPromises === true) {
            items = new Array(pairs.length);
            for (let i = 0; i < pairs.length; i++) {
                items[i] = pairs[i][0];
            }
        }
        else {
            items = pairs;
        }

        let promise = this.iterator(items);
        return this.itemPromises !== true ? this._processFlushIteratorWithoutItemPromises(promise) : this._processFlushIteratorWithItemPromises(promise, pairs);
    }

    _processFlushIteratorWithoutItemPromises(promise) {
        return promise
        .catch(err => {
            if (this.logger != null) {
                this.logger.notify(this.name + ' | BufferQueue Error').steps(this.loggerStart, this.loggerEach).msg('buffer-queue.js:: Error at %s Buffer. Error: ', this.name, err);
            }
        })
        .finally(() => {
            this.notify();
        });
    }

    _processFlushIteratorWithItemPromises(promise, pairs) {
        return promise
        .then(response => {
            let failedTasks = null;
            let errors = {};
            if(response.insertErrors && response.insertErrors.length > 0) {
                failedTasks = response.insertErrors.map(error => {
                    errors[error.index] = error.errors;
                    return error.index;
                });
            }
            for (let i = 0; i < pairs.length; i++) {
                if(failedTasks && failedTasks.includes(i)){
                    let errorArr = errors[i];
                    if(errorArr.length > 1) {
                        this.logger.notify(this.name + ' | BqBufferQueue Error').steps(this.loggerStart, this.loggerEach).msg('buffer-queue.js:: Error at %s Buffer. BqTask returned multiple errors. Errors: ', this.name, JSON.stringify(errorArr));
                    }
                    pairs[i][1].forceReject(new Exception(Error.BQ_INSERT_ERROR, `${errorArr[0].reason}: ${errorArr[0].message}`));
                } else {
                    pairs[i][1].forceResolve();
                }
            }
        })
        .catch(err => {
            for (let i = 0; i < pairs.length; i++) {
                pairs[i][1].forceReject(err);
            }
        });
    }
}

module.exports = BqBufferQueue;
