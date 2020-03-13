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
        if(this.queue.length === 0) {
            this.clearFlushInterval(); // prevents unnecessary flush when flushing due to full queue
        }
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
        if(this.itemPromises !== true){
            return this._processFlushIteratorWithoutItemPromises(promise);
        } else {
            return this._processFlushIteratorWithItemPromises(promise, pairs);
        }
    }

    _processFlushIteratorWithoutItemPromises(promise) {
        return promise
        .catch(err => {
            if (this.logger != null) {
                this.logger.notify(this.name + ' | BqBufferQueue Error').steps(this.loggerStart, this.loggerEach).msg('bq-buffer-queue.js:: Error at %s Buffer. Error: ', this.name, err);
            }
        })
        .finally(() => {
            this.notify();
        });
    }

    _processFlushIteratorWithItemPromises(promise, pairs) {
        return promise
        .then(response => {
            let errors = {};
            if(response.insertErrors && response.insertErrors.length > 0) {
                response.insertErrors.forEach(error => {
                    errors[error.index] = error.errors;
                });
            }
            for (let i = 0; i < pairs.length; i++) {
                let errorArr = errors[i];
                if(errorArr != null){
                    if(errorArr.length > 1) {
                        this.logger.notify(this.name + ' | BqBufferQueue Error').steps(this.loggerStart, this.loggerEach).msg('bq-buffer-queue.js:: Error at %s Buffer. BqTask returned multiple errors. Errors: ', this.name, JSON.stringify(errorArr));
                    }
                    pairs[i][1].__fulfilled = true;
                    pairs[i][1].forceReject(new Exception(Error.BQ_INSERT_ERROR, errorArr.length > 0 ? `${errorArr[0].reason}: ${errorArr[0].message}` : `Error inserting into bigQuery: ${JSON.stringify(errorArr)}`));
                } else {
                    pairs[i][1].__fulfilled = true;
                    pairs[i][1].forceResolve();
                }
            }
        })
        .catch(err => {
            for (let i = 0; i < pairs.length; i++) {
                if(pairs[i][1].__fulfilled !== true) {
                    pairs[i][1].forceReject(err);
                }
            }
        });
    }
}

module.exports = BqBufferQueue;
