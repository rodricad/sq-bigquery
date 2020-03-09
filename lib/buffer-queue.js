'use strict';

const map = require('lodash/map');
const BufferQueue = require('sq-toolkit/buffer-queue');

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
        let items = null;

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

        if (this.itemPromises !== true) {
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

        return promise
        .then(response => {
            let failedTasks = null;
            if(response.insertErrors && response.insertErrors.length > 0) {
                failedTasks = map(response.insertErrors, error => {
                    return error.index;
                });
            }
            for (let i = 0; i < pairs.length; i++) {
                if(failedTasks && failedTasks.includes(i)){
                    pairs[i][1].forceReject();
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
