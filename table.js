'use strict';

let _ = require('lodash');
let uuid = require('uuid');
let Exception = require('sq-toolkit/exception');
let BufferQueue = require('sq-toolkit/buffer-queue');
let BigQuery = require('@google-cloud/bigquery').BigQuery;
let Promise = require('bluebird');
let BigQueryError = require('./error');

const BigQueryTableConst = require('./lib/constants/table');

class BigQueryTable {

    /**
     * @param {BigQuery.Table} table
     * @param {Object=}        opts
     * @param {WinstonLogger=} opts.logger
     * @param {Number=}        opts.loggerStart
     * @param {Number=}        opts.loggerEach
     * @param {Boolean=}       opts.bufferEnabled
     * @param {Number=}        opts.bufferMaxItems
     * @param {Number=}        opts.bufferMaxTime
     * @param {Boolean=}       opts.bufferItemPromises
     */
    constructor(table, opts) {
        this.name           = table.id;
        this.table          = table;

        this.logger         = _.get(opts, 'logger', null);
        this.loggerStart    = _.get(opts, 'loggerStart', null);
        this.loggerEach     = _.get(opts, 'loggerEach', null);

        this.bufferEnabled      = _.get(opts, 'bufferEnabled', false);
        this.bufferMaxItems     = _.get(opts, 'bufferMaxItems', null);
        this.bufferMaxTime      = _.get(opts, 'bufferMaxTime', null);
        this.bufferItemPromises = _.get(opts, 'bufferItemPromises', false);

        this.bufferQueue = null;
        this._init();
    }

    /**
     * @private
     */
    _init() {
        if (this.isBufferEnabled() === false) {
            return;
        }

        let opts = {
            name: this.name,
            iterator: this._insert.bind(this),

            logger: this.logger,
            loggerStart: this.loggerStart,
            loggerEach: this.loggerEach,

            maxItems: this.bufferMaxItems,
            maxTime: this.bufferMaxTime,
            itemPromises: this.bufferItemPromises
        };

        this.bufferQueue = new BufferQueue(opts);
    }

    /**
     * @return {Boolean}
     */
    isBufferEnabled() {
        return this.bufferEnabled === true;
    }

    /**
     * @return {Boolean}
     */
    isBufferItemPromisesEnabled() {
        return this.bufferItemPromises === true;
    }

    /**
     * @param {Object|Object[]} rows
     * @return {Promise|null}
     */
    insert(rows) {
        if (this.isBufferEnabled() === false) {
            return this._insert(rows);
        }

        if (Array.isArray(rows) === true) {
            if(this.isBufferItemPromisesEnabled() === true) {
                return Promise.map(rows, row => { // TODO: Create promise map util on sq-toolkit and replace bluebird
                    return this.bufferQueue.add(row);
                });
            }
            return this.bufferQueue.addMany(rows);
        }

        return this.bufferQueue.add(rows);
    }

    /**
     * @param {Object|Object[]} rows
     * @return {Promise.<Object>}
     * @private
     */
    _insert(rows) {
        let items   = BigQueryTable.getRawRows(rows);
        let options = {
            raw: true
        };

        return this.table.insert(items, options)
        .catch(BigQueryError.parseErrorAndThrow)
        .then(data => {
            return data[0];
        });
    }

    /**
     * @param {Object|Object[]} rows
     * @return {Object|Object[]}
     */
    static getRawRows(rows) {
        let items = Array.isArray(rows) === false ? [rows] : rows;

        return items.map(item => {
            return { insertId: BigQueryTable.getInsertId(), json: item };
        });
    }

    /**
     * @return {String}
     */
    /* istanbul ignore next */
    static getInsertId() {
        return uuid.v4();
    }

    /**
     * @param {Object} schema
     * @return {Object}
     */
    static parseSchema(schema) {
        let pairs  = _.entries(schema);
        let fields = [];

        for (let i = 0; i < pairs.length; i++) {
            let pair = pairs[i];
            let key  = pair[0];
            let type = pair[1];

            if (BigQueryTableConst.Schema.Type[type] == null) {
                throw new Exception(BigQueryTableConst.ErrorCode.INVALID_SCHEMA_TYPE);
            }

            fields.push({ name: key, type: type, mode: BigQueryTableConst.Schema.Mode.NULLABLE });
        }

        return { fields: fields };
    }
}

BigQueryTable.ErrorCode = BigQueryTableConst.ErrorCode;
BigQueryTable.Schema    = BigQueryTableConst.Schema;

module.exports = BigQueryTable;
