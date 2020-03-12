'use strict';

let _ = require('lodash');
let Exception = require('sq-toolkit/exception');
let BufferQueue = require('./lib/bq-buffer-queue');
let BigQuery = require('@google-cloud/bigquery').BigQuery;
let BigQueryError = require('./error');
let uuid = require('uuid');

const BigQueryTableConst = require('./lib/constants/table');
const Error = require('./lib/constants/error');

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

        this.bufferEnabled  = _.get(opts, 'bufferEnabled', false);
        this.bufferMaxItems = _.get(opts, 'bufferMaxItems', null);
        this.bufferMaxTime  = _.get(opts, 'bufferMaxTime', null);
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
     * @param {Object|Object[]} data
     * @return {Promise|null}
     */
    insert(data) {
        if (this.isBufferEnabled() === false) {
            data = Array.isArray(data) === false ? [data] : data;
            return this._insert(data);
        }
        if (Array.isArray(data) === true) {
            if(this.isBufferItemPromisesEnabled() === true) {
                throw new Exception(Error.ERROR_ADD_MANY_NOT_SUPPORTED, 'Can\'t insert multiple rows at once when bufferItemPromises is enabled.')
            }
            return this.bufferQueue.addMany(data);
        }
        return this.bufferQueue.add(data);
    }

    /**
     * @param {Object|Object[]} items
     * @return {Promise.<Object>}
     * @private
     */
    _insert(items) {
        let options = {
            raw: true
        };
        this.logger.debug(`sq-bigquery[${this.name}]:: Inserting ${items.length} rows`);
        return this.table.insert(items, options)
        .catch(BigQueryError.parseErrorAndThrow)
        .then(data => {
            return data[0];
        });
    }

    static getInsertId() {
        return uuid.v4();
    }

    /**
     * @param {Object|Object[]} rows
     * @return {Object|Object[]}
     */
    static getRawRows(rows) {
        let items = Array.isArray(rows) === false ? [rows] : rows;

        return items.map(item => {
            return BigQueryTable.getRawRow(item);
        });
    }

    static getRawRow(row, insertId) {
        if(insertId == null) {
            insertId = BigQueryTable.getInsertId();
        }
        return { insertId: insertId, json: row };
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
                throw new Exception(Error.INVALID_SCHEMA_TYPE);
            }

            fields.push({ name: key, type: type, mode: BigQueryTableConst.Schema.Mode.NULLABLE });
        }

        return { fields: fields };
    }
}

BigQueryTable.ErrorCode = Error;
BigQueryTable.Schema    = BigQueryTableConst.Schema;

module.exports = BigQueryTable;
