'use strict';

let _ = require('lodash');
let Exception = require('sq-toolkit/exception');

const IdGenerator = require('./lib/id-generator');
const BigQueryDataset = require('./dataset');

const BigQueryTableConst = require('./lib/constants/table');
const Error = require('./lib/constants/error');

class BigQueryStorage {

    /**
     * @typedef {Object} BigQueryStorageOptions
     * @property {String}                  projectId
     * @property {String}                  datasetName
     * @property {String}                  tableName
     *
     * @property {String|undefined}        keyFilename
     * @property {String|undefined}        clientEmail
     * @property {String|undefined}        privateKey
     *
     * @property {WinstonLogger|undefined} logger
     * @property {Number|undefined}        loggerStart
     * @property {Number|undefined}        loggerEach
     *
     * @property {Boolean|undefined}       bufferEnabled
     * @property {Number|undefined}        bufferMaxItems
     * @property {Number|undefined}        bufferMaxTime
     */

    /**
     * @param {BigQueryStorageOptions} opts
     */
    constructor(opts) {
        this.projectId      = opts.projectId;
        this.datasetName    = opts.datasetName;
        this.tableName      = opts.tableName;

        this.keyFilename    = opts.keyFilename || null;
        this.clientEmail    = opts.clientEmail || null;
        this.privateKey     = opts.privateKey  || null;

        this.logger         = _.get(opts, 'logger', null);
        this.loggerStart    = _.get(opts, 'loggerStart', null);
        this.loggerEach     = _.get(opts, 'loggerEach', null);

        this.bufferEnabled  = _.get(opts, 'bufferEnabled', false);
        this.bufferMaxItems = _.get(opts, 'bufferMaxItems', null);
        this.bufferMaxTime  = _.get(opts, 'bufferMaxTime', null);

        this.dataset = null;
        this.table   = null;

        this._init();
    }

    /**
     * @private
     */
    _init() {

        let opts = {
            projectId: this.projectId,

            keyFilename: this.keyFilename,
            clientEmail: this.clientEmail,
            privateKey: this.privateKey,

            logger: this.logger,
            loggerStart: this.loggerStart,
            loggerEach: this.loggerEach,

            bufferEnabled: this.bufferEnabled,
            bufferMaxItems: this.bufferMaxItems,
            bufferMaxTime: this.bufferMaxTime
        };

        this.dataset = BigQueryDataset.getDataset(this.datasetName, opts);
        this.table   = this.dataset.getTable(this.tableName);
    }

    /**
     * @return {Object}
     */
    getSchema() {
        throw new Exception(Exception.ErrorCode.ERROR_NOT_IMPLEMENTED);
    }

    /**
     * @param {Object} item
     * @return {Object}
     */
    getInsertData(item) {
        throw new Exception(Exception.ErrorCode.ERROR_NOT_IMPLEMENTED);
    }

    /**
     * @param {Object|Object[]} rows
     * @return {Promise|null}
     */
    insert(rows) {
        let data = Array.isArray(rows) === true ? rows.map(this.mapRow) : this.mapRow(rows);
        return this.table.insert(data);
    }

    mapRow(row) {
        let insertData = this.getInsertData(row);
        row._insertId = row._insertId || IdGenerator.generateInsertId(); // uuid
        insertData.insertId = row._insertId;
        return insertData;
    }
}

BigQueryStorage.Schema    = BigQueryTableConst.Schema;
BigQueryStorage.ErrorCode = Error;

module.exports = BigQueryStorage;
