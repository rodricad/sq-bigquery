'use strict';

let _ = require('lodash');
let BigQuery = require('@google-cloud/bigquery').BigQuery;

let BigQueryTable = require('./table');
let BigQueryError = require('./error');
let BigQueryHelper = require('./helper');

class BigQueryDataset {

    /**
     * @param {BigQuery.Dataset} dataset
     * @param {Object=}          opts
     * @param {WinstonLogger=}   opts.logger
     * @param {Number=}          opts.loggerStart
     * @param {Number=}          opts.loggerEach
     * @param {Boolean=}         opts.bufferEnabled
     * @param {Number=}          opts.bufferMaxItems
     * @param {Number=}          opts.bufferMaxTime
     */
    constructor(dataset, opts) {
        this.name           = dataset.id;
        this.dataset        = dataset;

        this.logger         = _.get(opts, 'logger', null);
        this.loggerStart    = _.get(opts, 'loggerStart', null);
        this.loggerEach     = _.get(opts, 'loggerEach', null);

        this.bufferEnabled  = _.get(opts, 'bufferEnabled', false);
        this.bufferMaxItems = _.get(opts, 'bufferMaxItems', null);
        this.bufferMaxTime  = _.get(opts, 'bufferMaxTime', null);
    }

    /**
     * @return {Promise.<BigQueryDataset>}
     */
    init() {
        return this.dataset.get()
        .catch(BigQueryError.parseErrorAndThrow)
        .then(() => {
            return this;
        });
    }

    /**
     * @param {Object=} opts
     * @return {Object}
     */
    getOptions(opts) {
        let datasetOpts = {
            logger: this.logger,
            loggerStart: this.loggerStart,
            loggerEach: this.loggerEach,

            bufferEnabled: this.bufferEnabled,
            bufferMaxItems: this.bufferMaxItems,
            bufferMaxTime: this.bufferMaxTime
        };

        return _.defaults({}, opts, datasetOpts);
    }

    /**
     * @param {String}  tableName
     * @return {BigQueryTable}
     */
    getTable(tableName) {
        let options = this.getOptions();
        let table   = this.dataset.table(tableName);
        return new BigQueryTable(table, options);
    }

    /**
     * @param {String} tableName
     * @param {Object} schema
     * @return {Promise.<BigQueryTable>}
     */
    createTable(tableName, schema) {

        let parsedSchema = BigQueryTable.parseSchema(schema);

        return this.dataset.createTable(tableName, { schema: parsedSchema })
        .catch(BigQueryError.parseErrorAndThrow)
        .then((data) => {
            let options = this.getOptions();
            return new BigQueryTable(data[0], options);
        });
    }

    /**
     * @typedef {Object} DatasetOptions
     *
     * @property {String|undefined}        projectId
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
     * @param {String}         datasetName
     * @param {DatasetOptions} opts
     * @return {BigQueryDataset}
     */
    static getDataset(datasetName, opts) {
        let bigquery = BigQueryHelper.create(opts);
        let dataset  = bigquery.dataset(datasetName);

        return new BigQueryDataset(dataset, opts);
    }

    /**
     * @param {String}         datasetName
     * @param {DatasetOptions} opts
     * @return {Promise.<BigQueryDataset>}
     */
    static createDataset(datasetName, opts) {
        let bigquery = BigQueryHelper.create(opts);

        return bigquery.createDataset(datasetName)
        .catch(BigQueryError.parseErrorAndThrow)
        .then(data => {
            return new BigQueryDataset(data[0], opts);
        });
    }
}

module.exports = BigQueryDataset;
