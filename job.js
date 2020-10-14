'use strict';

const fs = require('fs-extra');
const _ = require('lodash');
const Error = require('./lib/constants/error');
const DummyLogger = require('sq-logger/dummy-logger');
const Duration = require('sq-toolkit/duration');
const PromiseTool = require('sq-toolkit/promise-native-tool');
const Exception = require('sq-toolkit/exception');
const Variables = require('sq-toolkit/variables');
const template = require('./lib/utils/template');
const BigQueryError = require('./error');
const BigQueryFactory = require('./factory');

const COST_THRESHOLD_IN_GB = 100;
const COST_PER_TB = 5;

const JOB_WAIT_INTERVAL_TEST = 10;
const JOB_WAIT_INTERVAL = 1000;

/**
 * Enum string values.
 * @enum {string}
 */
const WriteDisposition = {
    WRITE_APPEND: 'WRITE_APPEND',
    WRITE_EMPTY: 'WRITE_EMPTY',
    WRITE_TRUNCATE: 'WRITE_TRUNCATE'
};

/**
 * @typedef {Object} TempTableConfig
 * @property {String} datasetName
 * @property {String} tableName
 * @property {WriteDisposition=} writeDisposition
 */

class BigQueryJob {

    /**
     * @param {Object}    opts
     * @param {String=}   opts.name
     * @param {String}    opts.sqlFilename
     * @param {Number=}   [opts.costThresholdInGB = 100]
     * @param {Number=}   [opts.costPerTB = 5]
     * @param {Boolean=true}   opts.shouldQueryResults
     * @param {Number=30000}   opts.timeoutInSeconds
     * @param {TempTableConfig=}   opts.destinationTableConfig
     *
     * @param {BigQuery=} opts.bigQuery
     * @param {WinstonLogger|DummyLogger=} opts.logger
     */
    constructor(opts) {
        this.name = opts.name || null;
        this.sqlFilename = opts.sqlFilename || null;
        this.costThresholdInGB = opts.costThresholdInGB != null ? opts.costThresholdInGB : COST_THRESHOLD_IN_GB;
        this.costPerTB = opts.costPerTB != null ? opts.costPerTB : COST_PER_TB;

        this.destinationTableConfig = opts.destinationTableConfig;
        this.shouldQueryResults = opts.shouldQueryResults != null ? opts.shouldQueryResults : true;
        this.timeoutInMs = opts.timeoutInSeconds != null ? opts.timeoutInSeconds * 1000 : 30 * 1000;
        this.bigQuery = opts.bigQuery || null ;
        this.logger = opts.logger || new DummyLogger();

        this.sqlStr = null;
        this.sqlTemplate = null;
        this._initialized = false;
    }

    /**
     * @return {Boolean}
     */
    isInitialized() {
        return this._initialized === true;
    }

    /**
     * @return {Promise<BigQueryJob>}
     */
    async init() {
        if (this.isInitialized() === true) {
            return this;
        }
        this.bigQuery = this.bigQuery || BigQueryFactory.getInstance();
        if(this.destinationTableConfig != null){
            let destinationDataset = this.bigQuery.dataset(this.destinationTableConfig.datasetName);
            this.destinationTable = destinationDataset.table(this.destinationTableConfig.tableName);
        }
        this.sqlStr = await fs.readFile(this.sqlFilename, 'utf8');
        this.sqlTemplate = template(this.sqlStr);
        this._initialized = true;
        return this;
    }

    getDestinationTableAndWriteDispostion(){
        let ret = {};
        ret.destination = this.destinationTable || null;
        let writeDisposition = this.destinationTableConfig && this.destinationTableConfig.writeDisposition;
        if(writeDisposition != null){
            ret.writeDisposition = writeDisposition;
        }
        return ret;
    }

    /**
     * @return {Object|null}
     */
    getQueryParams() {
        // ATTENTION: This should be overridden to pass parameters to the sql template
        return null;
    }

    /**
     * @return {String}
     */
    getQuerySQLDebug() {
        // ATTENTION: This should be overridden to a debug comment at the header of each executed query
        return '';
    }

    /**
     * @return {String}
     */
    getQuerySQL() {
        const debugStr = this.getQuerySQLDebug();
        const params = this.getQueryParams();
        return (debugStr ? (debugStr.trim() + '\n') : '') + this.sqlTemplate(params || {});
    }

    /**
     * @params {Object=}  opts
     * @params {Boolean=} opts.dryRun
     * @params {Boolean=} opts.useLegacySql
     * @params {String=}  opts.query
     * @return {Object}
     */
    getQueryOptions(opts) {
        return {
            // This params CAN be overridden
            dryRun: false,
            useLegacySql: false,
            query: this.getQuerySQL(),
            ...this.getDestinationTableAndWriteDispostion(),
            ...opts,

            // This params CAN'T be overridden
            location: null,     // By default, it will take the default service account location
            jobId: null,        // BigQuery will generate the jobId randomly
            jobPrefix: null
        };
    }

    /**
     * @return {Promise<void>}
     */
    async validate() {
        this.logger.info('bigquery-job.js Validating job and getting estimated cost. name:%s', this.name);

        await this.init();
        const jobOpts = this.getQueryOptions({ dryRun: true });

        try {
            const [job] = await this.bigQuery.createQueryJob(jobOpts);
            const cost = _getCost(job.metadata.statistics.totalBytesProcessed, this.costPerTB);

            if (cost.gb >= this.costThresholdInGB) {
                this.logger.notify('BigQuery Job | Expensive Query').steps(0,1).msg('bigquery-job.js Expensive query over threshold. name:%s costThresholdInGB:%s. Estimated cost: $%s | %s TB | %s GB | %s MB | %s KB | %s bytes', this.name, this.costThresholdInGB, cost.price, cost.tb, cost.gb, cost.mb, cost.kb, cost.bytes)
            }
            else {
                this.logger.info('bigquery-job.js Validated job. name:%s. Estimated cost: $%s | %s TB | %s GB | %s MB | %s KB | %s bytes', this.name, cost.price, cost.tb, cost.gb, cost.mb, cost.kb, cost.bytes)
            }
        }
        catch(err) {
            BigQueryError.parseErrorAndThrow(err);
        }
    }


    isJobDone(jobMetadata){
        return jobMetadata.status.state === 'DONE';
    }

    getJobWaitInterval() {
        return Variables.isTestingMode() ? JOB_WAIT_INTERVAL_TEST : JOB_WAIT_INTERVAL;
    }

    async waitForJobDone(job) {
        const elapsed = Duration.start();
        let metadata;
        while(elapsed.end() < this.timeoutInMs){
            [metadata] = await job.getMetadata();
            if(this.isJobDone(metadata)){
                return metadata;
            }
            this.logger.info('bigquery-job.js Waiting for job to complete... name:%s elapsed:%s', this.name, elapsed.end());
            await PromiseTool.delay(this.getJobWaitInterval());
        }
        this.logger.error('bigquery-job.js Timeout waiting for job to complete. Most probably it will be billed anyway. name:%s elapsed:%s', this.name, elapsed.end());
        throw new Exception(Error.JOB_TIMEOUT, 'Timeout exceeded (%s ms) waiting for job completion', this.timeoutInMs);
    }

    /**
     * @return {Promise<Object[]>}
     */
    async run() {
        let elapsed = Duration.start();
        await this.validate();

        this.logger.info('bigquery-job.js Running job. name:%s', this.name);
        const jobOpts = this.getQueryOptions({ dryRun: false });

        try {
            const [job] = await this.bigQuery.createQueryJob(jobOpts);
            this.logger.info('bigquery-job.js Executed job. Getting query results. name:%s', this.name);

            let rows;

            const destinationMsg = this._getDestinationLogMsg();
            let metadata;
            if(this.shouldQueryResults){
                [rows] = await this._getQueryResults(job);
                this.logger.info('bigquery-job.js Got query results. name:%s totalRows:%s elapsed:%s ms%s', this.name, rows.length, elapsed.end(), destinationMsg);
                [metadata] = await job.getMetadata();
            } else {
                this.logger.info('bigquery-job.js Query results won\'t be returned due to shouldQueryResults:false. name:%s elapsed:%s ms%s', this.name, elapsed.end(), destinationMsg);
                metadata = await this.waitForJobDone(job);
            }
            const cacheHit = metadata.statistics.query.cacheHit;
            const cost = _getCost(metadata.statistics.query.totalBytesBilled, this.costPerTB);
            this.logger.info('bigquery-job.js Got query metadata. name:%s costThresholdInGB:%s cacheHit:%s. Billed cost: $%s | %s TB | %s GB | %s MB | %s KB | %s bytes', this.name, this.costThresholdInGB, cacheHit, cost.price, cost.tb, cost.gb, cost.mb, cost.kb, cost.bytes);

            return rows;
        }
        catch(err) {
            BigQueryError.parseErrorAndThrow(err);
        }
    }

    _getDestinationLogMsg() {
        let destinationInfo = this.getDestinationTableAndWriteDispostion();
        let destinationMsg = '';
        let destinationTable = destinationInfo.destination;
        if (destinationInfo.destination != null) {
            destinationMsg = ` destination:${destinationTable.parent.id}.${destinationTable.id}`;
        }
        if (destinationInfo.writeDisposition) {
            destinationMsg += ` writeDisposition:${destinationInfo.writeDisposition}`;
        }
        return destinationMsg;
    }

    /**
     * @returns {Promise<stream.Readable>}
     */
    async stream() {
        let elapsed = Duration.start();
        await this.validate();

        this.logger.info('bigquery-job.js Running job. name:%s', this.name);
        const jobOpts = this.getQueryOptions({ dryRun: false });

        try {
            const [job] = await this.bigQuery.createQueryJob(jobOpts);
            this.logger.info('bigquery-job.js Executed job. Getting query results stream. name:%s', this.name);

            const destinationMsg = this._getDestinationLogMsg();

            const stream = await this._getQueryResultsStream(job);
            this.logger.info('bigquery-job.js Got query results stream. name:%s elapsed:%s ms%s', this.name, elapsed.end(), destinationMsg);
            const [metadata] = await job.getMetadata();

            const cacheHit = metadata.statistics.query.cacheHit;
            const cost = _getCost(metadata.statistics.query.totalBytesBilled, this.costPerTB);
            this.logger.info('bigquery-job.js Got query metadata. name:%s costThresholdInGB:%s cacheHit:%s. Billed cost: $%s | %s TB | %s GB | %s MB | %s KB | %s bytes', this.name, this.costThresholdInGB, cacheHit, cost.price, cost.tb, cost.gb, cost.mb, cost.kb, cost.bytes);

            return stream;
        }
        catch(err) {
            BigQueryError.parseErrorAndThrow(err);
        }
    }

    /**
     * @param job
     * @return {Promise<QueryRowsResponse> | void}
     * @private
     */
    _getQueryResults(job) {
        return job.getQueryResults({ autoPaginate: true });
    }

    /**
     * @param job
     * @returns {Promise<stream.Readable>}
     * @private
     */
    _getQueryResultsStream(job) {
        return job.getQueryResultsStream();
    }
}

/**
 * @param {String} bytes
 * @param {Number} costPerTB
 * @return {{mb: string, bytes: string, price: string, gb: string, tb: string}}
 * @private
 */
function _getCost(bytes, costPerTB) {
    let kb = parseInt(bytes) / 1024;
    let mb = kb / 1024;
    let gb = mb / 1024;
    let tb = gb / 1024;
    let price = tb * costPerTB;

    return {
        tb: tb.toFixed(4),
        gb: gb.toFixed(4),
        mb: mb.toFixed(2),
        kb: kb.toFixed(2),
        bytes: bytes,
        price: price.toFixed(4)
    };
}

BigQueryJob.WriteDisposition = WriteDisposition;

module.exports = BigQueryJob;