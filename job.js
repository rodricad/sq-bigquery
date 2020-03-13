'use strict';

const fs = require('fs-extra');
const _ = require('lodash');
const DummyLogger = require('sq-logger/dummy-logger');
const Duration = require('sq-toolkit/duration');

const template = require('./lib/utils/template');
const BigQueryError = require('./error');
const BigQueryFactory = require('./factory');

const COST_THRESHOLD_IN_GB = 100;
const COST_PER_TB = 5;

class BigQueryJob {

    /**
     * @param {Object}    opts
     * @param {String=}   opts.name
     * @param {String}    opts.sqlFilename
     * @param {Number=}   [opts.costThresholdInGB = 100]
     * @param {Number=}   [opts.costPerTB = 5]
     *
     * @param {BigQuery=} opts.bigQuery
     * @param {WinstonLogger|DummyLogger=} opts.logger
     */
    constructor(opts) {
        this.name = opts.name || null;
        this.sqlFilename = opts.sqlFilename || null;
        this.costThresholdInGB = opts.costThresholdInGB != null ? opts.costThresholdInGB : COST_THRESHOLD_IN_GB;
        this.costPerTB = opts.costPerTB != null ? opts.costPerTB : COST_PER_TB;

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
        this.sqlStr = await fs.readFile(this.sqlFilename, 'utf8');
        this.sqlTemplate = template(this.sqlStr);
        this._initialized = true;
        return this;
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

            ...opts,

            // This params CAN'T be overridden
            destination: null,  // With destination = null, BigQuery will generate a temporal table that lives for 24 hs
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
                this.logger.notify('BigQuery Job | Expensive Query').steps(0,1).msg('bigquery-job.js Expensive query over threshold. name:%s costThresholdInGB:%s. Estimated cost: $%s | %s TB | %s GB | %s KB | %s MB | %s bytes', this.name, this.costThresholdInGB, cost.price, cost.tb, cost.gb, cost.mb, cost.kb, cost.bytes)
            }
            else {
                this.logger.info('bigquery-job.js Validated job. name:%s. Estimated cost: $%s | %s TB | %s GB | %s KB | %s MB | %s bytes', this.name, cost.price, cost.tb, cost.gb, cost.mb, cost.kb, cost.bytes)
            }
        }
        catch(err) {
            BigQueryError.parseErrorAndThrow(err);
        }
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

            const [rows] = await this._getQueryResults(job);
            this.logger.info('bigquery-job.js Got query results. name:%s totalRows:%s elapsed:%s ms', this.name, rows.length, elapsed.end());

            const [metadata] = await job.getMetadata();
            const cacheHit = metadata.statistics.query.cacheHit;
            const cost = _getCost(metadata.statistics.query.totalBytesBilled, this.costPerTB);
            this.logger.info('bigquery-job.js Got query metadata. name:%s costThresholdInGB:%s cacheHit:%s. Billed cost: $%s | %s TB | %s GB | %s KB | %s MB | %s bytes', this.name, this.costThresholdInGB, cacheHit, cost.price, cost.tb, cost.gb, cost.mb, cost.kb, cost.bytes);

            return rows;
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

module.exports = BigQueryJob;