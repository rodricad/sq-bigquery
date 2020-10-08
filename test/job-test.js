'use strict';

describe('BigQueryJob Test', function () {

    const path = require('path');
    const sinon = require('sinon');
    const _ = require('lodash');
    const chai = require('chai');
    const { expect, assert } = chai;

    const DummyLogger = require('sq-logger/dummy-logger');
    const { BigQuery } = require('@google-cloud/bigquery');

    const PROJECT_ID = 'test-project';
    const DATASET_NAME = 'DatasetNew';

    const BigQueryUtil = require('../util');
    const BigQueryFactory = require('../factory');
    const BigQueryJob = require('../job');
    const Error = require('../lib/constants/error');
    const WinstonLogger = require('sq-logger/winston-logger');
    const logger = new WinstonLogger({ console: { enabled: true } });

    before(() => {
        const bigQueryOpts = {
            projectId: PROJECT_ID,
            keyFilename: path.resolve(__dirname, '../data/bigquery-credentials-test.json')
        };
        BigQueryFactory.createInstance(bigQueryOpts);
    });

    after(() => {
        BigQueryFactory.clearInstance();
    });

    describe('1. init()', () => {

        it('1. Create instance with default values and make init. Expect to be initialized correctly', async () => {

            const opts = _getOptions();
            const bigQueryJob = new BigQueryJob(opts);

            expect(bigQueryJob.name).to.equals('TestQuery');
            expect(bigQueryJob.sqlFilename).to.contains('/test/data/dummy-query.sql');
            expect(bigQueryJob.costThresholdInGB).to.equals(100);
            expect(bigQueryJob.bigQuery).to.equals(null);
            expect(bigQueryJob.sqlStr).to.equals(null);
            expect(bigQueryJob.sqlTemplate).to.equals(null);
            expect(bigQueryJob.logger).to.instanceOf(WinstonLogger);
            expect(bigQueryJob.isInitialized()).to.equals(false);

            await bigQueryJob.init();

            expect(bigQueryJob.isInitialized()).to.equals(true);
            expect(bigQueryJob.bigQuery).to.instanceOf(BigQuery);
            expect(bigQueryJob.sqlStr).to.equals('SELECT some_field, other_field FROM `${dataset}.${table}`');
            assert.isFunction(bigQueryJob.sqlTemplate, 'sqlTemplate should be a function');

            const getInstanceSpy = sinon.spy(BigQueryFactory, 'getInstance');
            await bigQueryJob.init();
            expect(getInstanceSpy.called).to.equals(false);
            getInstanceSpy.restore();
        });

        it('2. Create instance with custom values and make init. Expect to be initialized correctly', async () => {

            const privateKeyFilename = path.resolve(__dirname, '../data/bigquery-credentials-test.json');
            const bigQuery = BigQueryFactory.create({ projectId: PROJECT_ID, privateKey: privateKeyFilename });
            const logger = new DummyLogger();

            const writeDisposition = 'WRITE_APPEND';
            const destinationTableConfig = {
                datasetName: 'DATESET_NAME',
                tableName: 'TABLE_NAME',
                writeDisposition: writeDisposition
            };
            const opts = _getOptions({ logger, bigQuery, costThresholdInGB: 10, destinationTableConfig });
            const bigQueryJob = new BigQueryJob(opts);

            expect(bigQueryJob.name).to.equals('TestQuery');
            expect(bigQueryJob.sqlFilename).to.contains('/test/data/dummy-query.sql');
            expect(bigQueryJob.costThresholdInGB).to.equals(10);
            expect(bigQueryJob.bigQuery).to.equals(bigQuery);
            expect(bigQueryJob.sqlStr).to.equals(null);
            expect(bigQueryJob.sqlTemplate).to.equals(null);
            expect(bigQueryJob.logger).to.equals(logger);
            expect(bigQueryJob.isInitialized()).to.equals(false);

            await bigQueryJob.init();

            expect(bigQueryJob.isInitialized()).to.equals(true);
            expect(bigQueryJob.bigQuery).to.equals(bigQuery);
            let destinationInfo = bigQueryJob.getDestinationTableAndWriteDispostion();
            expect(destinationInfo.destination.id).to.eql(destinationTableConfig.tableName);
            expect(destinationInfo.destination.parent.id).to.eql(destinationTableConfig.datasetName);
            expect(destinationInfo.writeDisposition).to.eql(writeDisposition);
            expect(bigQueryJob.sqlStr).to.equals('SELECT some_field, other_field FROM `${dataset}.${table}`');
            assert.isFunction(bigQueryJob.sqlTemplate, 'sqlTemplate should be a function');
        });
    });

    describe('2. getQuerySQL()', () => {

        let bigQueryJob = null;

        before(async () => {
            const opts = _getOptions();
            bigQueryJob = new BigQueryJob(opts);
            await bigQueryJob.init();
        });

        beforeEach(() => {
            sinon.restore();
        });

        after(() => {
            sinon.restore();
        });

        it('1. Build query with missing variables. Expect to throw exception ', async () => {

            try {
                bigQueryJob.getQuerySQL();
                expect.fail('getQuerySQL() should have failed');
            }
            catch(err) {
                expect(err.code).to.equals('ERROR_TEMPLATE');
                expect(err.message).to.equals('dataset is not defined');
                expect(err.stack).to.contains('TemplateException: ReferenceError: dataset is not defined');
            }
        });

        it('2. Build query with only [dataset] variable. Expect to throw exception ', async () => {

            try {
                class TestBigQueryJob extends BigQueryJob {
                    getQueryParams() {
                        return { dataset: 'DATASET' };
                    }
                }

                const opts = _getOptions();
                const bigQueryJob = new TestBigQueryJob(opts);
                await bigQueryJob.init();

                bigQueryJob.getQuerySQL();
                expect.fail('getQuerySQL() should have failed');
            }
            catch(err) {
                expect(err.code).to.equals('ERROR_TEMPLATE');
                expect(err.message).to.equals('table is not defined');
                expect(err.stack).to.contains('TemplateException: ReferenceError: table is not defined');
            }
        });

        it('3. Build query with variables. Expect to build query successfully ', async () => {

            class TestBigQueryJob extends BigQueryJob {
                getQueryParams() {
                    return { dataset: 'DATASET', table: 'TABLE' };
                }
            }

            const opts = _getOptions();
            const bigQueryJob = new TestBigQueryJob(opts);
            await bigQueryJob.init();

            const querySQL = bigQueryJob.getQuerySQL();

            expect(querySQL).to.equals('SELECT some_field, other_field FROM `DATASET.TABLE`');
        });

        it('4. Build query with variables and debug string. Expect to build query successfully ', async () => {

            class TestBigQueryJob extends BigQueryJob {

                getQuerySQLDebug() {
                    return '-- JobId: JOB_ID\n-- JobName: JOB_NAME';
                }

                getQueryParams() {
                    return { dataset: 'DATASET', table: 'TABLE' };
                }
            }

            const opts = _getOptions();
            const bigQueryJob = new TestBigQueryJob(opts);
            await bigQueryJob.init();

            const querySQL = bigQueryJob.getQuerySQL();

            expect(querySQL).to.equals('-- JobId: JOB_ID\n-- JobName: JOB_NAME\nSELECT some_field, other_field FROM `DATASET.TABLE`');
        });
    });

    describe('3. getQueryOptions()', () => {

        let bigQueryJob = null;

        before(async () => {
            class TestBigQueryJob extends BigQueryJob {
                getQueryParams() {
                    return { dataset: 'DATASET', table: 'TABLE' };
                }
            }

            const opts = _getOptions();
            bigQueryJob = new TestBigQueryJob(opts);
            await bigQueryJob.init();
        });

        beforeEach(() => {
            sinon.restore();
        });

        after(() => {
            sinon.restore();
        });

        it('1. Get query params with default values. Expect to return correct values with defaults', async () => {

            let opts = bigQueryJob.getQueryOptions();

            expect(opts).to.eql({
                dryRun: false,
                useLegacySql: false,
                query: 'SELECT some_field, other_field FROM `DATASET.TABLE`',
                destination: null,
                location: null,
                jobId: null,
                jobPrefix: null
            });
        });

        it('2. Get query params with custom values. Expect to override only allowed vales and return correct values', () => {

            let opts = bigQueryJob.getQueryOptions({
                dryRun: false,
                useLegacySql: true,
                query: 'SELECT * FROM some_table',
                location: 'VALUE',
                jobId: 'VALUE',
                jobPrefix: 'VALUE'
            });

            expect(opts).to.eql({
                dryRun: false,
                useLegacySql: true,
                query: 'SELECT * FROM some_table',
                destination: null,
                location: null,
                jobId: null,
                jobPrefix: null
            });
        });
    });

    describe('4. run()', () => {

        let authScope = null;
        let bigQueryUtil = null;

        before(() => {
            bigQueryUtil = new BigQueryUtil(PROJECT_ID, DATASET_NAME);
            bigQueryUtil.cleanAll();

            authScope = bigQueryUtil.nockOAuth();
        });

        beforeEach(() => {
            sinon.restore();
        });

        after(() => {
            authScope.done();

            sinon.restore();
            bigQueryUtil.cleanAll();
        });

        it('1. Create BigQueryJob and make run(). Expect to validate first and the return rows', async () => {

            const opts = _getOptionsComplete();
            const bigQueryJob = new BigQueryJob(opts);

            await bigQueryJob.init();

            const queryStr = bigQueryJob.getQuerySQL();
            const rows = _getResults();

            const jobScope = bigQueryUtil.nockJob(queryStr, rows);

            const results = await bigQueryJob.run();

            jobScope.done();

            expect(results).to.instanceOf(Array);
            expect(results).to.length(3);
        });

        it('2. run() must not return rows and wait for job completion if avoidReturningResults is passed on construction', async () => {
            const destinationTableConfig = {
                datasetName: 'DATASET',
                tableName: 'TABLE',
                writeDisposition: 'WRITE_APPEND'
            };
            const moreOptions = {
                shouldQueryResults: false,
                destinationTableConfig: destinationTableConfig
            };
            const opts = Object.assign(_getOptionsComplete(), moreOptions);
            const bigQueryJob = new BigQueryJob(opts);

            await bigQueryJob.init();

            const queryStr = bigQueryJob.getQuerySQL();

            const jobScope = bigQueryUtil.nockJob(queryStr, null, {statuses: ['RUNNING', 'DONE'], destinationTableConfig: destinationTableConfig, expectQueryResultToBeCalled: false});

            const results = await bigQueryJob.run();

            jobScope.done();
            expect(results).to.eql(undefined);
        });

        it('3. run() throw timeout exception if job timeout is exceeded before getting DONE status', async () => {
            const destinationTableConfig = {
                datasetName: 'DATASET',
                tableName: 'TABLE',
                writeDisposition: 'WRITE_APPEND'
            };
            const moreOptions = {
                shouldQueryResults: false,
                destinationTableConfig: destinationTableConfig,
                timeoutInSeconds: 0.001
            };
            const opts = Object.assign(_getOptionsComplete(), moreOptions);
            const bigQueryJob = new BigQueryJob(opts);

            await bigQueryJob.init();

            const queryStr = bigQueryJob.getQuerySQL();

            const jobScope = bigQueryUtil.nockJob(queryStr, null, {statuses: ['RUNNING'], destinationTableConfig: destinationTableConfig, expectQueryResultToBeCalled: false});

            try {
                await bigQueryJob.run();
                expect.fail('should not reach here');
            } catch(err) {
                expect(err.code).to.eql(Error.JOB_TIMEOUT);
                jobScope.done();
            }
        });
    });

    /**
     * @param {Object=} opts
     * @return {Object}
     * @private
     */
    function _getOptions(opts= {}) {

        return {
            name: 'TestQuery',
            sqlFilename: path.resolve(__dirname, './data/dummy-query.sql'),
            costThresholdInGB: null,
            bigQuery: null,
            logger: logger,
            ...opts
        };
    }

    /**
     * @param {Object=} opts
     * @return {Object}
     * @private
     */
    function _getOptionsComplete(opts= {}) {
        return {
            ..._getOptions(),
            sqlFilename: path.resolve(__dirname, './data/dummy-query-complete.sql')
        };
    }

    /**
     * @return {Array}
     * @private
     */
    function _getResults() {
        const items = require('./data/dummy-query-complete-results');
        return _.cloneDeep(items);
    }
});
