'use strict';

describe('BigQueryJob Test', function () {

    const path = require('path');
    const sinon = require('sinon');
    const chai = require('chai');
    const { expect, assert } = chai;

    const DummyLogger = require('sq-logger/dummy-logger');
    const { BigQuery } = require('@google-cloud/bigquery');

    const PROJECT_ID = 'test-project';
    const DATASET_NAME = 'DatasetNew';

    const BigQueryUtil = require('../util');
    const BigQueryHelper = require('../helper');
    const BigQueryJob = require('../job');

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
            expect(bigQueryJob.logger).to.instanceOf(DummyLogger);
            expect(bigQueryJob.isInitialized()).to.equals(false);

            await bigQueryJob.init();

            expect(bigQueryJob.isInitialized()).to.equals(true);
            expect(bigQueryJob.bigQuery).to.instanceOf(BigQuery);
            expect(bigQueryJob.sqlStr).to.equals('SELECT some_field, other_field FROM `${dataset}.${table}`');
            assert.isFunction(bigQueryJob.sqlTemplate, 'sqlTemplate should be a function');

            const getInstanceSpy = sinon.spy(BigQueryHelper, 'getInstance');
            await bigQueryJob.init();
            expect(getInstanceSpy.called).to.equals(false);
            getInstanceSpy.restore();
        });

        it('2. Create instance with custom values and make init. Expect to be initialized correctly', async () => {

            const privateKeyFilename = path.resolve(__dirname, '../data/bigquery-credentials-test.json');
            const bigQuery = BigQueryHelper.create({ projectId: PROJECT_ID, privateKey: privateKeyFilename });
            const logger = new DummyLogger();

            const opts = _getOptions({ logger, bigQuery, costThresholdInGB: 10 });
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
                const getQueryParamsStub = sinon.stub(bigQueryJob, 'getQueryParams').returns({ dataset: 'DATASET' });
                bigQueryJob.getQuerySQL();
                expect.fail('getQuerySQL() should have failed');
            }
            catch(err) {
                expect(err.code).to.equals('ERROR_TEMPLATE');
                expect(err.message).to.equals('table is not defined');
                expect(err.stack).to.contains('TemplateException: ReferenceError: table is not defined');
            }
            finally {
                sinon.restore();
            }
        });

        it('3. Build query with variables. Expect to build query successfully ', async () => {

            const getQueryParamsStub = sinon.stub(bigQueryJob, 'getQueryParams').returns({ dataset: 'DATASET', table: 'TABLE' });
            const querySQL = bigQueryJob.getQuerySQL();

            expect(querySQL).to.equals('SELECT some_field, other_field FROM `DATASET.TABLE`');
            getQueryParamsStub.restore();
        });

        it('4. Build query with variables and debug string. Expect to build query successfully ', async () => {

            const getQuerySQLDebugStub = sinon.stub(bigQueryJob, 'getQuerySQLDebug').returns('-- JobId: JOB_ID\n-- JobName: JOB_NAME');
            const getQueryParamsStub = sinon.stub(bigQueryJob, 'getQueryParams').returns({ dataset: 'DATASET', table: 'TABLE' });
            const querySQL = bigQueryJob.getQuerySQL();

            expect(querySQL).to.equals('-- JobId: JOB_ID\n-- JobName: JOB_NAME\nSELECT some_field, other_field FROM `DATASET.TABLE`');
            getQueryParamsStub.restore();
        });
    });

    describe('3. getQueryOptions()', () => {

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

        it('1. Get query params with default values. Expect to return correct values with defaults', () => {
            const getQueryParamsStub = sinon.stub(bigQueryJob, 'getQueryParams').returns({ dataset: 'DATASET', table: 'TABLE' });
            let opts = bigQueryJob.getQueryOptions();

            expect(opts).to.eql({
                dryRun: null,
                useLegacySql: false,
                query: 'SELECT some_field, other_field FROM `DATASET.TABLE`',
                destination: null,
                location: null,
                jobId: null,
                jobPrefix: null
            });
        });

        it('2. Get query params with custom values. Expect to override only allowed vales and return correct values', () => {
            const getQueryParamsStub = sinon.stub(bigQueryJob, 'getQueryParams').returns({ dataset: 'DATASET', table: 'TABLE' });

            let opts = bigQueryJob.getQueryOptions({
                dryRun: false,
                useLegacySql: true,
                query: 'SELECT * FROM some_table',
                destination: 'VALUE',
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

    describe('4. validate()', () => {

        let authScope = null;
        let bigQueryUtil = null;

        before(() => {
            bigQueryUtil = new BigQueryUtil(PROJECT_ID, DATASET_NAME);

            bigQueryUtil.cleanAll();
            authScope = bigQueryUtil.nockOAuth();

            const privateKeyFilename = path.resolve(__dirname, '../data/bigquery-credentials-test.json');
            BigQueryHelper.createInstance({ projectId: PROJECT_ID, privateKey: privateKeyFilename });
        });

        beforeEach(() => {
            sinon.restore();
        });

        after(() => {
            expect(authScope.isDone()).to.equals(true);
            sinon.restore();
            bigQueryUtil.cleanAll();
            BigQueryHelper.clearInstance();
        });

        it('1.', async () => {

            const opts = _getOptions();
            const bigQueryJob = new BigQueryJob(opts);

            await bigQueryJob.init();
            await bigQueryJob.validate();
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
            logger: null,
            ...opts
        };
    }
});
