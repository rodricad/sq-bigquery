'use strict';

describe('BigQueryDataset Test', function () {

    let Promise = require('bluebird');
    let path    = require('path');
    let nock    = require('nock');
    let _       = require('lodash');
    let chai    = require('chai');
    let expect  = chai.expect;

    let PROJECT_ID   = 'test-project';

    let BigQueryUtil = require('../util');
    let bigQueryUtil = new BigQueryUtil(PROJECT_ID);

    let BigQueryDataset = require('../dataset');
    let BigQueryTable   = require('../table');

    let authScope = null;

    before(() => {
        bigQueryUtil.cleanAll();
        authScope = bigQueryUtil.nockOAuth();
    });

    after(() => {
        expect(authScope.isDone()).to.equals(true);
        bigQueryUtil.cleanAll();
    });

    describe('1. static getDataset() and .init()', function () {

        // it('1. Get dataset without credentials and init. Expect to raise Exception', function () {
        //
        //     let name = 'DatasetUnknown';
        //     let opts = _getOptions({ keyFilename: null });
        //
        //     let dataset = BigQueryDataset.getDataset(name, opts);
        //
        //     return dataset.init()
        //     .then(() => {
        //         chai.assert();
        //     })
        //     .catch(err => {
        //         expect(err.message).to.equals('Could not load the default credentials. Browse to https://cloud.google.com/docs/authentication/getting-started for more information.');
        //     });
        // });

        it('2. Get dataset with invalid filename location and init. Expect to raise Exception', function () {

            let name = 'DatasetUnknown';
            let opts = _getOptions({ keyFilename: path.resolve(__dirname, './credentials.json') });

            let dataset = BigQueryDataset.getDataset(name, opts);

            return dataset.init()
            .then(() => {
                chai.assert();
            })
            .catch(err => {
                expect(err.code).to.equals('ENOENT');
                expect(err.message).to.includes('ENOENT: no such file or directory, open');
            });
        });

        it('3. Get dataset that does not exists and init. Expect to raise 404 Not Found Exception', function () {

            let name = 'DatasetUnknown';
            let opts = _getOptions();

            let scope = bigQueryUtil.nockGetDatasetNotFound(name);

            let dataset = BigQueryDataset.getDataset(name, opts);

            return dataset.init()
            .then(() => {
                chai.assert();
            })
            .catch(err => {
                scope.done();
                expect(err.code).to.equals(404);
                expect(err.message).to.equals(`Not found: Dataset ${opts.projectId}:${name}`);
            });
        });

        it('4. Get dataset that exists and init. Expect to return BigQueryDataset instance', function () {

            let name = 'DatasetExists';
            let opts = _getOptions();

            let scope = bigQueryUtil.nockGetDatasetFound(name);

            let dataset = BigQueryDataset.getDataset(name, opts);

            return dataset.init()
            .then((dataset) => {
                expect(scope.isDone()).to.equals(true);
                expect(dataset).to.be.instanceOf(BigQueryDataset);
                expect(dataset.name).to.equals('DatasetExists');
                expect(dataset.bufferEnabled).to.equals(false);
                expect(dataset.logger).to.equals(null);
            });
        });
    });

    describe('2. static createDataset()', function () {

        // it('1. Create dataset without credentials. Expect to raise Exception', function () {
        //
        //     let name = 'DatasetUnknown';
        //     let opts = _getOptions({ keyFilename: null });
        //
        //     return BigQueryDataset.createDataset(name, opts)
        //     .then(() => {
        //         chai.assert();
        //     })
        //     .catch(err => {
        //         expect(err.message).to.equals('Could not load the default credentials. Browse to https://cloud.google.com/docs/authentication/getting-started for more information.');
        //     });
        // });

        it('2. Create dataset with invalid filename location. Expect to raise Exception', function () {

            let name = 'DatasetUnknown';
            let opts = _getOptions({ keyFilename: path.resolve(__dirname, './credentials.json') });

            return BigQueryDataset.createDataset(name, opts)
            .then(() => {
                chai.assert();
            })
            .catch(err => {
                expect(err.code).to.equals('ENOENT');
                expect(err.message).to.includes('ENOENT: no such file or directory, open');
            });
        });

        it('3. Create dataset that already exists. Expect to raise 409 Already Exists Exception', function () {

            let name = 'DatasetExists';
            let opts = _getOptions();

            let scope = bigQueryUtil.nockCreateDatasetAlreadyExists(name);

            return BigQueryDataset.createDataset(name, opts)
            .then(() => {
                chai.assert();
            })
            .catch(err => {
                scope.done();
                expect(err.code).to.equals(409);
                expect(err.message).to.equals(`Already Exists: Dataset ${opts.projectId}:${name}`);
            });
        });

        it('4. Create dataset that does not already exists. Expect to return dataset', function () {

            let name = 'DatasetNew';
            let opts = _getOptions();

            let scope = bigQueryUtil.nockCreateDataset(name);

            return BigQueryDataset.createDataset(name, opts)
            .then(dataset => {
                expect(scope.isDone()).to.equals(true);
                expect(dataset).to.be.instanceOf(BigQueryDataset);
                expect(dataset.name).to.equals('DatasetNew');
                expect(dataset.bufferEnabled).to.equals(false);
                expect(dataset.logger).to.equals(null);
            });
        });
    });

    describe('3. createTable()', function () {

        it('1. Create table that already exists. Expect to raise 409 Already Exists Exception', function () {

            let dataset = _getDataset();
            let name    = 'TableExists';
            let schema  = _getSchema();

            let scope  = bigQueryUtil.nockCreateTableAlreadyExists(dataset.name, name, BigQueryTable.parseSchema(schema));

            return dataset.createTable(name, schema)
            .then(() => {
                chai.assert();
            })
            .catch(err => {
                scope.done();
                expect(err.code).to.equals(409);
                expect(err.message).to.equals(`Already Exists: Table ${PROJECT_ID}:${dataset.name}.${name}`);
            });
        });

        it('2. Create table that does not already exists. Expect to return table', function () {

            let dataset = _getDataset();
            let name    = 'TableNew';
            let schema  = _getSchema();

            let scope  = bigQueryUtil.nockCreateTable(dataset.name, name, BigQueryTable.parseSchema(schema));

            return dataset.createTable(name, schema)
            .then(table => {
                scope.done();
                expect(table).to.be.instanceOf(BigQueryTable);
                expect(table.name).to.equals('TableNew');
                expect(table.bufferEnabled).to.equals(false);
                expect(table.logger).to.equals(null);
            });
        });

        /**
         * @return {BigQueryDataset}
         * @private
         */
        function _getDataset() {
            let name  = 'DatasetNew';
            let opts  = _getOptions();
            return BigQueryDataset.getDataset(name, opts);
        }
    });

    /**
     * @param {Object=} opts
     * @return {Object}
     * @private
     */
    function _getOptions(opts) {
        let options = {
            projectId: PROJECT_ID,
            keyFilename: path.resolve(__dirname, '../data/bigquery-credentials-test.json'),
            logger: null,
            bufferEnabled: false
        };
        if (opts != null) {
            _.assignIn(options, opts);
        }
        return options;
    }

    /**
     * @param {Object=} opts
     * @return {Object}
     * @private
     */
    function _getSchema(opts) {
        let options = {
            title: BigQueryTable.Schema.Type.STRING,
        };
        if (opts != null) {
            _.assignIn(options, opts);
        }
        return options;
    }
});
