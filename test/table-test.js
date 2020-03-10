'use strict';

describe('BigQueryTable Test', function () {

    let Promise = require('bluebird');
    let path    = require('path');
    let _       = require('lodash');
    let chai    = require('chai');
    let expect  = chai.expect;
    const Error = require('../lib/constants/error');

    let PROJECT_ID   = 'test-project';
    let DATASET_NAME = 'DatasetNew';
    let TABLE_NAME   = 'TableExists';

    let BufferQueue = require('sq-toolkit/buffer-queue');
    let NotifyUtil = require('sq-toolkit/test/utils/notify-util');
    let DummyLogger = require('sq-toolkit/test/utils/dummy-logger');

    let BigQueryUtil = require('../util');
    let bigQueryUtil = new BigQueryUtil(PROJECT_ID, DATASET_NAME);

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

    describe('1. static parseSchema()', function () {

        it('1. Parse schema with invalid type. Expect to raise Exception', function () {

            let schema = {
                title: 'INVALID_TYPE'
            };

            try {
                BigQueryTable.parseSchema(schema);
                chai.assert();
            }
            catch(err) {
                expect(err.code).to.equals('ERROR_INVALID_SCHEMA_TYPE');
            }
        });

        it('2. Parse schema with valid types. Expect to raise Exception', function () {

            let schema = {
                string: BigQueryTable.Schema.Type.STRING,
                date: BigQueryTable.Schema.Type.TIMESTAMP,
                int: BigQueryTable.Schema.Type.INTEGER,
                float: BigQueryTable.Schema.Type.FLOAT,
                bool: BigQueryTable.Schema.Type.BOOLEAN
            };

            let parsed = BigQueryTable.parseSchema(schema);
            expect(parsed).to.eql({
                "fields": [
                    {
                        "mode": "NULLABLE",
                        "name": "string",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "date",
                        "type": "TIMESTAMP"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "int",
                        "type": "INTEGER"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "float",
                        "type": "FLOAT"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "bool",
                        "type": "BOOLEAN"
                    }
                ]
            });
        });
    });

    describe('2. insert()', function () {

        it('1. Inert a single object with buffer disabled. Expect to be inserted immediately', function () {

            let table = _getTable({ bufferEnabled: false });

            let item = {
                title: 'value'
            };

            bigQueryUtil.patchInsertId();

            let scope = bigQueryUtil.nockInsert(table.name, item);

            return table.insert(item)
            .then(response => {
                scope.done();
                expect(response.kind).to.equals('bigquery#tableDataInsertAllResponse');
            })
            .finally(() => {
                bigQueryUtil.restoreInsertId();
            });
        });

        it('2. Insert a multiple objects with buffer enabled and maxItems = 5. Expect all items to be inserted in bulk and no promise returned', function () {

            let notify = NotifyUtil.getNotify(BufferQueue);

            let table = _getTable({ bufferEnabled: true, bufferMaxItems: 5});
            let items = [];

            for (let i = 0; i < 5; i++) {
                let item = { title: 'value ' + i };
                items.push(item);
            }

            bigQueryUtil.patchInsertId();

            let scope = bigQueryUtil.nockInsert(table.name, items);

            table.insert(items);

            return notify.deferred
            .then(() => {
                scope.done();
            })
            .finally(() => {
                NotifyUtil.restore(notify);
                bigQueryUtil.restoreInsertId();
            });
        });

        it('3. Insert a multiple objects with buffer enabled and maxItems = 2. Expect to log error', function () {

            let notify = NotifyUtil.getNotify(BufferQueue);

            let table = _getTable({ bufferEnabled: true, bufferMaxItems: 2});

            let item1 = { value: 1 };
            let item2 = { value: 'string' };

            bigQueryUtil.patchInsertId();

            let scope = bigQueryUtil.nockInsertError(table.name, [item1, item2]);

            // Valid item insert
            table.insert(item1);

            // Invalid item insert
            table.insert(item2);

            return notify.deferred
            .then(() => {
                scope.done();

                let logger = table.logger;

                expect(logger.notifier.values.key).to.equals('TableExists | BqBufferQueue Error');
                expect(logger.notifier.values.start).to.equals(10);
                expect(logger.notifier.values.each).to.equals(100);
                expect(logger.notifier.values.msg).to.equals('bq-buffer-queue.js:: Error at %s Buffer. Error: ');
            })
            .finally(() => {
                NotifyUtil.restore(notify);
                bigQueryUtil.restoreInsertId();
            });
        });

        it('4. Insert objects with buffer enabled and buffer item promises enabled. maxItems = 2. Expect promises returned', function () {

            let table = _getTable({ bufferEnabled: true, bufferMaxItems: 2, bufferItemPromises: true});

            let item1 = { value: 1 };
            let item2 = { value: 'string' };

            bigQueryUtil.patchInsertId();

            let scope = bigQueryUtil.nockInsert(table.name, [item1, item2]);

            let promise1 = table.insert(item1);
            let promise2 = table.insert(item2);

            return Promise.all([promise1, promise2])
            .then(() => {
                scope.done();
            })
            .finally(() => {
                bigQueryUtil.restoreInsertId();
            });
        });

        it('5. Try to insert multiple objects at once with buffer enabled and buffer item promises enabled. Expect error thrown', async () => {

            let table = _getTable({ bufferEnabled: true, bufferMaxItems: 2, bufferItemPromises: true});

            let items = [{ value: 1 }, { value: 'string' }];

            try {
                await table.insert(items);
            } catch (err) {
                expect(err.code).to.eql(Error.ERROR_ADD_MANY_NOT_SUPPORTED);
            }
        });

        it('6. Test inserting passing insertId. Expect promises returned', async () => {

            let table = _getTable({ bufferEnabled: true, bufferMaxItems: 2, bufferItemPromises: true});

            let item1 = { value: 1, _insertId: '123456789' };
            let item2 = { value: 'string', _insertId: '987654321' };

            let scope = bigQueryUtil.nockInsert(table.name, [item1, item2]);

            let promise1 = table.insert(item1);
            let promise2 = table.insert(item2);

            return Promise.all([promise1, promise2])
            .then(() => {
                scope.done();
            });
        })
    });

    /**
     * @param {Object=}  opts
     * @param {Boolean=} opts.bufferEnabled
     * @param {Number=} opts.bufferMaxItems
     * @param {Boolean=} opts.bufferItemPromises
     * @return {BigQueryTable}
     * @private
     */
    function _getTable(opts) {
        let name    = DATASET_NAME;
        let options = _getOptions(opts);
        let dataset = BigQueryDataset.getDataset(name, options);
        return dataset.getTable(TABLE_NAME);
    }

    /**
     * @param {Object=} opts
     * @return {Object}
     * @private
     */
    function _getOptions(opts) {
        let options = {
            projectId: PROJECT_ID,
            keyFilename: path.resolve(__dirname, '../data/bigquery-credentials-test.json'),
            logger: new DummyLogger(),
            bufferEnabled: false,
            bufferMaxItems: null,
            bufferMaxTime: null,
            bufferItemPromises: false
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
            value: BigQueryTable.Schema.Type.INTEGER
        };
        if (opts != null) {
            _.assignIn(options, opts);
        }
        return options;
    }
});
