'use strict';

let nock = require('nock');
let sinon = require('sinon');

let BigQueryTable = require('./table');

class BigQueryUtil {

    /**
     * @param {String}  projectId
     * @param {String=} datasetName
     */
    constructor(projectId, datasetName) {
        this.projectId    = projectId;
        this.datasetName  = datasetName || null;
        this.stubInsertId = null;
    }

    /**
     * @return {String[]}
     */
    getResponseHeaders() {
        return [
            'Vary',
            'X-Origin',
            'Vary',
            'Referer',
            'Content-Type',
            'application/json; charset=UTF-8',
            'Date',
            'Fri, 25 Oct 2019 14:24:34 GMT',
            'Server',
            'ESF',
            'Cache-Control',
            'private',
            'X-XSS-Protection',
            '0',
            'X-Frame-Options',
            'SAMEORIGIN',
            'X-Content-Type-Options',
            'nosniff',
            'Alt-Svc',
            'quic=":443"; ma=2592000; v="46,43",h3-Q048=":443"; ma=2592000,h3-Q046=":443"; ma=2592000,h3-Q043=":443"; ma=2592000',
            'Accept-Ranges',
            'none',
            'Vary',
            'Origin,Accept-Encoding',
            'Transfer-Encoding',
            'chunked'
        ];
    }

    /**
     * @return {String[]}
     */
    getResponseGzipHeaders() {
        return [
            'Content-Type',
            'application/json; charset=utf-8',
            'Vary',
            'Origin',
            'Vary',
            'X-Origin',
            'Vary',
            'Referer',
            'Content-Encoding',
            'gzip',
            'Date',
            'Fri, 25 Oct 2019 12:08:17 GMT',
            'Server',
            'scaffolding on HTTPServer2',
            'Cache-Control',
            'private',
            'X-XSS-Protection',
            '0',
            'X-Frame-Options',
            'SAMEORIGIN',
            'X-Content-Type-Options',
            'nosniff',
            'Alt-Svc',
            'quic=":443"; ma=2592000; v="46,43",h3-Q048=":443"; ma=2592000,h3-Q046=":443"; ma=2592000,h3-Q043=":443"; ma=2592000',
            'Connection',
            'close',
            'Transfer-Encoding',
            'chunked'
        ];
    }

    /**
     * @return {nock.Scope}
     */
    getBaseNock() {
        return nock('https://bigquery.googleapis.com:443', { encodedQueryParams: true });
    }

    /**
     * @param {Number=} times If not defined, persist option is used by default
     * @return {nock.Scope}
     */
    nockOAuth(times) {
        let scope = nock('https://www.googleapis.com:443', { encodedQueryParams: true });

        if (times == null) {
            scope.persist();
        }

        scope = scope.post('/oauth2/v4/token', body => true);

        if (times != null) {
            scope = scope.times(times);
        }

        return scope.reply(200, ["1f","8b","08","00","00","00","00","00","02ff1dcecd1242400000e07b4f61f61c238aea4635d13636257f971dcc3642cb585bd1f4ee8daedfe9fb4c040124594618c35d5d120ad602e81365256512acb4c65444d5af5d44c3e7d5c2212a726fb34bd5a5ce53976b81e88b9c53fbbd481f2b3d6e2d3a4fa1c74f2c77cdc8e1b9db572cdedeca0046e7f8b887913f3866827248bbe1e084c8400ffb252e1947b8302e05931d1997e5010f119b194f301d73e4dddc5bc2f07daca99a2cfff57fc55ddf90316c92a4252d987c7f195006bacf000000"], this.getResponseGzipHeaders());
    }

    /**
     * @param {String} datasetName
     * @return {nock.Scope}
     */
    nockGetDatasetNotFound(datasetName) {

        let response = {
            "error": {
                "code": 404,
                "message": `Not found: Dataset ${this.projectId}:${datasetName}`,
                "errors": [{
                    "message": `Not found: Dataset ${this.projectId}:${datasetName}`,
                    "domain": "global",
                    "reason": "notFound"
                }],
                "status": "NOT_FOUND"
            }
        };

        return this.getBaseNock()
        .get(`/bigquery/v2/projects/${this.projectId}/datasets/${datasetName}`)
        .query({})
        .reply(404, response, this.getResponseHeaders());
    }

    /**
     * @param {String} datasetName
     * @return {nock.Scope}
     */
    nockGetDatasetFound(datasetName) {

        let response = {
            "kind": "bigquery#dataset",
            "etag": "XTrx5aYnJTH8D7PSJwGjWw==",
            "id": `${this.projectId}:${datasetName}`,
            "selfLink": `https://bigquery.googleapis.com/bigquery/v2/projects/${this.projectId}/datasets/${datasetName}`,
            "datasetReference": {"datasetId": datasetName, "projectId": this.projectId},
            "defaultTableExpirationMs": "5184000000",
            "access": [
                {"role": "WRITER", "specialGroup": "projectWriters"},
                {"role": "OWNER", "specialGroup": "projectOwners"},
                {"role": "OWNER", "userByEmail": "example@example.com"},
                {"role": "READER", "specialGroup": "projectReaders"}
            ],
            "creationTime": "1575200000000",
            "lastModifiedTime": "1575200000000",
            "location": "US",
            "defaultPartitionExpirationMs": "5184000000"
        };

        return this.getBaseNock()
        .get(`/bigquery/v2/projects/${this.projectId}/datasets/${datasetName}`)
        .query({})
        .reply(200, response, this.getResponseHeaders());
    }

    /**
     * @param {String} datasetName
     * @return {nock.Scope}
     */
    nockCreateDatasetAlreadyExists(datasetName) {

        let body = { "datasetReference": { "datasetId": datasetName } };

        let response = {
            "error": {
                "code": 409,
                "message": `Already Exists: Dataset ${this.projectId}:${datasetName}`,
                "errors": [{
                    "message": `Already Exists: Dataset ${this.projectId}:${datasetName}`,
                    "domain": "global",
                    "reason": "duplicate"
                }],
                "status": "ALREADY_EXISTS"
            }
        };

        return this.getBaseNock()
        .post(`/bigquery/v2/projects/${this.projectId}/datasets`, body)
        .reply(409, response, this.getResponseHeaders());
    }

    /**
     * @param {String} datasetName
     * @return {nock.Scope}
     */
    nockCreateDataset(datasetName) {

        let body = { "datasetReference": { "datasetId": datasetName } };

        let response = {
            "kind": "bigquery#dataset",
            "etag": "nBR2Ij8PPtR7DLvHgtKlMA==",
            "id": `${this.projectId}:${datasetName}`,
            "selfLink": `https://bigquery.googleapis.com/bigquery/v2/projects/${this.projectId}/datasets/${datasetName}`,
            "datasetReference": {"datasetId": datasetName, "projectId": this.projectId},
            "defaultTableExpirationMs": "5184000000",
            "access": [
                {"role": "WRITER", "specialGroup": "projectWriters"},
                {"role": "OWNER", "specialGroup": "projectOwners"},
                {"role": "OWNER", "userByEmail": "example@example.com"},
                {"role": "READER", "specialGroup": "projectReaders"}
            ],
            "creationTime": "1575200000000",
            "lastModifiedTime": "1575200000000",
            "location": "US",
            "defaultPartitionExpirationMs": "5184000000"
        };

        return this.getBaseNock()
        .post(`/bigquery/v2/projects/${this.projectId}/datasets`, body)
        .reply(200, response, this.getResponseHeaders());
    }

    /**
     * @param {String} datasetName
     * @param {String} tableName
     * @param {Object} schema
     * @return {nock.Scope}
     */
    nockCreateTableAlreadyExists(datasetName, tableName, schema) {

        let body = {
            "schema": schema,
            "tableReference": {
                "projectId": this.projectId,
                "datasetId": datasetName,
                "tableId": tableName
            }
        };

        let response = {
            "error": {
                "code": 409,
                "message": `Already Exists: Table ${this.projectId}:${datasetName}.${tableName}`,
                "errors": [{
                    "message": `Already Exists: Table ${this.projectId}:${datasetName}.${tableName}`,
                    "domain": "global",
                    "reason": "duplicate"
                }],
                "status": "ALREADY_EXISTS"
            }
        };

        return this.getBaseNock()
        .post(`/bigquery/v2/projects/${this.projectId}/datasets/${datasetName}/tables`, body)
        .reply(409, response, this.getResponseHeaders());
    }

    /**
     * @param {String} datasetName
     * @param {String} tableName
     * @param {Object} schema
     * @return {nock.Scope}
     */
    nockCreateTable(datasetName, tableName, schema) {

        let body = {
            "schema": schema,
            "tableReference": {
                "projectId": this.projectId,
                "datasetId": datasetName,
                "tableId": tableName
            }
        };

        let response = {
            "kind": "bigquery#table",
            "etag": "YoSPs1PVuL/YUbChJQTt7g==",
            "id": `${this.projectId}:${datasetName}.${tableName}`,
            "selfLink": `https://bigquery.googleapis.com/bigquery/v2/projects/${this.projectId}/datasets/${datasetName}/tables/${tableName}`,
            "tableReference": {"projectId": this.projectId, "datasetId": datasetName, "tableId": tableName},
            "schema": schema,
            "numBytes": "0",
            "numLongTermBytes": "0",
            "numRows": "0",
            "creationTime": "1575200000000",
            "expirationTime": "1577208094562",
            "lastModifiedTime": "1575200000000",
            "type": "TABLE",
            "location": "US"
        };

        return this.getBaseNock()
        .post(`/bigquery/v2/projects/${this.projectId}/datasets/${datasetName}/tables`, body)
        .reply(200, response, this.getResponseHeaders());
    }

    /**
     * @param {String} tableName
     * @param {Object} rows
     * @param {Number=2} delay
     * @return {nock.Scope}
     */
    nockInsert(tableName, rows, delay=2) {

        let items = BigQueryUtil.parseInsertRows(rows);
        let body  = { rows: items };

        let response = { "kind": "bigquery#tableDataInsertAllResponse" };

        return this.getBaseNock()
        .post(`/bigquery/v2/projects/${this.projectId}/datasets/${this.datasetName}/tables/${tableName}/insertAll`, body)
        .delay(delay)
        .reply(200, response, this.getResponseHeaders());
    }

    /**
     * @param {String} tableName
     * @param {Object} rows
     * @return {nock.Scope}
     */
    nockInsertError(tableName, rows) {

        let items = BigQueryUtil.parseInsertRows(rows);
        let body  = { rows: items };

        let response = {
            "kind": "bigquery#tableDataInsertAllResponse",
            "insertErrors": [
                {
                    "index": 0,
                    "errors": [
                        {
                            "reason": "invalid",
                            "location": "value",
                            "debugInfo": "",
                            "message": "INVALID_MESSAGE_PLACEHOLDER"
                        }
                    ]
                }
            ]
        };

        return this.getBaseNock()
        .post(`/bigquery/v2/projects/${this.projectId}/datasets/${this.datasetName}/tables/${tableName}/insertAll`, body)
        .reply(200, response, this.getResponseHeaders());
    }

    patchInsertId() {
        if (this.stubInsertId != null) {
            return;
        }
        this.stubInsertId = sinon.stub(BigQueryTable, 'getInsertId').callsFake(function () {
            return BigQueryUtil.getDummyInsertId();
        });
    }

    restoreInsertId() {
        if (this.stubInsertId != null) {
            this.stubInsertId.restore();
            this.stubInsertId = null;
        }
    }

    /**
     * @param {Object|Object[]} rows
     * @return {Object[]}
     */
    static parseInsertRows(rows) {
        return Array.isArray(rows) === true ? rows : [rows];
    }

    /**
     * @return {String}
     */
    static getDummyInsertId() {
        return '00000000-0000-0000-0000-000000000000';
    }

    cleanAll() {
        BigQueryUtil.cleanAll();
    }

    static cleanAll() {
        nock.cleanAll();
    }
}

module.exports = BigQueryUtil;
