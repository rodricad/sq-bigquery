'use strict';

const Variables = require('sq-toolkit/variables');
const Exception = require('sq-toolkit/exception');
const BigQuery = require('@google-cloud/bigquery').BigQuery;

let _instance = null;

const ErrorCode = {
    ERROR_INSTANCE_ALREADY_CREATED: 'ERROR_INSTANCE_ALREADY_CREATED',
    ERROR_INSTANCE_NOT_CREATED: 'ERROR_INSTANCE_NOT_CREATED'
};

class BigQueryHelper {

    static ErrorCode = ErrorCode;

    /**
     * @return {BigQuery}
     */
    static getInstance() {
        if (_instance == null) {
            throw new Exception(ErrorCode.ERROR_INSTANCE_NOT_CREATED, 'BigQuery instance should be created with .createInstance()');
        }
        return _instance;
    }

    /**
     * @param {Object}  opts
     * @param {String}  opts.projectId
     * @param {String=} opts.clientEmail
     * @param {String=} opts.privateKey
     * @param {String=} opts.keyFilename
     * @return {BigQuery}
     */
    static createInstance(opts) {
        if (_instance != null) {
            throw new Exception(ErrorCode.ERROR_INSTANCE_ALREADY_CREATED, 'BigQuery instance is already created and initialized. Use .getInstance()');
        }
        _instance = BigQueryHelper.create(opts);
        return _instance;
    }

    static clearInstance() {
        _instance = null;
    }

    /**
     * @param {Object}  opts
     * @param {String}  opts.projectId
     * @param {String=} opts.clientEmail
     * @param {String=} opts.privateKey
     * @param {String=} opts.keyFilename
     * @return {BigQuery}
     */
    static create(opts) {

        const options = {
            projectId: opts.projectId
        };

        if (opts.clientEmail != null && opts.privateKey != null) {
            options.credentials = {
                client_email: opts.clientEmail,
                private_key: opts.privateKey
            };
        }
        else if (opts.keyFilename != null) {
            options.keyFilename = opts.keyFilename;
        }

        const bigquery = new BigQuery(options);

        if (Variables.isTestingMode() === true) {
            bigquery.interceptors.push({
                request: function (reqOpts) {
                    reqOpts.gzip = false;
                    return reqOpts
                }
            });
        }

        return bigquery;
    }
}

module.exports = BigQueryHelper;