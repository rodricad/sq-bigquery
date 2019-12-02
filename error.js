'use strict';

let _ = require('lodash');
let Exception = require('sq-toolkit/exception');

const PARTIAL_FAILURE_ERROR = 'PartialFailureError';

class BigQueryError {

    /**
     * @param {Object} err
     * @return {Exception}
     */
    static parseError(err) {
        let exp = new Exception(err.code, err.message || err.name);

        if (err.name !== PARTIAL_FAILURE_ERROR) {
            exp.errors = null;
            return exp;
        }

        let errors = [];

        for (let i = 0; i < err.errors.length; i++) {
            let item   = err.errors[i];
            let parsed = {
                row: item.row,
                reason: _.get(item, 'errors[0].reason', null),
                message: _.get(item, 'errors[0].message', null),
            };

            errors.push(parsed);
        }

        exp.errors = errors;

        return exp;
    }

    /**
     * @param {Object} err
     */
    static parseErrorAndThrow(err) {
        throw BigQueryError.parseError(err);
    }
}

module.exports = BigQueryError;
