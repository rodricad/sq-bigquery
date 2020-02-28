'use strict';

const Exception = require('sq-toolkit/exception');
const STR_ESCAPE_REGEX = /`/g;

const ERROR_CODE = 'ERROR_TEMPLATE';

/**
 * @param {String} str
 * @return {String}
 */
function escape(str) {
    return str.replace(STR_ESCAPE_REGEX, '\\`');
}

/**
 * @param {String=} str
 * @return {function(Object=): String}
 */
function template(str = '') {
    const escapedStr = escape(str);

    /**
     * @param {Object=} params
     * @return {String}
     */
    function interpolate(params = {}) {
        const names = Object.keys(params);
        const values = Object.values(params);
        const fn = new Function(...names, 'return `' + escapedStr + '`;');

        try {
            return fn(...values);
        }
        catch(err) {
            let exp = new Exception(ERROR_CODE, err.message);
            exp.stack = 'TemplateException: ' + err.stack;
            throw exp;
        }
    }

    return interpolate;
}

module.exports = template;