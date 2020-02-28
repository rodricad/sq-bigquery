'use strict';

const STR_ESCAPE_REGEX = /`/g;

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
        return new Function(...names, 'return `' + escapedStr + '`;')(...values);
    }

    return interpolate;
}

module.exports = template;