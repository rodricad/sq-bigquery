'use strict';

const TableConst = {

    ErrorCode: {
        INVALID_SCHEMA_TYPE: 'ERROR_INVALID_SCHEMA_TYPE'
    },

    Schema: {
        Type: {
            STRING: 'STRING',
            INTEGER: 'INTEGER',
            FLOAT: 'FLOAT',
            BOOLEAN: 'BOOLEAN',
            TIMESTAMP: 'TIMESTAMP'
        },
        Mode: {
            NULLABLE: 'NULLABLE'
        }
    }
};

module.exports = TableConst;
