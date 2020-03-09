const uuid = require('uuid');

class IdGenerator {
    static generateInsertId() {
        return uuid.v4();
    }
}
module.exports = IdGenerator;