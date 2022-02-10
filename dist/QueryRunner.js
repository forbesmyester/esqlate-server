"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getQueryRunner = exports.DatabaseType = void 0;
const MySQLQueryRunner_1 = __importDefault(require("./MySQLQueryRunner"));
const PostgreSQLQueryRunner_1 = __importDefault(require("./PostgreSQLQueryRunner"));
var DatabaseType;
(function (DatabaseType) {
    DatabaseType[DatabaseType["PostgreSQL"] = 1] = "PostgreSQL";
    DatabaseType[DatabaseType["MySQL"] = 2] = "MySQL";
})(DatabaseType = exports.DatabaseType || (exports.DatabaseType = {}));
function getQueryRunner(dbType, parallelism = 1, logger) {
    switch (dbType) {
        case DatabaseType.PostgreSQL:
            return PostgreSQLQueryRunner_1.default(parallelism, logger);
        case DatabaseType.MySQL:
            return MySQLQueryRunner_1.default(parallelism, logger);
    }
}
exports.getQueryRunner = getQueryRunner;
