"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var Level;
(function (Level) {
    Level["FATAL"] = "FATAL";
    Level["WARN"] = "WARN";
    Level["ERROR"] = "ERROR";
    Level["INFO"] = "INFO";
    Level["LOG"] = "LOG";
    Level["DEBUG"] = "DEBUG";
})(Level = exports.Level || (exports.Level = {}));
var EsqlateErrorEnum;
(function (EsqlateErrorEnum) {
    EsqlateErrorEnum["InvalidDefinition"] = "ERR_INVALID_DEFINITION";
    EsqlateErrorEnum["MissingDefinition"] = "ERR_MISSING_DEFINITION";
    EsqlateErrorEnum["MissingLocal"] = "ERR_MISSING_LOCAL";
    EsqlateErrorEnum["MissingRequestParam"] = "ERR_MISSING_REQUEST_PARAM";
    EsqlateErrorEnum["InvalidRequestBody"] = "ERR_INVALID_REQUEST_BODY";
    EsqlateErrorEnum["NotFoundPersistenceError"] = "ERR_NOT_FOUND_PERSISTENCE_ERROR";
    EsqlateErrorEnum["SqlExecution"] = "ERR_SQL_EXECUTION";
    EsqlateErrorEnum["MissingVariable"] = "ERR_MISSING_VARIABLE";
    EsqlateErrorEnum["InvalidRequestParameter"] = "ERR_INVALID_REQUEST_PARAMETER";
})(EsqlateErrorEnum = exports.EsqlateErrorEnum || (exports.EsqlateErrorEnum = {}));
class EsqlateError extends Error {
    constructor(code, msg) {
        super(`${code}: ${msg}`);
        this.code = code;
    }
}
exports.EsqlateError = EsqlateError;
class EsqlateErrorInvalidRequestParameter extends EsqlateError {
    constructor(msg) { super(EsqlateErrorEnum.InvalidDefinition, msg); }
}
exports.EsqlateErrorInvalidRequestParameter = EsqlateErrorInvalidRequestParameter;
class EsqlateErrorInvalidDefinition extends EsqlateError {
    constructor(msg) { super(EsqlateErrorEnum.InvalidDefinition, msg); }
}
exports.EsqlateErrorInvalidDefinition = EsqlateErrorInvalidDefinition;
class EsqlateErrorMissingDefinition extends EsqlateError {
    constructor(msg) { super(EsqlateErrorEnum.MissingDefinition, msg); }
}
exports.EsqlateErrorMissingDefinition = EsqlateErrorMissingDefinition;
class EsqlateErrorMissingLocal extends EsqlateError {
    constructor(msg) { super(EsqlateErrorEnum.MissingLocal, msg); }
}
exports.EsqlateErrorMissingLocal = EsqlateErrorMissingLocal;
class EsqlateErrorInvalidRequestBody extends EsqlateError {
    constructor(msg) { super(EsqlateErrorEnum.InvalidRequestBody, msg); }
}
exports.EsqlateErrorInvalidRequestBody = EsqlateErrorInvalidRequestBody;
class EsqlateErrorMissingVariables extends EsqlateError {
    constructor(msg) { super(EsqlateErrorEnum.MissingVariable, msg); }
}
exports.EsqlateErrorMissingVariables = EsqlateErrorMissingVariables;
class EsqlateErrorNotFoundPersistence extends EsqlateError {
    constructor(msg) { super(EsqlateErrorEnum.NotFoundPersistenceError, msg); }
}
exports.EsqlateErrorNotFoundPersistence = EsqlateErrorNotFoundPersistence;
class EsqlateErrorSqlExecution extends EsqlateError {
    constructor(msg) { super(EsqlateErrorEnum.SqlExecution, msg); }
}
exports.EsqlateErrorSqlExecution = EsqlateErrorSqlExecution;
var LogTo;
(function (LogTo) {
    LogTo[LogTo["STDOUT"] = 0] = "STDOUT";
    LogTo[LogTo["STDERR"] = 1] = "STDERR";
})(LogTo || (LogTo = {}));
const loggerImpl = (logTo, level, component, message, err) => {
    if (err && !(err instanceof EsqlateError)) {
        level = Level.ERROR;
    }
    let out = { message };
    if (err) {
        out = { ...out, err };
    }
    function writer(s) {
        if (logTo === LogTo.STDERR) {
            process.stderr.write(s + "\n");
            return;
        }
        process.stdout.write(s + "\n");
    }
    switch (level) {
        case "DEBUG":
            writer(`${level} ${component}: ` + JSON.stringify(out));
            break;
        case "LOG":
            writer(`${level} ${component}: ` + JSON.stringify(out));
            break;
        case "WARN":
            writer(`${level} ${component}: ` + JSON.stringify(out));
            break;
        case "INFO":
            writer(`${level} ${component}: ` + JSON.stringify(out));
            break;
        default:
            writer(`${level} ${component}: ` + JSON.stringify(out));
            if (level !== "ERROR") {
                process.exit(1);
            }
    }
};
exports.stderrLogger = loggerImpl.bind(null, LogTo.STDERR);
const logger = loggerImpl.bind(null, LogTo.STDOUT);
exports.default = logger;
