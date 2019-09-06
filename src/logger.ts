export enum Level {
    "FATAL" = "FATAL",
    "WARN" = "WARN",
    "ERROR" = "ERROR",
    "INFO" = "INFO",
    "LOG" = "LOG",
    "DEBUG" = "DEBUG",
}

export type Component = "REQTIME" | "STARTUP" | "UNKNOWN" | EsqlateErrorEnum;

export type Logger = (level: Level, component: Component, message: string, err?: Error) => void;

export enum EsqlateErrorEnum {
    InvalidDefinition = "ERR_INVALID_DEFINITION",
    MissingDefinition = "ERR_MISSING_DEFINITION",
    MissingLocal = "ERR_MISSING_LOCAL",
    MissingRequestParam = "ERR_MISSING_REQUEST_PARAM",
    InvalidRequestBody = "ERR_INVALID_REQUEST_BODY",
    NotFoundPersistenceError = "ERR_NOT_FOUND_PERSISTENCE_ERROR",
    SqlExecution = "ERR_SQL_EXECUTION",
    MissingVariable = "ERR_MISSING_VARIABLE",
    InvalidRequestParameter = "ERR_INVALID_REQUEST_PARAMETER",
}

export class EsqlateError extends Error {
    public code: EsqlateErrorEnum;
    constructor(code: EsqlateErrorEnum, msg: string) {
        super(`${code}: ${msg}`);
        this.code = code;
    }
}

export class EsqlateErrorInvalidRequestParameter extends EsqlateError {
    constructor(msg: string) { super(EsqlateErrorEnum.InvalidDefinition, msg); }
}

export class EsqlateErrorInvalidDefinition extends EsqlateError {
    constructor(msg: string) { super(EsqlateErrorEnum.InvalidDefinition, msg); }
}

export class EsqlateErrorMissingDefinition extends EsqlateError {
    constructor(msg: string) { super(EsqlateErrorEnum.MissingDefinition, msg); }
}

export class EsqlateErrorMissingLocal extends EsqlateError {
    constructor(msg: string) { super(EsqlateErrorEnum.MissingLocal, msg); }
}

export class EsqlateErrorInvalidRequestBody extends EsqlateError {
    constructor(msg: string) { super(EsqlateErrorEnum.InvalidRequestBody, msg); }
}

export class EsqlateErrorMissingVariables extends EsqlateError {
    constructor(msg: string) { super(EsqlateErrorEnum.MissingVariable, msg); }
}

export class EsqlateErrorNotFoundPersistence extends EsqlateError {
    constructor(msg: string) { super(EsqlateErrorEnum.NotFoundPersistenceError, msg); }
}

export class EsqlateErrorSqlExecution extends EsqlateError {
    constructor(msg: string) { super(EsqlateErrorEnum.SqlExecution, msg); }
}


const logger: Logger = (level: Level, component: Component, message: string, err?: Error) => {
    if (err && !(err instanceof EsqlateError)) {
        level = Level.ERROR;
    }
    let out: { message: string, err?: Error } = { message };
    if (err) {
        out = { ...out, err };
    }
    // tslint:disable:no-console
    switch (level) {
        case "DEBUG":
            console.debug(`${level} ${component}: ` + JSON.stringify(out));
            break;
        case "LOG":
            console.log(`${level} ${component}: ` + JSON.stringify(out));
            break;
        case "WARN":
            console.warn(`${level} ${component}: ` + JSON.stringify(out));
            break;
        case "INFO":
            console.info(`${level} ${component}: ` + JSON.stringify(out));
            break;
        default:
            console.error(`${level} ${component}: ` + JSON.stringify(out));
            if (level !== "ERROR") {
                process.exit(1);
            }
    }
    // tslint:enable:no-console
};

export default logger;
