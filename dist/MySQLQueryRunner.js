"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const esqlate_lib_1 = require("esqlate-lib");
const esqlate_promise_returning_function_to_generator_1 = __importDefault(require("esqlate-promise-returning-function-to-generator"));
const esqlate_queue_1 = __importDefault(require("esqlate-queue"));
const mysql_1 = require("mysql");
const random_crypto_string_1 = __importDefault(require("random-crypto-string"));
const logger_1 = require("./logger");
function rowMapper(fields, rdp) {
    return fields.map(({ name }) => {
        return rdp.hasOwnProperty(name) ? rdp[name] : null;
    });
}
function getQuery(normalizedStatement, serverParameters, parameters) {
    const inputValues = parameters.concat(serverParameters).reduce((acc, p) => {
        return { ...acc, [p.name]: p.value };
    }, {});
    function getSqlValue(p) {
        if (p.empty_string_is_null && (inputValues[p.name] === "")) {
            return null;
        }
        return inputValues[p.name];
    }
    function reducer({ text, paramValues }, ed) {
        if (typeof ed === "string") {
            text = `${text}${ed}`;
        }
        else {
            paramValues = paramValues.concat(getSqlValue(ed));
            text = text + "?";
        }
        return { text, paramValues };
    }
    return normalizedStatement.reduce(reducer, {
        text: "",
        paramValues: [],
    });
}
exports.getQuery = getQuery;
function getEsqlateErrorResult(e) {
    let message = e.message + " - debugging information: ";
    const keys = [
        "severity",
        "code",
        "detail",
        "hint",
        "position",
        "internalPosition",
        "internalQuery",
        "where",
        "schema",
        "table",
        "column",
        "dataType",
        "constraint",
    ];
    const merge = {};
    for (const k of keys) {
        if (e[k]) {
            merge[k] = e[k];
        }
    }
    message = message + JSON.stringify(merge);
    return { status: "error", message };
}
function getDemandRunner(pool) {
    return demandRunner.bind(null, pool);
}
function demandRunner(pool, definition, serverParameters, userParameters) {
    const normalized = esqlate_lib_1.normalize(definition.parameters, definition.statement);
    const getQueryOut = getQuery(normalized, serverParameters, userParameters);
    return demandRunnerImpl(pool, getQueryOut.text, getQueryOut.paramValues)
        .catch(getEsqlateErrorResult);
}
function demandRunnerImpl(pool, inSql, values) {
    return new Promise((resolve, reject) => {
        const sql = mysql_1.format(inSql, values);
        const fieldsFound = new Set();
        const fields = [];
        const qry = {
            sql,
            typeCast: (field, next) => {
                if (!fieldsFound.has(field.name)) {
                    fields.push({ name: field.name, type: field.type });
                    fieldsFound.add(field.name);
                }
                return next();
            },
        };
        pool.query(qry, (err, res) => {
            if (err) {
                return reject(err);
            }
            resolve({
                fields,
                rows: res.map(rowMapper.bind(null, fields)),
                status: "complete",
            });
        });
    });
}
function getEsqlateQueueWorker(pool) {
    const REQUEST_PER_TIME = 1024;
    return async function getEsqlateQueueWorkerImpl(qi) {
        const fieldsFound = new Set();
        let aborted = false;
        let complete = false;
        const fields = [];
        let started = false;
        let buffer = [];
        let resolve = null;
        let reject = null;
        const normalized = esqlate_lib_1.normalize(qi.definition.parameters, qi.definition.statement);
        const conn = await new Promise((myResolve, myReject) => {
            pool.getConnection((err, connection) => {
                if (err) {
                    return myReject(getEsqlateErrorResult(err));
                }
                myResolve(connection);
            });
        });
        const getQueryOut = getQuery(normalized, qi.serverParameters, qi.userParameters);
        function register() {
            const result = conn.query({
                sql: conn.format(getQueryOut.text, getQueryOut.paramValues),
                typeCast: (field, next) => {
                    if (!fieldsFound.has(field.name)) {
                        fields.push({ name: field.name, type: field.type });
                        fieldsFound.add(field.name);
                    }
                    return next();
                },
            });
            result.on("error", (e) => {
                aborted = true;
                resolve = null;
                pool.releaseConnection(conn);
                if (reject === null) {
                    throw e;
                }
                reject(getEsqlateErrorResult(e));
            });
            result.on("result", (row) => {
                buffer.push(row);
                if (complete || aborted || (buffer.length >= REQUEST_PER_TIME)) {
                    conn.pause();
                    const r = buffer.concat([]);
                    buffer = [];
                    const x = resolve;
                    resolve = null;
                    reject = null;
                    x(r);
                }
            });
            result.on("end", () => {
                pool.releaseConnection(conn);
                const r = buffer.concat([]);
                buffer = [];
                const x = resolve;
                resolve = null;
                reject = null;
                complete = true;
                x(r);
            });
        }
        async function getter() {
            if (!started) {
                started = true;
                register();
            }
            function catcher(myResolve, myReject) {
                resolve = myResolve;
                reject = myReject;
                conn.resume();
            }
            if (aborted || complete) {
                const res = buffer.concat([]);
                buffer = [];
                return Promise.resolve({
                    fields,
                    rows: res.map(rowMapper.bind(null, fields)),
                });
            }
            const p = new Promise(catcher);
            const results = await p;
            return {
                fields,
                rows: results.concat([]).map(rowMapper.bind(null, fields)),
            };
        }
        function isComplete(rows) {
            return rows.rows.length === 0;
        }
        const rand = await random_crypto_string_1.default(4);
        return {
            definitionName: qi.definition.name,
            resultId: qi.requestId + rand,
            // We have a result set, this will call getter() until the result of
            // isComplete([THE_RESULT_OF_GETTER]) returns true. See https://github.com/forbesmyester/esqlate-promise-returning-function-to-generator
            result: esqlate_promise_returning_function_to_generator_1.default(getter, isComplete),
        };
    };
}
function getQueryRunner(logger) {
    for (const name of ["MYUSER", "MYPASSWORD", "MYDATABASE", "MYHOST"]) {
        if (!process.env.hasOwnProperty(name)) {
            throw new Error(`The eSQLate MySQL driver requires ["MYUSER", "MYPASSWORD", "MYDATABASE", "MYHOST"] as environmental variables. At least ${name} is missing`);
        }
    }
    const pool = mysql_1.createPool({
        host: process.env.MYHOST,
        user: process.env.MYUSER,
        database: process.env.MYDATABASE,
        password: process.env.MYPASSWORD,
        port: process.env.hasOwnProperty("MYPORT") ?
            parseInt(process.env.MYPORT, 10) :
            3306,
    });
    let connectionCount = 0;
    pool.on("acquire", () => {
        logger(logger_1.Level.INFO, "DATABASE", `Database Connection Count Incremented: ${++connectionCount} Connections`);
    });
    pool.on("release", () => {
        logger(logger_1.Level.INFO, "DATABASE", `Database Connection Count Decremented: ${--connectionCount} Connections`);
    });
    return Promise.resolve({
        queue: esqlate_queue_1.default(getEsqlateQueueWorker(pool)),
        demand: getDemandRunner(pool),
    });
}
exports.default = getQueryRunner;
