"use strict";
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
    result["default"] = mod;
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const pg = __importStar(require("pg"));
const random_crypto_string_1 = __importDefault(require("random-crypto-string"));
const esqlate_lib_1 = require("esqlate-lib");
const esqlate_promise_returning_function_to_generator_1 = __importDefault(require("esqlate-promise-returning-function-to-generator"));
const esqlate_queue_1 = __importDefault(require("esqlate-queue"));
const logger_1 = require("./logger");
/* tslint:disable */
const Cursor = require("pg-cursor");
/* tslint:enable */
// Private
function pgQuery(statement, inputValues) {
    function getSqlValue(p) {
        if (p.empty_string_is_null && (inputValues[p.name] === "")) {
            return null;
        }
        return inputValues[p.name];
    }
    function reducer(acc, ed) {
        if (typeof ed === "string") {
            return {
                text: `${acc.text}${ed.split("$").join("$$")}`,
                values: acc.values,
                knownValues: acc.knownValues,
            };
        }
        if (acc.knownValues.indexOf(ed.name) === -1) {
            acc.values = acc.values.concat(getSqlValue(ed));
            acc.knownValues = acc.knownValues.concat([ed.name]);
        }
        return {
            text: acc.text + "$" + (acc.knownValues.indexOf(ed.name) + 1),
            values: acc.values,
            knownValues: acc.knownValues,
        };
    }
    const { values, text } = statement.reduce(reducer, {
        text: "",
        values: [],
        knownValues: [],
    });
    return { text, values };
}
exports.pgQuery = pgQuery;
// Private
function getQuery(normalizedStatement, serverParameters, parameters) {
    function getEsqlateStatementNullableVariables() {
        return Array.from(new Set(normalizedStatement
            .filter((ns) => ns && ns.empty_string_is_null)
            .map((ns) => ns.name)));
    }
    function esqlateRequestCreationParameterToOb(acc, parameter) {
        const merger = {};
        merger[parameter.name] = parameter.value;
        return { ...acc, ...merger };
    }
    const nullable = getEsqlateStatementNullableVariables();
    const sqlValuesFromUser = serverParameters.reduce(esqlateRequestCreationParameterToOb, (parameters || []).reduce(esqlateRequestCreationParameterToOb, {}));
    const sqlValues = nullable
        .filter((n) => !sqlValuesFromUser.hasOwnProperty(n))
        .reduce((acc, n) => {
        acc[n] = null;
        return acc;
    }, sqlValuesFromUser);
    return pgQuery(normalizedStatement, sqlValues);
}
exports.getQuery = getQuery;
// Private
function queryResultToEsqlateResult(dataTypeIDToName, results) {
    const fields = results.fields.map((f) => {
        return {
            name: f.name,
            type: dataTypeIDToName(f.dataTypeID),
        };
    });
    return {
        fields,
        rows: results.rows,
        status: "complete",
    };
}
// Private
function format(dataTypeIDToName, promiseResults) {
    return promiseResults
        .then(queryResultToEsqlateResult.bind(null, dataTypeIDToName))
        .catch((e) => {
        return getEsqlateErrorResult(e);
    });
}
exports.format = format;
// Utility
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
exports.getEsqlateErrorResult = getEsqlateErrorResult;
function getLookupOid(pool) {
    return pool.query("select typname, oid from pg_type order by oid")
        .then(({ rows }) => {
        const m = new Map();
        rows.forEach(({ typname, oid }) => {
            m.set(oid, typname);
        });
        return function lookupOidFuncImpl(oid) {
            if (!m.has(oid)) {
                throw new Error("Could not PG OID " + oid);
            }
            return m.get(oid);
        };
    });
}
function getDemandRunner(pool, lookupOid) {
    return async function demandRunner(definition, serverArguments, userArguments) {
        const normalized = esqlate_lib_1.normalize(definition.parameters, definition.statement);
        const qry = getQuery(normalized, serverArguments, userArguments);
        return await format(lookupOid, pool.query({ ...qry, rowMode: "array" }));
    };
}
function getEsqlateQueueWorker(pool, lookupOid) {
    const REQUEST_PER_TIME = 1024;
    return async function getEsqlateQueueWorkerImpl(qi) {
        const client = await pool.connect();
        const normalized = esqlate_lib_1.normalize(qi.definition.parameters, qi.definition.statement);
        const qry = getQuery(normalized, qi.serverParameters, qi.userParameters);
        const cursor = client.query(new Cursor(qry.text, qry.values, { rowMode: "array" }));
        let fields = [];
        function end(err1) {
            cursor.close((err2) => {
                client.release(err1 || err2);
            });
        }
        function isComplete(cursorResult) {
            const r = (cursorResult.rows && cursorResult.rows.length) ? false : true;
            if (r) {
                end();
            }
            return r;
        }
        function getter() {
            const p = new Promise((resolve, reject) => {
                cursor.read(REQUEST_PER_TIME, (err, rows, result) => {
                    if (err) {
                        end(err);
                        return reject(getEsqlateErrorResult(err));
                    }
                    if (rows.length === 0) {
                        return resolve({ fields, rows: [] });
                    }
                    const x = queryResultToEsqlateResult(lookupOid, result);
                    fields = x.fields;
                    resolve({ fields: x.fields, rows: x.rows });
                });
            });
            return p;
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
function getQueryRunner(parallelism = 1, logger) {
    const pool = new pg.Pool();
    pool.on("connect", () => {
        logger(logger_1.Level.INFO, "DATABASE", `Database Connection Count Incremented: ${pool.totalCount} Connections (with ${pool.waitingCount} waiting and ${pool.idleCount} idle)`);
    });
    pool.on("remove", () => {
        logger(logger_1.Level.INFO, "DATABASE", `Database Connection Count Decremented: ${pool.totalCount} Connections (with ${pool.waitingCount} waiting and ${pool.idleCount} idle)`);
    });
    pool.on("error", (e) => {
        logger(logger_1.Level.FATAL, "DATABASE", `Database Error: ${e.message}`);
    });
    return getLookupOid(pool)
        .then((lookupOid) => {
        return {
            queue: esqlate_queue_1.default(getEsqlateQueueWorker(pool, lookupOid), parallelism),
            demand: getDemandRunner(pool, lookupOid),
            closePool: () => pool.end(),
            worker: getEsqlateQueueWorker(pool, lookupOid),
        };
    });
}
exports.default = getQueryRunner;
