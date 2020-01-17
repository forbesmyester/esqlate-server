"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const esqlate_promise_returning_function_to_generator_1 = __importDefault(require("esqlate-promise-returning-function-to-generator"));
const random_crypto_string_1 = __importDefault(require("random-crypto-string"));
/* tslint:disable */
const Cursor = require("pg-cursor");
/* tslint:enable */
const esqlate_lib_1 = require("esqlate-lib");
const REQUEST_PER_TIME = 1024;
function pgQuery(statement, inputValues) {
    function getSqlValue(p) {
        if (p.empty_string_is_null && (inputValues[p.name] == "")) {
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
function getQuery(normalizedStatement, serverParameters, parameters) {
    function esqlateRequestCreationParameterToOb(acc, parameter) {
        const merger = {};
        merger[parameter.name] = parameter.value;
        return { ...acc, ...merger };
    }
    const inputValues = serverParameters.reduce(esqlateRequestCreationParameterToOb, (parameters || []).reduce(esqlateRequestCreationParameterToOb, {}));
    return pgQuery(normalizedStatement, inputValues);
}
exports.getQuery = getQuery;
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
function format(dataTypeIDToName, promiseResults) {
    return promiseResults
        .then(queryResultToEsqlateResult.bind(null, dataTypeIDToName))
        .catch((e) => {
        return getEsqlateErrorResult(e);
    });
}
exports.format = format;
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
exports.getLookupOid = getLookupOid;
function getDemandRunner(pool, lookupOid) {
    return async function demandRunner(definition, serverArguments, userArguments) {
        const normalized = esqlate_lib_1.normalize(definition.parameters, definition.statement);
        const qry = getQuery(normalized, serverArguments, userArguments);
        return await format(lookupOid, pool.query({ ...qry, rowMode: "array" }));
    };
}
exports.getDemandRunner = getDemandRunner;
function getEsqlateQueueWorker(pool, lookupOid) {
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
            return new Promise((resolve, reject) => {
                cursor.read(REQUEST_PER_TIME, (err, rows, result) => {
                    if (err) {
                        end(err);
                        return reject(err);
                    }
                    if (rows.length === 0) {
                        return resolve({ fields, rows: [] });
                    }
                    const x = queryResultToEsqlateResult(lookupOid, result);
                    fields = x.fields;
                    resolve({ fields: x.fields, rows: x.rows });
                });
            });
        }
        const rand = await random_crypto_string_1.default(4);
        return {
            definitionName: qi.definition.name,
            resultId: qi.requestId + rand,
            result: esqlate_promise_returning_function_to_generator_1.default(getter, isComplete),
        };
    };
}
exports.getEsqlateQueueWorker = getEsqlateQueueWorker;
