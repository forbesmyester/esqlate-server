import * as pg from "pg";
import { FieldDef, QueryArrayResult } from "pg";
import randCryptoString from "random-crypto-string";

import { EsqlateArgument, EsqlateDefinition, EsqlateErrorResult, EsqlateFieldDefinition, EsqlateParameter, EsqlateResult, EsqlateStatementNormalized, EsqlateSuccessResult, normalize } from "esqlate-lib";
import streamPromisesAsGenerator from "esqlate-promise-returning-function-to-generator";
import getEsqlateQueue from "esqlate-queue";
import { EsqlateQueueWorker } from "esqlate-queue";

import { Level, Logger } from "./logger";
import { DatabaseCursorResult, DatabaseInterface, DemandRunner, QueueItem, ResultCreated } from "./QueryRunner";


type Oid = FieldDef["dataTypeID"];


export interface PgQuery {
    text: string;
    values: any[];
}


/* tslint:disable */
const Cursor = require("pg-cursor");
/* tslint:enable */

// Private
export function pgQuery(statement: EsqlateStatementNormalized, inputValues: { [k: string]: any }): PgQuery {

    interface PgQueryExtra extends PgQuery {
        knownValues: string[];
    }

    function getSqlValue(p: EsqlateParameter) {
        if (p.empty_string_is_null && (inputValues[p.name] === "")) {
            return null;
        }
        return inputValues[p.name];
    }

    function reducer(acc: PgQueryExtra, ed: string | EsqlateParameter): PgQueryExtra {

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

    const { values, text } = statement.reduce(
        reducer,
        {
            text: "",
            values: [],
            knownValues: [],
        },
    );

    return { text, values };
}


// Private
export function getQuery(normalizedStatement: EsqlateStatementNormalized, serverParameters: EsqlateArgument[], parameters: EsqlateArgument[]) {

    function esqlateRequestCreationParameterToOb(acc: { [k: string]: any }, parameter: EsqlateArgument): { [k: string]: any } {
        const merger: { [k: string]: any } = {};
        merger[parameter.name] = parameter.value;
        return { ...acc, ...merger };
    }

    const inputValues: { [k: string]: any } = serverParameters.reduce(
        esqlateRequestCreationParameterToOb,
        (parameters || []).reduce(
            esqlateRequestCreationParameterToOb,
            {},
        ),
    );
    return pgQuery(normalizedStatement, inputValues);
}


// Private
function queryResultToEsqlateResult(dataTypeIDToName: (dataTypeID: Oid) => string, results: QueryArrayResult): EsqlateSuccessResult {
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
export function format(dataTypeIDToName: (dataTypeID: Oid) => string, promiseResults: Promise<QueryArrayResult>): Promise<EsqlateResult> {

    return promiseResults
        .then(queryResultToEsqlateResult.bind(null, dataTypeIDToName))
        .catch((e) => {
            return getEsqlateErrorResult(e);
        });

}


// Utility
export function getEsqlateErrorResult(e: Error & any): EsqlateErrorResult {
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
    const merge: {[k: string]: any} = {};
    for (const k of keys) {
        if (e[k]) {
            merge[k] = e[k];
        }
    }
    message = message + JSON.stringify(merge);
    return { status: "error", message };
}


function getLookupOid(pool: pg.Pool) {
    return pool.query("select typname, oid from pg_type order by oid")
        .then(({rows}) => {
            const m = new Map<Oid, string>();
            rows.forEach(({typname, oid}: {typname: string, oid: number}) => {
                m.set(oid, typname);
            });

            return function lookupOidFuncImpl(oid: Oid): string {
                if (!m.has(oid)) {
                    throw new Error("Could not PG OID " + oid);
                }
                return m.get(oid) as string;
            };

        });

}


function getDemandRunner(pool: pg.Pool, lookupOid: (oid: number) => string): DemandRunner {

    return async function demandRunner(
        definition: EsqlateDefinition,
        serverArguments: EsqlateArgument[],
        userArguments: EsqlateArgument[],
    ) {

        const normalized = normalize(
            definition.parameters,
            definition.statement,
        );

        const qry = getQuery(normalized, serverArguments, userArguments);

        return await format(lookupOid, pool.query({ ...qry, rowMode: "array" }));

    };

}


function getEsqlateQueueWorker(pool: pg.Pool, lookupOid: (oid: number) => string): EsqlateQueueWorker<QueueItem, ResultCreated> {

    const REQUEST_PER_TIME = 1024;

    return async function getEsqlateQueueWorkerImpl(qi) {

        const client: pg.PoolClient = await pool.connect();
        const normalized = normalize(
            qi.definition.parameters,
            qi.definition.statement,
        );
        const qry = getQuery(normalized, qi.serverParameters, qi.userParameters);
        const cursor: any = client.query(new Cursor(qry.text, qry.values, { rowMode: "array" }));
        let fields: EsqlateFieldDefinition[] = [];

        function end(err1?: Error) {
            cursor.close((err2: Error) => {
                client.release(err1 || err2);
            });
        }

        function isComplete(cursorResult: any): boolean {
            const r = (cursorResult.rows && cursorResult.rows.length) ? false : true;
            if (r) { end(); }
            return r;
        }

        function getter(): Promise<DatabaseCursorResult> {
            const p: Promise<DatabaseCursorResult> = new Promise((resolve, reject) => {
                cursor.read(REQUEST_PER_TIME, (err: Error, rows: any[], result: pg.QueryArrayResult) => {
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

        const rand = await randCryptoString(4);

        return {
            definitionName: qi.definition.name,
            resultId: qi.requestId + rand,
            // We have a result set, this will call getter() until the result of
            // isComplete([THE_RESULT_OF_GETTER]) returns true. See https://github.com/forbesmyester/esqlate-promise-returning-function-to-generator
            result: streamPromisesAsGenerator(getter, isComplete),
        };

    };

}


export default function getQueryRunner(parallelism: number = 1, logger: Logger): Promise<DatabaseInterface> {
    const pool = new pg.Pool();
    let connectionCount = 0;
    pool.on("connect", () => {
        logger(Level.INFO, "DATABASE", `Database Connection Count Incremented: ${++connectionCount} Connections`);
    });
    pool.on("connect", () => {
        logger(Level.INFO, "DATABASE", `Database Connection Count Decremented: ${--connectionCount} Connections`);
    });
    pool.on("error", (e) => {
        logger(Level.FATAL, "DATABASE", `Database Error: ${e.message}`);
    });
    return getLookupOid(pool)
        .then((lookupOid) => {
            return {
                queue: getEsqlateQueue(getEsqlateQueueWorker(pool, lookupOid), parallelism),
                demand: getDemandRunner(pool, lookupOid),
            };
        });
}
