import { EsqlateArgument, EsqlateDefinition, EsqlateErrorResult, EsqlateFieldDefinition, EsqlateParameter, EsqlateResult, EsqlateStatementNormalized, EsqlateSuccessResult, EsqlateSuccessResultRow, normalize} from "esqlate-lib";
import streamPromisesAsGenerator from "esqlate-promise-returning-function-to-generator";
import getEsqlateQueue, { EsqlateQueueWorker } from "esqlate-queue";
import { createPool, FieldInfo, format, Pool, PoolConnection } from "mysql";
import randCryptoString from "random-crypto-string";

import { Level, Logger } from "./logger";
import { DatabaseCursorResult, DatabaseInterface, DemandRunner, QueueItem, ResultCreated } from "./QueryRunner";

interface MysqlRow { [k: string]: any; }

function rowMapper(fields: EsqlateFieldDefinition[], rdp: MysqlRow): EsqlateSuccessResultRow {
    return fields.map(({ name }) => {
        return rdp.hasOwnProperty(name) ? rdp[name] : null;
    });
}

export function getQuery(normalizedStatement: EsqlateStatementNormalized, serverParameters: EsqlateArgument[], parameters: EsqlateArgument[]): {text: string, paramValues: any[]}  {

    const inputValues: { [k: string]: any } =
        parameters.concat(serverParameters).reduce(
            (acc, p) => {
                return {...acc, [p.name]: p.value};
            },
            {},
    );

    function getSqlValue(p: EsqlateParameter) {
        if (p.empty_string_is_null && (inputValues[p.name] === "")) {
            return null;
        }
        return inputValues[p.name];
    }

    function reducer({ text, paramValues }: { text: string, paramValues: string[] }, ed: string | EsqlateParameter) {

        if (typeof ed === "string") {
            text = `${text}${ed}`;
        } else {
            paramValues = paramValues.concat(getSqlValue(ed));
            text = text + "?";
        }

        return { text, paramValues };
    }

    return normalizedStatement.reduce(
        reducer,
        {
            text: "",
            paramValues: [],
        },
    );

}

function getEsqlateErrorResult(e: Error & any): EsqlateErrorResult {
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
    const merge: { [k: string]: any } = {};
    for (const k of keys) {
        if (e[k]) {
            merge[k] = e[k];
        }
    }
    message = message + JSON.stringify(merge);
    return { status: "error", message };
}

function getDemandRunner(pool: Pool): DemandRunner {
    return demandRunner.bind(null, pool);
}

function demandRunner(pool: Pool, definition: EsqlateDefinition, serverParameters: EsqlateArgument[], userParameters: EsqlateArgument[]): Promise<EsqlateResult> {

    const normalized = normalize(
        definition.parameters,
        definition.statement,
    );

    const getQueryOut = getQuery(normalized, serverParameters, userParameters);

    return demandRunnerImpl(pool, getQueryOut.text, getQueryOut.paramValues)
        .catch(getEsqlateErrorResult);

}

function demandRunnerImpl(pool: Pool, inSql: string, values: any[]): Promise<EsqlateSuccessResult> {
    return new Promise((resolve, reject) => {

        const sql = format(inSql, values);
        const fieldsFound: Set<string> = new Set();
        const fields: EsqlateFieldDefinition[] = [];

        const qry = {
            sql,
            typeCast: (field: FieldInfo & { type: string }, next: () => void) => {
                if (!fieldsFound.has(field.name)) {
                    fields.push({ name: field.name, type: field.type });
                    fieldsFound.add(field.name);
                }
                return next();
            },
        };

        pool.query(qry, (err, res: MysqlRow[]) => {
            if (err) { return reject(err); }
            resolve({
                fields,
                rows: res.map(rowMapper.bind(null, fields)),
                status: "complete",
            });
        });
    });
}

function getEsqlateQueueWorker(pool: Pool): EsqlateQueueWorker<QueueItem, ResultCreated> {

    const REQUEST_PER_TIME = 1024;

    return async function getEsqlateQueueWorkerImpl(qi) {

        type BufferResolve = (rows: MysqlRow[]) => void;
        type BufferReject = (reason: any) => void;

        const fieldsFound: Set<string> = new Set();
        let aborted = false;
        let complete = false;
        const fields: EsqlateFieldDefinition[] = [];
        let started = false;

        let buffer: MysqlRow[] = [];
        let resolve: BufferResolve | null = null;
        let reject: BufferReject | null = null;

        const normalized = normalize(
            qi.definition.parameters,
            qi.definition.statement,
        );

        const conn: PoolConnection = await new Promise((myResolve, myReject) => {
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

            result.on("result", (row: any) => {
                buffer.push(row);
                if (complete || aborted || (buffer.length >= REQUEST_PER_TIME)) {
                    conn.pause();
                    const r = buffer.concat([]);
                    buffer = [];
                    const x = resolve;
                    resolve = null;
                    reject = null;
                    (x as BufferResolve)(r);
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
                (x as BufferResolve)(r);
            });
        }

        async function getter(): Promise<DatabaseCursorResult> {

            if (!started) {
                started = true;
                register();
            }

            function catcher(myResolve: (rows: MysqlRow[]) => void, myReject: (rows: MysqlRow[]) => void) {
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

            const p: Promise<MysqlRow[]> = new Promise(catcher);

            const results = await p;
            return {
                fields,
                rows: results.concat([]).map(rowMapper.bind(null, fields)),
            };
        }

        function isComplete(rows: DatabaseCursorResult): boolean {
            return rows.rows.length === 0;
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


export default function getQueryRunner(logger: Logger): Promise<DatabaseInterface> {
    for (const name of ["MYUSER", "MYPASSWORD", "MYDATABASE", "MYHOST"]) {
        if (!process.env.hasOwnProperty(name)) {
            throw new Error(`The eSQLate MySQL driver requires ["MYUSER", "MYPASSWORD", "MYDATABASE", "MYHOST"] as environmental variables. At least ${name} is missing`);
        }
    }
    const pool = createPool({
        host: process.env.MYHOST,
        user: process.env.MYUSER,
        database: process.env.MYDATABASE,
        password: process.env.MYPASSWORD,
    });
    let connectionCount = 0;
    pool.on("acquire", () => {
        logger(Level.INFO, "DATABASE", `Database Connection Count Incremented: ${++connectionCount} Connections`);
    });
    pool.on("release", () => {
        logger(Level.INFO, "DATABASE", `Database Connection Count Decremented: ${--connectionCount} Connections`);
    });
    return Promise.resolve({
        queue: getEsqlateQueue(getEsqlateQueueWorker(pool)),
        demand: getDemandRunner(pool),
    });
}
