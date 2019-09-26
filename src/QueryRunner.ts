import * as pg from "pg";
import { FieldDef, QueryResult } from "pg";
import randCryptoString from "random-crypto-string";

import { EsqlateDefinition, EsqlateErrorResult, EsqlateRequestCreationParameter, EsqlateRequestCreationParameterItem, EsqlateResult, EsqlateStatementNormalized, EsqlateVariable, normalize } from "esqlate-lib";
import { EsqlateQueueWorker } from "esqlate-queue";

import {ResultId} from "./persistence";


type Oid = FieldDef["dataTypeID"];


export interface PgQuery {
    text: string;
    values: any[];
}


export interface ResultCreated {
    definitionName: EsqlateDefinition["name"];
    resultId: ResultId;
    result: EsqlateResult;
}


export interface QueueItem {
    definition: EsqlateDefinition;
    requestId: string;
    serverParameters: EsqlateRequestCreationParameter;
    userParameters: EsqlateRequestCreationParameter;
}


export type DemandRunner = (
    definition: EsqlateDefinition,
    serverParameters: EsqlateRequestCreationParameter,
    parameters: EsqlateRequestCreationParameter
    ) => Promise<EsqlateResult>;


export function pgQuery(statement: EsqlateStatementNormalized, inputValues: {[k: string]: any}): PgQuery {

    interface PgQueryExtra extends PgQuery {
        knownValues: string[];
    }

    function reducer(acc: PgQueryExtra, ed: string | EsqlateVariable): PgQueryExtra {

        if (typeof ed === "string") {
            return {
                text: `${acc.text}${ed.split("$").join("$$")}`,
                values: acc.values,
                knownValues: acc.knownValues,
            };
        }

        if (acc.knownValues.indexOf(ed.name) === -1) {
            acc.values = acc.values.concat(inputValues[ed.name]);
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


export function getQuery(normalizedStatement: EsqlateStatementNormalized, serverParameters: EsqlateRequestCreationParameter, parameters: EsqlateRequestCreationParameter) {

    function esqlateRequestCreationParameterToOb(acc: { [k: string]: any }, parameter: EsqlateRequestCreationParameterItem): { [k: string]: any } {
        const merger: { [k: string]: any } = {};
        merger[parameter.field_name] = parameter.field_value;
        return { ...acc, ...merger };
    }

    const inputValues: { [k: string]: any } = serverParameters.reduce(
        esqlateRequestCreationParameterToOb,
        parameters.reduce(
            esqlateRequestCreationParameterToOb,
            {},
        ),
    );
    return pgQuery(normalizedStatement, inputValues);
}


export function format(dataTypeIDToName: (dataTypeID: Oid) => string, promiseResults: Promise<QueryResult>): Promise<EsqlateResult> {

    function success(results: QueryResult): EsqlateResult {
        const fields = results.fields.map((f) => {
            return {
                field_name: f.name,
                field_type: dataTypeIDToName(f.dataTypeID),
            };
        });
        const rows = results.rows.map((row) => {
            return fields.map((fn) => row[fn.field_name]);
        });
        return {
            fields,
            rows,
            status: "complete",
        };
    }

    function fail(e: Error & any): EsqlateErrorResult {
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

    return promiseResults.then(success).catch((e) => {
        return fail(e);
    });

}


export function getLookupOid(pool: pg.Pool) {
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


export function getDemandRunner(pool: pg.Pool, lookupOid: (oid: number) => string): DemandRunner {

    return async function demandRunner(definition: EsqlateDefinition, serverParameters: EsqlateRequestCreationParameter, parameters: EsqlateRequestCreationParameter) {

        const normalized = normalize(
            definition.variables,
            definition.statement,
        );

        const qry = getQuery(normalized, serverParameters, parameters);

        return await format(lookupOid, pool.query(qry));

    };

}


export function getEsqlateQueueWorker(pool: pg.Pool, lookupOid: (oid: number) => string): EsqlateQueueWorker<QueueItem, ResultCreated> {

    return async function getEsqlateQueueWorkerImpl(qi) {

        const demandRunner = getDemandRunner(pool, lookupOid);

        const result = await demandRunner(
            qi.definition,
            qi.serverParameters,
            qi.userParameters
        );

        const rand = await randCryptoString(4);

        return {
            definitionName: qi.definition.name,
            result,
            resultId: qi.requestId + rand,
        };
    };

}
