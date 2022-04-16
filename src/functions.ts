import Ajv from "ajv";
import assert = require("assert");
import JSON5 from "json5";
import { join as pathJoin } from "path";
import randCryptoString from "random-crypto-string";

import { EsqlateArgument, EsqlateDefinition, EsqlateRequestCreation } from "esqlate-lib";
import * as schemaRequestCreation from "esqlate-lib/res/schema-request-creation.json";

import { EsqlateErrorInvalidDefinition, EsqlateErrorInvalidRequestBody, EsqlateErrorMissingDefinition, EsqlateErrorMissingVariables, Level, Logger } from "./logger";
import { Persistence, safeDefinitionName } from "./persistence";
import { DemandRunner } from "./QueryRunner";

import fs from "fs";
import path from "path";

export function unixPathJoin(...ar: string[]): string {
    return ar.join("/").replace(/\/\/+/, "/");
}

export interface ServiceInformation {
    getApiRoot: () => string;
}

export interface DefinitionListItem {
    name: string;
    title: string;
}
export type DefinitionList = DefinitionListItem[];

const ajv = new Ajv();
import * as schemaDefinition from "esqlate-lib/res/schema-definition.json";
import {EsqlateQueue} from "esqlate-queue";
import {QueueItem, ResultCreated} from "./QueryRunner";

export interface Input<X> {
    params: { [k: string]: string };
    body: X;
}

export interface ServerVariableRequester {
    listServerVariable: (req: Input<any>) => string[];
    getServerVariable: (req: Input<any>, name: string) => EsqlateArgument["value"];
}

const ajvValidateDefinition = ajv.compile(schemaDefinition);

async function sequentialPromises<T, R>(ar: T[], f: (t: T) => Promise<R|false>): Promise<R[]> {
    const ps = ar.map(f);
    const items = await Promise.all(ps);
    return items.filter((i) => i !== false) as R[];
}


const ajvValidateRequestCreation = ajv.compile(schemaRequestCreation);

export type LoadDefinition = (definitionName: string) => Promise<EsqlateDefinition>;
export interface NeedsPersistence { persistence: Persistence; }
export interface NeedsServiceInformation { serviceInformation: ServiceInformation; }
export interface NeedsQueue { queue: EsqlateQueue<QueueItem, ResultCreated>; }
export interface NeedsServerVariableRequester { serverVariableRequester: ServerVariableRequester; }
export interface NeedsDemandRunner { demandRunner: DemandRunner; }
export interface NeedsLoadDefinition { loadDefinition: LoadDefinition; }

export function getVariables({ getApiRoot }: ServiceInformation, serverVariableRequester: ServerVariableRequester, definitionParameters: EsqlateDefinition["parameters"], req: Input<EsqlateRequestCreation>): EsqlateArgument[] {

    function get(): EsqlateArgument[] {
        const serverVariables: EsqlateArgument[] = serverVariableRequester.listServerVariable(req).map(
            (name) => ({ name, value: serverVariableRequester.getServerVariable(req, name) }),
        );

        return serverVariables.reduce(
            (acc: EsqlateArgument[], sv) => {
                const r = acc.filter((a) => !(("" + a.name) === ("" + sv.name)));
                r.push(sv);
                return r;
            },
            (req.body as EsqlateRequestCreation).arguments.concat([]),
        );
    }

    function getMissingVariables(args: EsqlateArgument[]) {
        const available = new Set(args.map((rb) => rb.name));
        return definitionParameters
            .filter((reqd) => {
                return !available.has(reqd.name);
            })
            .filter((emptyStringIsNull) => !emptyStringIsNull.empty_string_is_null)
            .map(({ name }) => name);
    }


    assert(req.params.hasOwnProperty("definitionName"), "Missing request param definitionName");

    const definitionName = req.params.definitionName;
    const valid = ajvValidateRequestCreation(req.body);

    if (!valid) {
        const errors = ajvValidateRequestCreation.errors;
        const msg = unixPathJoin(
            getApiRoot(),
            "request",
            definitionName,
        ) + ": " + JSON.stringify(errors);
        throw new EsqlateErrorInvalidRequestBody(msg);
    }

    const variables = get();

    const missingVariables = getMissingVariables(variables);
    if (missingVariables.length) {
        const errorMsg = "Missing Variables: " + JSON.stringify(missingVariables);
        const msg = unixPathJoin(
            getApiRoot(),
            "request",
            definitionName,
        ) + ": " + JSON.stringify(errorMsg);
        throw new EsqlateErrorMissingVariables(msg);
    }

    return variables;

}


function certifyDefinition(logger: Logger, loggerLevel: Level, fullPath: string, data: string): DefinitionListItem | false {

    let json: EsqlateDefinition;

    try {
        json = JSON5.parse(data);
    } catch (e) {
        logger(loggerLevel, "DEFINITION", `Could not unserialize definition ${fullPath}`);
        throw new Error(`Could not unserialize definition ${fullPath}`);
    }

    const valid = ajvValidateDefinition(json);
    if (!valid) {
        const errors = ajvValidateDefinition.errors;
        logger(loggerLevel, "DEFINITION", `Definition ${fullPath} has errors ${JSON.stringify(errors)}`);
        throw new Error(`Definition ${fullPath} has errors ${JSON.stringify(errors)}`);
    }

    if (json.name !== path.basename(fullPath).replace(/\.json5?$/, "")) {
        logger(loggerLevel, "DEFINITION", `Definition ${fullPath} has different name to filename`);
        throw new Error(`Definition ${fullPath} has different name to filename`);
    }

    if (
        (path.basename(fullPath).substring(0, 1) !== "_") &&
        hasNoStaticParams(json)
    ) {
        return { title: json.title, name: json.name };
    }

    return false;

}

function hasNoStaticParams(def: EsqlateDefinition): boolean {
    return !def.parameters.some((param) => param.type === "static");
}

export interface HasDefinitionDirectory { definitionDirectory: string; }

export interface ReadDefinitionListDependencies { logger: Logger; }
export interface ReadDefinitionConfiguration extends HasDefinitionDirectory {
    loggerLevel: Level;
    knownDefinitions: string[];
}


export async function readDefinitionList({ logger }: ReadDefinitionListDependencies, { loggerLevel, knownDefinitions, definitionDirectory }: ReadDefinitionConfiguration): Promise<DefinitionList> {

    function readFile(fullPath: string): Promise<{ data: string, fullPath: string}> {

        return new Promise((resolve, reject) => {
            fs.readFile(fullPath, { encoding: "utf8" }, (readFileErr, data) => {
                if (readFileErr) {
                    logger(loggerLevel, "DEFINITION", `Could not read definition ${fullPath}`);
                    return reject(`Could not read filename ${fullPath}`);
                }
                resolve({ data, fullPath });
            });
        });

    }

    function certify({ data, fullPath }: { data: string, fullPath: string }): Promise<DefinitionListItem | false> {
        return Promise.resolve(certifyDefinition(
            logger,
            loggerLevel,
            fullPath,
            data,
        ));
    }

    function myReadDir(): Promise<string[]> {
        return new Promise((resolve, reject) => {
            fs.readdir(definitionDirectory, (readDirErr, filenames) => {
                if (readDirErr) {
                    logger(Level.FATAL, "DEFINITION", "Could not list definition in (" + definitionDirectory + ")");
                    reject("Could not list definition");
                }
                resolve(filenames);
            });
        });
    }

    const fullPaths = (await myReadDir())
        .map((filename) => {
            return path.join(definitionDirectory, filename);
        })
        .filter((fp) => path.basename(fp).substring(0, 1) !== "_")
        .filter((fp) => knownDefinitions.indexOf(fp) === -1);
    const datas = await sequentialPromises(fullPaths, readFile);
    return sequentialPromises(datas, certify);

}


export function getLoadDefinition(conf: HasDefinitionDirectory) {
    return function loadDefinition(definitionName: string): Promise<EsqlateDefinition> {

        let errCount = 0;
        let sent = false;

        return new Promise((resolve, reject) => {
            function process(err: any, data: string) {
                if (sent) { return; }

                if ((err) && (errCount++ > 0) && (sent === false)) {
                    sent = true;
                    return reject(new EsqlateErrorMissingDefinition(`${definitionName}`));
                }
                if (err) { return; }

                let j: EsqlateDefinition;
                try {
                    j = JSON5.parse(data);
                } catch (e) {
                    errCount = errCount + 1;
                    sent = true;
                    return reject(new EsqlateErrorInvalidDefinition(`${definitionName}`));
                }
                sent = true;
                resolve(j);
            }

            fs.readFile(
                pathJoin(conf.definitionDirectory, definitionName + ".json5"),
                { encoding: "utf8" },
                process,
            );
            fs.readFile(
                pathJoin(conf.definitionDirectory, definitionName + ".json"),
                { encoding: "utf8" },
                process,
            );
        });

    };
}

export type RequestPath = string;

export type CreateRequestDeps = NeedsPersistence & NeedsServiceInformation & NeedsQueue & NeedsServerVariableRequester & NeedsLoadDefinition;

export function createRequestSideEffects({ serverVariableRequester, loadDefinition, persistence, queue, serviceInformation: { getApiRoot } }: CreateRequestDeps, req: Input<EsqlateRequestCreation>): Promise<{ definition: EsqlateDefinition, variables: EsqlateArgument[], requestId: string }> {

    let definition: EsqlateDefinition;
    let variables: EsqlateArgument[];
    const definitionName = safeDefinitionName(req.params.definitionName);

    return loadDefinition(definitionName)
        .then((def) => { definition = def; })
            .then(() => {
                variables = getVariables(
                    { getApiRoot },
                    serverVariableRequester,
                    definition.parameters,
                    req,
                );
            })
        .then(() => randCryptoString(8) )
        .then((requestId) => {
            return persistence.createRequest(
                definitionName,
                requestId,
                variables,
            ).then(() => ({ requestId, definition, variables }));
        })

}

export function createRequestFile({ serverVariableRequester, loadDefinition, persistence, queue, serviceInformation: { getApiRoot } }: CreateRequestDeps, req: Input<EsqlateRequestCreation>): Promise<RequestPath> {

    let definition: EsqlateDefinition;
    let variables: EsqlateArgument[];
    const definitionName = safeDefinitionName(req.params.definitionName);

    return createRequestSideEffects({ serverVariableRequester, loadDefinition, persistence, queue, serviceInformation: { getApiRoot } }, req)
        .then(({ definition, variables, requestId }) => {
            return unixPathJoin(getApiRoot(), "request", definitionName, requestId);
        });
}


export function createRequest({ serverVariableRequester, loadDefinition, persistence, queue, serviceInformation: { getApiRoot } }: CreateRequestDeps, req: Input<EsqlateRequestCreation>): Promise<RequestPath> {

    let definition: EsqlateDefinition;
    let variables: EsqlateArgument[];
    const definitionName = safeDefinitionName(req.params.definitionName);

    return createRequestSideEffects({ serverVariableRequester, loadDefinition, persistence, queue, serviceInformation: { getApiRoot } }, req)
        .then(({ definition, variables, requestId }) => {
            // TODO: Pass in server variables properly
            const queueItem: QueueItem = {
                definition,
                requestId,
                serverParameters: [],
                userParameters: variables,
            };
            queue.push(queueItem);
            return requestId;
        })
        .then((requestId) => {
            return unixPathJoin(getApiRoot(), "request", definitionName, requestId);
        });
}


export function processCmdlineArgumentsToEsqlateArgument(args: string[]): EsqlateArgument[] {
    return args.reduce(
        (acc, arg) => {
            if (arg.substring(0, 1) === "[") {
                try {
                    const ar = JSON.parse(arg);
                    if (ar.length < 1) {
                        throw new Error("TOO SHORT");
                    }
                    const toAdd: EsqlateArgument = {
                        name: "" + ar[0],
                        value: ar[1],
                    };
                    acc.push(toAdd);
                } catch (e) {
                    throw new Error(`Error processing parameter ${arg}.\n\nThe error was ${e.message}`);
                }
                return acc;
            }
            return acc;
        },
        [] as EsqlateArgument[]
    );
}
