import Ajv from "ajv";
import { EsqlateDefinition } from "esqlate-lib";
import JSON5 from "json5";
import { Level, Logger } from "./logger";

import fs from "fs";
import path from "path";

export interface DefinitionListItem {
    name: string;
    title: string;
}
export type DefinitionList = DefinitionListItem[];

const ajv = new Ajv();
import * as schemaDefinition from "esqlate-lib/res/schema-definition.json";
const ajvValidateDefinition = ajv.compile(schemaDefinition);

async function sequentialPromises<T, R>(ar: T[], f: (t: T) => Promise<R|false>): Promise<R[]> {
    const ps = ar.map(f);
    const items = await Promise.all(ps);
    return items.filter((i) => i !== false) as R[];
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

export interface ReadDefinitionListDependencies { logger: Logger; }
export interface ReadDefinitionConfiguration {
    definitionDirectory: string;
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
                    logger(Level.FATAL, "DEFINITION", "Could not list definition");
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


