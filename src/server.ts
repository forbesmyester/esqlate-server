import Ajv from "ajv";
import bodyParser from "body-parser";
import cors, { CorsOptions } from "cors";
import express, { Express, NextFunction, Request, Response } from "express";
import fs from "fs";
import path from "path";

import { EsqlateDefinition } from "esqlate-lib";
import * as schemaDefinition from "esqlate-lib/res/schema-definition.json";
import { EsqlateQueue } from "esqlate-queue";
import JSON5 from "json5";
import logger, { Level } from "./logger";
import { captureRequestStart, createRequest, getCaptureRequestEnd, getCaptureRequestErrorHandler, getDefinition, getRequest, getResult, getResultCsv, loadDefinition, outstandingRequestId, runDemand, ServerVariableRequester, ServiceInformation } from "./middleware";
import nextWrap, { NextWrapDependencies } from "./nextWrap";
import { FilesystemPersistence, Persistence } from "./persistence";
import { DatabaseType, DemandRunner, getQueryRunner, QueueItem, ResultCreated } from "./QueryRunner";

if (!process.env.hasOwnProperty("LISTEN_PORT")) {
    logger(Level.FATAL, "STARTUP", "no LISTEN_PORT environmental variable defined");
}

const DEFINITION_DIRECTORY: string = process.env.DEFINITION_DIRECTORY || (__dirname + "/example_definition");

const ajv = new Ajv();
const ajvValidateDefinition = ajv.compile(schemaDefinition);

interface DefinitionListItem {
    name: string;
    title: string;
}
type DefinitionList = DefinitionListItem[];
type DefinitionMap = Map<string, string>;
const definitionMap: DefinitionMap = new Map();

function hasNoStaticParams(def: EsqlateDefinition): boolean {
    return !def.parameters.some((param) => param.type === "static");
}

function certifyDefinition(loggerLevel: Level, fullPath: string, data: string): DefinitionListItem | false {

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

function sequentialPromises<T, R>(ar: T[], f: (t: T) => Promise<R|false>): Promise<R[]> {
    const ps = ar.map(f);
    return Promise.all(ps)
        .then((items) => {
            return items.filter((i) => i !== false) as R[];
        });
}

function readDefinitionList(level: Level, knownDefinitions: string[]): Promise<DefinitionList> {

    function readFile(fullPath: string): Promise<{ data: string, fullPath: string}> {

        return new Promise((resolve, reject) => {
            fs.readFile(fullPath, { encoding: "utf8" }, (readFileErr, data) => {
                if (readFileErr) {
                    logger(level, "DEFINITION", `Could not read definition ${fullPath}`);
                    return reject(`Could not read filename ${fullPath}`);
                }
                resolve({ data, fullPath });
            });
        });

    }

    function certify({ data, fullPath }: { data: string, fullPath: string }): Promise<DefinitionListItem | false> {
        return Promise.resolve(certifyDefinition(
            level,
            fullPath,
            data,
        ));
    }

    function myReadDir(): Promise<string[]> {
        return new Promise((resolve, reject) => {
            fs.readdir(DEFINITION_DIRECTORY, (readDirErr, filenames) => {
                if (readDirErr) {
                    logger(Level.FATAL, "DEFINITION", "Could not list definition");
                    reject("Could not list definition");
                }
                resolve(filenames);
            });
        });
    }

    return myReadDir()
        .then((filenames) => {

            const fullPaths = filenames
                .map((filename) => {
                    return path.join(DEFINITION_DIRECTORY, filename);
                })
                .filter((fp) => path.basename(fp).substring(0, 1) !== "_")
                .filter((fp) => knownDefinitions.indexOf(fp) === -1);

            return sequentialPromises(fullPaths, readFile)
                .then((datas) => {
                    return sequentialPromises(datas, certify);
                });
        });

}

function patchDefinitionMap(level: Level) {
    return readDefinitionList(level, Array.from(definitionMap.keys()))
        .then((dl: DefinitionList) => {
            dl.forEach(({ name, title }) => {
                definitionMap.set(name, title);
            });
        })
        .catch((_e) => {
            // Logs already output.
        });
}

patchDefinitionMap(Level.FATAL);

async function writeResults(persistence: Persistence, queue: EsqlateQueue<QueueItem, ResultCreated>) {
    try {
        for await (const rc of queue.results()) {
            await persistence.createResult(
                rc.definitionName,
                rc.resultId,
                rc.result,
            );
        }
    } catch (e) {
        logger(Level.WARN, "QUEUE", e.message);
    }
}

const serverVariableRequester: ServerVariableRequester = {
    listServerVariable: (_req: Request) => {
        return [];
    },
    getServerVariable: (_req: Request, _name: string) => {
        return "";
    },
};


function setupApp(
    persistence: Persistence,
    serviceInformation: ServiceInformation,
    queue: EsqlateQueue<QueueItem, ResultCreated>,
    demandRunner: DemandRunner,
): Express {

    const app = express();
    app.use(bodyParser.json());
    const corsOpts: CorsOptions = {};
    if (process.env.hasOwnProperty("CORS_WEB_FRONTEND_ORIGIN")) {
        corsOpts.origin = process.env.CORS_WEB_FRONTEND_ORIGIN;
    } else {
        logger(
            Level.WARN,
            "STARTUP",
            "no CORS_WEB_FRONTEND_ORIGIN environmental variable will allow cors from ANYWHERE",
        );
    }
    app.use(cors(corsOpts));

    const nextWrapDependencies: NextWrapDependencies = {
        logger,
        setTimeout,
        clearTimeout,
    };

    const nwCaptureRequestStart = nextWrap(nextWrapDependencies, 1000, captureRequestStart);
    const nwCaptureRequestEnd = nextWrap(nextWrapDependencies, 1000, getCaptureRequestEnd(logger));
    const nwLoadDefinition = nextWrap(nextWrapDependencies, 1000, loadDefinition);

    app.get(
        "/definition",
        nwCaptureRequestStart,
        nextWrap(nextWrapDependencies, 1000, (_req, res, next) => {
            patchDefinitionMap(Level.ERROR).then(() => {
                res.json(
                    Array.from(definitionMap.entries())
                        .map((([ name, title ]) => ({ name, title })))
                );
                next();
            });
        }),
        nwCaptureRequestEnd,
        getCaptureRequestErrorHandler(logger),
    );

    app.get(
        "/definition/:definitionName",
        nwCaptureRequestStart,
        nwLoadDefinition,
        nextWrap(nextWrapDependencies, 1000, getDefinition),
        nwCaptureRequestEnd,
        getCaptureRequestErrorHandler(logger),
    );

    app.post(
        "/demand/:definitionName",
        nwCaptureRequestStart,
        nwLoadDefinition,
        nextWrap(
            nextWrapDependencies,
            1000,
            runDemand({ serviceInformation, serverVariableRequester, demandRunner }),
        ),
        nwCaptureRequestEnd,
        getCaptureRequestErrorHandler(logger),
    );


    app.post(
        "/request/:definitionName",
        nwCaptureRequestStart,
        nwLoadDefinition,
        nextWrap(
            nextWrapDependencies,
            1000,
            createRequest({ persistence, serviceInformation, queue, serverVariableRequester }),
        ),
        nwCaptureRequestEnd,
        getCaptureRequestErrorHandler(logger),
    );


    app.get(
        "/pending-request",
        nwCaptureRequestStart,
        nextWrap(nextWrapDependencies, 1000, outstandingRequestId({ persistence })),
        nwCaptureRequestEnd,
        getCaptureRequestErrorHandler(logger),
    );


    app.get(
        "/request/:definitionName/:requestId",
        nwCaptureRequestStart,
        nwLoadDefinition,
        nextWrap(nextWrapDependencies, 1000, getRequest({ persistence, serviceInformation })),
        nwCaptureRequestEnd,
        getCaptureRequestErrorHandler(logger),
    );


    app.get(
        "/result/:definitionName/:resultId.csv",
        nwCaptureRequestStart,
        nwLoadDefinition,
        nextWrap(nextWrapDependencies, 1000, getResultCsv({ persistence })),
        nwCaptureRequestEnd,
        getCaptureRequestErrorHandler(logger),
    );


    app.get(
        "/result/:definitionName/:resultId",
        (req: Request, _res: Response, next: NextFunction) => {
            if (req.params && req.params.resultId.match(/\.csv$/)) { return; }
            next();
        },
        nwCaptureRequestStart,
        nwLoadDefinition,
        nextWrap(nextWrapDependencies, 1000, getResult({ persistence, serviceInformation })),
        nwCaptureRequestEnd,
        getCaptureRequestErrorHandler(logger),
    );


    return app;

}

const databaseType = (process.env.hasOwnProperty("DATABASE_TYPE") &&
    (("" + process.env.DATABASE_TYPE).toLowerCase() === "mysql")) ?
        DatabaseType.MySQL :
        DatabaseType.PostgreSQL;

getQueryRunner(databaseType, logger).then(({queue, demand}) => {
        const persistence = new FilesystemPersistence("persistence");
        const serviceInformation: ServiceInformation = {
            getApiRoot: (_req: Request) => {
                return "/";
            },
        };

        const app = setupApp(
            persistence,
            serviceInformation,
            queue,
            demand,
        );

        app.listen(process.env.LISTEN_PORT, () => {
            logger(Level.INFO, "STARTUP", "eslate-server listening on " + process.env.LISTEN_PORT);
        });

        writeResults(persistence, queue);

});
