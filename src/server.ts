import Ajv from "ajv";
import assert = require("assert");
import bodyParser from "body-parser";
import cors, {CorsOptions} from "cors";
import express, { Express, NextFunction, Request, Response, static as expressStatic } from "express";
import fs from "fs";
import path from "path";
import { Pool } from "pg";

import {EsqlateDefinition} from "esqlate-lib";
import * as schemaDefinition from "esqlate-lib/res/schema-definition.json";
import getEsqlateQueue from "esqlate-queue";
import { EsqlateQueue, EsqlateQueueWorker } from "esqlate-queue";
import logger, { Level } from "./logger";
import { captureRequestStart, createRequest, getCaptureRequestEnd, getCaptureRequestErrorHandler, getDefinition, getRequest, getResult, loadDefinition, outstandingRequestId, runDemand, ServerVariableRequester, ServiceInformation } from "./middleware";
import nextWrap, { NextWrapDependencies } from "./nextWrap";
import { FilesystemPersistence, Persistence } from "./persistence";
import { DemandRunner, getDemandRunner, getEsqlateQueueWorker, getLookupOid, QueueItem, ResultCreated } from "./QueryRunner";
import JSON5 from 'json5';

if (!process.env.hasOwnProperty("ADVERTISED_API_ROOT")) {
    logger(Level.FATAL, "STARTUP", "no ADVERTISED_API_ROOT environmental variable defined");
}

if (!process.env.hasOwnProperty("LISTEN_PORT")) {
    logger(Level.FATAL, "STARTUP", "no LISTEN_PORT environmental variable defined");
}

if (!process.env.hasOwnProperty("DEFINITION_DIRECTORY")) {
    logger(Level.FATAL, "STARTUP", "no DEFINITION_DIRECTORY environmental variable defined");
}

const DEFINITION_DIRECTORY: string = process.env.DEFINITION_DIRECTORY as string;

const ajv = new Ajv();
const ajvValidateDefinition = ajv.compile(schemaDefinition);

let definitionList: { name: string; title: string }[] = [];

function hasNoStaticParams(def: EsqlateDefinition): boolean {
    return !def.parameters.some((param) => param.type == "static");
}

fs.readdir(DEFINITION_DIRECTORY, (readDirErr, filenames) => {
    if (readDirErr) { logger(Level.FATAL, "STARTUP", "Could not list definition"); }
    filenames.forEach((filename) => {
        const fp = path.join(DEFINITION_DIRECTORY, filename);
        fs.readFile(fp, { encoding: "utf8" }, (readFileErr, data) => {

            if (readFileErr) {
                logger(Level.FATAL, "STARTUP", `Could not read definition ${fp}`);
            }

            let json: EsqlateDefinition;

            try {
                json = JSON5.parse(data);
            } catch (e) {
                logger(Level.FATAL, "STARTUP", `Could not unserialize definition ${fp}`);
                return;
            }

            const valid = ajvValidateDefinition(json);
            if (!valid) {
                const errors = ajvValidateDefinition.errors;
                logger(Level.FATAL, "STARTUP", `Definition ${fp} has errors ${JSON.stringify(errors)}`);
            }

            if (json.name != filename.replace(/\.json5?$/, '')) {
                logger(Level.FATAL, "STARTUP", `Definition ${fp} has different name to filename`);
            }

            if ((filename.substring(0, 1) != "_") && hasNoStaticParams(json)) {
                definitionList.push({ title: json.title, name: json.name });
            }

        });
    });
});


async function writeResults(persistence: Persistence, queue: EsqlateQueue<QueueItem, ResultCreated>) {
    for await (const rc of queue.results()) {
        await persistence.createResult(
            rc.definitionName,
            rc.resultId,
            rc.result,
        );
    }
}

const serverVariableRequester: ServerVariableRequester = {
    listServerVariable: (_req: Request) => {
        return ["user_id"];
    },
    getServerVariable: (_req: Request, _name: string) => {
        return 999;
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
            res.json(definitionList);
            next();
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
        "/result/:definitionName/:resultId",
        nwCaptureRequestStart,
        nwLoadDefinition,
        nextWrap(nextWrapDependencies, 1000, getResult({ persistence })),
        nwCaptureRequestEnd,
        getCaptureRequestErrorHandler(logger),
    );


    return app;

}

const pool = new Pool();
getLookupOid(pool)
    .then((lookupOid) => {

        const persistence = new FilesystemPersistence("persistence");
        const serviceInformation: ServiceInformation = {
            getApiRoot: () => "" + process.env.ADVERTISED_API_ROOT,
        };
        const queue = getEsqlateQueue(getEsqlateQueueWorker(pool, lookupOid));

        const app = setupApp(
            persistence,
            serviceInformation,
            queue,
            getDemandRunner(pool, lookupOid),
        );

        app.listen(process.env.LISTEN_PORT, () => {
            logger(Level.INFO, "STARTUP", "eslate-server listening on " + process.env.LISTEN_PORT);
        });

        writeResults(persistence, queue);

    });
