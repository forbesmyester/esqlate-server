import assert = require("assert");
import bodyParser from "body-parser";
import cors, {CorsOptions} from "cors";
import express, { Express, NextFunction, Request, Response, static as expressStatic } from "express";
import { Pool } from "pg";

import getEsqlateQueue from "esqlate-queue";
import { EsqlateQueue, EsqlateQueueWorker } from "esqlate-queue";
import logger, { Level } from "./logger";
import { captureRequestStart, createRequest, getCaptureRequestEnd, getCaptureRequestErrorHandler, getDefinition, getRequest, getResult, loadDefinition, outstandingRequestId, ServerVariableRequester, ServiceInformation } from "./middleware";
import nextWrap, { NextWrapDependencies } from "./nextWrap";
import { FilesystemPersistence, Persistence } from "./persistence";
import { getEsqlateQueueWorker, getLookupOid, QueueItem, ResultCreated } from "./QueryRunner";


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
        "/definition/:definitionName",
        nwCaptureRequestStart,
        nwLoadDefinition,
        nextWrap(nextWrapDependencies, 1000, getDefinition),
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

        if (!process.env.hasOwnProperty("ADVERTISED_API_ROOT")) {
            logger(Level.FATAL, "STARTUP", "no ADVERTISED_API_ROOT environmental variable defined");
        }

        if (!process.env.hasOwnProperty("LISTEN_PORT")) {
            logger(Level.FATAL, "STARTUP", "no LISTEN_PORT environmental variable defined");
        }

        const persistence = new FilesystemPersistence("persistence");
        const serviceInformation: ServiceInformation = {
            getApiRoot: () => "" + process.env.ADVERTISED_API_ROOT,
        };
        const queue = getEsqlateQueue(getEsqlateQueueWorker(pool, lookupOid));

        const app = setupApp(
            persistence,
            serviceInformation,
            queue,
        );

        app.listen(process.env.LISTEN_PORT, () => {
            logger(Level.INFO, "STARTUP", "eslate-server listening on " + process.env.LISTEN_PORT);
        });

        writeResults(persistence, queue);

    });
