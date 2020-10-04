import bodyParser from "body-parser";
import cors, { CorsOptions } from "cors";
import express, { Express, NextFunction, Request, Response } from "express";

import { EsqlateQueue } from "esqlate-queue";
import { DefinitionList, readDefinitionList } from "./functions";
import logger, { Level } from "./logger";
import { captureRequestStart, createRequest, getCaptureRequestEnd, getCaptureRequestErrorHandler, getDefinition, getRequest, getResult, getResultCsv, loadDefinition, outstandingRequestId, runDemand, ServerVariableRequester, ServiceInformation } from "./middleware";
import nextWrap, { NextWrapDependencies } from "./nextWrap";
import { FilesystemPersistence, Persistence } from "./persistence";
import { DatabaseType, DemandRunner, getQueryRunner, QueueItem, ResultCreated } from "./QueryRunner";

if (!process.env.hasOwnProperty("LISTEN_PORT")) {
    logger(Level.FATAL, "STARTUP", "no LISTEN_PORT environmental variable defined");
}

const DEFINITION_DIRECTORY: string = process.env.DEFINITION_DIRECTORY || (__dirname + "/example_definition");


type DefinitionMap = Map<string, string>;
const definitionMap: DefinitionMap = new Map();

function patchDefinitionMap(level: Level) {

    const conf = {
        loggerLevel: level,
        knownDefinitions: Array.from(definitionMap.keys()),
        definitionDirectory: DEFINITION_DIRECTORY,
    };
    const deps = { logger };

    return readDefinitionList(deps, conf)
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

const databaseParallelism = parseInt(process.env.DATABASE_PARALLELISM || "5", 10) || 5;

getQueryRunner(databaseType, databaseParallelism, logger)
    .then(({queue, demand}) => {
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
