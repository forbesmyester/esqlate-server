"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
    result["default"] = mod;
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
const body_parser_1 = __importDefault(require("body-parser"));
const cors_1 = __importDefault(require("cors"));
const express_1 = __importDefault(require("express"));
const functions_1 = require("./functions");
const logger_1 = __importStar(require("./logger"));
const middleware_1 = require("./middleware");
const nextWrap_1 = __importDefault(require("./nextWrap"));
const persistence_1 = require("./persistence");
const QueryRunner_1 = require("./QueryRunner");
if (!process.env.hasOwnProperty("LISTEN_PORT")) {
    logger_1.default(logger_1.Level.FATAL, "STARTUP", "no LISTEN_PORT environmental variable defined");
}
// TODO: THIS SHOULD NOT BE HERE!
const DEFINITION_DIRECTORY = process.env.DEFINITION_DIRECTORY || (process.cwd() + "/example_definition");
const loadDefinition = functions_1.getLoadDefinition({ definitionDirectory: DEFINITION_DIRECTORY });
const definitionMap = new Map();
function patchDefinitionMap(level) {
    const conf = {
        loggerLevel: level,
        knownDefinitions: Array.from(definitionMap.keys()),
        definitionDirectory: DEFINITION_DIRECTORY,
    };
    const deps = { logger: logger_1.default };
    return functions_1.readDefinitionList(deps, conf)
        .then((dl) => {
        dl.forEach(({ name, title }) => {
            definitionMap.set(name, title);
        });
    })
        .catch((_e) => {
        // Logs already output.
    });
}
patchDefinitionMap(logger_1.Level.FATAL);
async function writeResults(persistence, queue) {
    try {
        for await (const rc of queue.results()) {
            await persistence.createResult(rc.definitionName, rc.resultId, rc.result);
        }
    }
    catch (e) {
        logger_1.default(logger_1.Level.WARN, "QUEUE", e.message);
    }
}
const serverVariableRequester = {
    listServerVariable: (_req) => {
        return [];
    },
    getServerVariable: (_req, _name) => {
        return "";
    },
};
function setupApp(persistence, serviceInformation, queue, demandRunner) {
    const app = express_1.default();
    app.use(body_parser_1.default.json());
    const corsOpts = {};
    if (process.env.hasOwnProperty("CORS_WEB_FRONTEND_ORIGIN")) {
        corsOpts.origin = process.env.CORS_WEB_FRONTEND_ORIGIN;
    }
    else {
        logger_1.default(logger_1.Level.WARN, "STARTUP", "no CORS_WEB_FRONTEND_ORIGIN environmental variable will allow cors from ANYWHERE");
    }
    app.use(cors_1.default(corsOpts));
    const nextWrapDependencies = {
        logger: logger_1.default,
        setTimeout,
        clearTimeout,
    };
    const nwCaptureRequestStart = nextWrap_1.default(nextWrapDependencies, 1000, middleware_1.captureRequestStart);
    const nwCaptureRequestEnd = nextWrap_1.default(nextWrapDependencies, 1000, middleware_1.getCaptureRequestEnd(logger_1.default));
    app.get("/definition", nwCaptureRequestStart, nextWrap_1.default(nextWrapDependencies, 1000, (_req, res, next) => {
        patchDefinitionMap(logger_1.Level.ERROR).then(() => {
            res.json(Array.from(definitionMap.entries())
                .map((([name, title]) => ({ name, title }))));
            next();
        });
    }), nwCaptureRequestEnd, middleware_1.getCaptureRequestErrorHandler(logger_1.default));
    app.get("/definition/:definitionName", nwCaptureRequestStart, nextWrap_1.default(nextWrapDependencies, 1000, middleware_1.getGetDefinition({ loadDefinition })), nwCaptureRequestEnd, middleware_1.getCaptureRequestErrorHandler(logger_1.default));
    app.post("/demand/:definitionName", nwCaptureRequestStart, nextWrap_1.default(nextWrapDependencies, 1000, middleware_1.runDemand({ loadDefinition, serviceInformation, serverVariableRequester, demandRunner })), nwCaptureRequestEnd, middleware_1.getCaptureRequestErrorHandler(logger_1.default));
    app.post("/request/:definitionName", nwCaptureRequestStart, nextWrap_1.default(nextWrapDependencies, 1000, middleware_1.createRequest({ loadDefinition, persistence, serviceInformation, queue, serverVariableRequester })), nwCaptureRequestEnd, middleware_1.getCaptureRequestErrorHandler(logger_1.default));
    app.get("/pending-request", nwCaptureRequestStart, nextWrap_1.default(nextWrapDependencies, 1000, middleware_1.outstandingRequestId({ persistence })), nwCaptureRequestEnd, middleware_1.getCaptureRequestErrorHandler(logger_1.default));
    app.get("/request/:definitionName/:requestId", nwCaptureRequestStart, nextWrap_1.default(nextWrapDependencies, 1000, middleware_1.getRequest({ persistence, serviceInformation })), nwCaptureRequestEnd, middleware_1.getCaptureRequestErrorHandler(logger_1.default));
    app.get("/result/:definitionName/:resultId.csv", nwCaptureRequestStart, nextWrap_1.default(nextWrapDependencies, 1000, middleware_1.getResultCsv({ persistence })), nwCaptureRequestEnd, middleware_1.getCaptureRequestErrorHandler(logger_1.default));
    app.get("/result/:definitionName/:resultId", (req, _res, next) => {
        if (req.params && req.params.resultId.match(/\.csv$/)) {
            return;
        }
        next();
    }, nwCaptureRequestStart, nextWrap_1.default(nextWrapDependencies, 1000, middleware_1.getResult({ persistence, serviceInformation })), nwCaptureRequestEnd, middleware_1.getCaptureRequestErrorHandler(logger_1.default));
    return app;
}
const databaseType = (process.env.hasOwnProperty("DATABASE_TYPE") &&
    (("" + process.env.DATABASE_TYPE).toLowerCase() === "mysql")) ?
    QueryRunner_1.DatabaseType.MySQL :
    QueryRunner_1.DatabaseType.PostgreSQL;
const databaseParallelism = parseInt(process.env.DATABASE_PARALLELISM || "5", 10) || 5;
QueryRunner_1.getQueryRunner(databaseType, databaseParallelism, logger_1.default)
    .then(({ queue, demand }) => {
    const persistence = new persistence_1.FilesystemPersistence("persistence");
    const serviceInformation = {
        getApiRoot: () => {
            return "/";
        },
    };
    const app = setupApp(persistence, serviceInformation, queue, demand);
    app.listen(process.env.LISTEN_PORT, () => {
        logger_1.default(logger_1.Level.INFO, "STARTUP", "eslate-server listening on " + process.env.LISTEN_PORT);
    });
    writeResults(persistence, queue);
});
