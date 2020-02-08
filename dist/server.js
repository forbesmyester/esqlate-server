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
const ajv_1 = __importDefault(require("ajv"));
const body_parser_1 = __importDefault(require("body-parser"));
const cors_1 = __importDefault(require("cors"));
const express_1 = __importDefault(require("express"));
const fs_1 = __importDefault(require("fs"));
const path_1 = __importDefault(require("path"));
const pg_1 = require("pg");
const schemaDefinition = __importStar(require("esqlate-lib/res/schema-definition.json"));
const esqlate_queue_1 = __importDefault(require("esqlate-queue"));
const json5_1 = __importDefault(require("json5"));
const logger_1 = __importStar(require("./logger"));
const middleware_1 = require("./middleware");
const nextWrap_1 = __importDefault(require("./nextWrap"));
const persistence_1 = require("./persistence");
const QueryRunner_1 = require("./QueryRunner");
if (!process.env.hasOwnProperty("LISTEN_PORT")) {
    logger_1.default(logger_1.Level.FATAL, "STARTUP", "no LISTEN_PORT environmental variable defined");
}
const DEFINITION_DIRECTORY = process.env.DEFINITION_DIRECTORY || (__dirname + '/example_definition');
const ajv = new ajv_1.default();
const ajvValidateDefinition = ajv.compile(schemaDefinition);
const definitionMap = new Map();
function hasNoStaticParams(def) {
    return !def.parameters.some((param) => param.type === "static");
}
function certifyDefinition(loggerLevel, fullPath, data) {
    let json;
    try {
        json = json5_1.default.parse(data);
    }
    catch (e) {
        logger_1.default(loggerLevel, "DEFINITION", `Could not unserialize definition ${fullPath}`);
        throw new Error(`Could not unserialize definition ${fullPath}`);
    }
    const valid = ajvValidateDefinition(json);
    if (!valid) {
        const errors = ajvValidateDefinition.errors;
        logger_1.default(loggerLevel, "DEFINITION", `Definition ${fullPath} has errors ${JSON.stringify(errors)}`);
        throw new Error(`Definition ${fullPath} has errors ${JSON.stringify(errors)}`);
    }
    if (json.name !== path_1.default.basename(fullPath).replace(/\.json5?$/, "")) {
        logger_1.default(loggerLevel, "DEFINITION", `Definition ${fullPath} has different name to filename`);
        throw new Error(`Definition ${fullPath} has different name to filename`);
    }
    if ((path_1.default.basename(fullPath).substring(0, 1) !== "_") &&
        hasNoStaticParams(json)) {
        return { title: json.title, name: json.name };
    }
    return false;
}
function sequentialPromises(ar, f) {
    const ps = ar.map(f);
    return Promise.all(ps)
        .then((items) => {
        return items.filter((i) => i !== false);
    });
}
function readDefinitionList(level, knownDefinitions) {
    function readFile(fullPath) {
        return new Promise((resolve, reject) => {
            fs_1.default.readFile(fullPath, { encoding: "utf8" }, (readFileErr, data) => {
                if (readFileErr) {
                    logger_1.default(level, "DEFINITION", `Could not read definition ${fullPath}`);
                    return reject(`Could not read filename ${fullPath}`);
                }
                resolve({ data, fullPath });
            });
        });
    }
    function certify({ data, fullPath }) {
        return Promise.resolve(certifyDefinition(level, fullPath, data));
    }
    function myReadDir() {
        return new Promise((resolve, reject) => {
            fs_1.default.readdir(DEFINITION_DIRECTORY, (readDirErr, filenames) => {
                if (readDirErr) {
                    logger_1.default(logger_1.Level.FATAL, "DEFINITION", "Could not list definition");
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
            return path_1.default.join(DEFINITION_DIRECTORY, filename);
        })
            .filter((fp) => path_1.default.basename(fp).substring(0, 1) !== "_")
            .filter((fp) => knownDefinitions.indexOf(fp) === -1);
        return sequentialPromises(fullPaths, readFile)
            .then((datas) => {
            return sequentialPromises(datas, certify);
        });
    });
}
function patchDefinitionMap(level) {
    return readDefinitionList(level, Array.from(definitionMap.keys()))
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
    const nwLoadDefinition = nextWrap_1.default(nextWrapDependencies, 1000, middleware_1.loadDefinition);
    app.get("/definition", nwCaptureRequestStart, nextWrap_1.default(nextWrapDependencies, 1000, (_req, res, next) => {
        patchDefinitionMap(logger_1.Level.ERROR).then(() => {
            res.json(Array.from(definitionMap.entries())
                .map((([name, title]) => ({ name, title }))));
            next();
        });
    }), nwCaptureRequestEnd, middleware_1.getCaptureRequestErrorHandler(logger_1.default));
    app.get("/definition/:definitionName", nwCaptureRequestStart, nwLoadDefinition, nextWrap_1.default(nextWrapDependencies, 1000, middleware_1.getDefinition), nwCaptureRequestEnd, middleware_1.getCaptureRequestErrorHandler(logger_1.default));
    app.post("/demand/:definitionName", nwCaptureRequestStart, nwLoadDefinition, nextWrap_1.default(nextWrapDependencies, 1000, middleware_1.runDemand({ serviceInformation, serverVariableRequester, demandRunner })), nwCaptureRequestEnd, middleware_1.getCaptureRequestErrorHandler(logger_1.default));
    app.post("/request/:definitionName", nwCaptureRequestStart, nwLoadDefinition, nextWrap_1.default(nextWrapDependencies, 1000, middleware_1.createRequest({ persistence, serviceInformation, queue, serverVariableRequester })), nwCaptureRequestEnd, middleware_1.getCaptureRequestErrorHandler(logger_1.default));
    app.get("/pending-request", nwCaptureRequestStart, nextWrap_1.default(nextWrapDependencies, 1000, middleware_1.outstandingRequestId({ persistence })), nwCaptureRequestEnd, middleware_1.getCaptureRequestErrorHandler(logger_1.default));
    app.get("/request/:definitionName/:requestId", nwCaptureRequestStart, nwLoadDefinition, nextWrap_1.default(nextWrapDependencies, 1000, middleware_1.getRequest({ persistence, serviceInformation })), nwCaptureRequestEnd, middleware_1.getCaptureRequestErrorHandler(logger_1.default));
    app.get("/result/:definitionName/:resultId.csv", nwCaptureRequestStart, nwLoadDefinition, nextWrap_1.default(nextWrapDependencies, 1000, middleware_1.getResultCsv({ persistence })), nwCaptureRequestEnd, middleware_1.getCaptureRequestErrorHandler(logger_1.default));
    app.get("/result/:definitionName/:resultId", (req, _res, next) => {
        if (req.params && req.params.resultId.match(/\.csv$/)) {
            return;
        }
        next();
    }, nwCaptureRequestStart, nwLoadDefinition, nextWrap_1.default(nextWrapDependencies, 1000, middleware_1.getResult({ persistence, serviceInformation })), nwCaptureRequestEnd, middleware_1.getCaptureRequestErrorHandler(logger_1.default));
    return app;
}
const pool = new pg_1.Pool();
let connectionCount = 0;
pool.on("connect", () => {
    logger_1.default(logger_1.Level.INFO, "DATABASE", `Database Connection Count Incremented: ${++connectionCount} Connections`);
});
pool.on("connect", () => {
    logger_1.default(logger_1.Level.INFO, "DATABASE", `Database Connection Count Decremented: ${--connectionCount} Connections`);
});
pool.on("error", (e) => {
    logger_1.default(logger_1.Level.FATAL, "DATABASE", `Database Error: ${e.message}`);
});
QueryRunner_1.getLookupOid(pool)
    .then((lookupOid) => {
    const persistence = new persistence_1.FilesystemPersistence("persistence");
    const serviceInformation = {
        getApiRoot: (_req) => {
            return "/";
        },
    };
    const queue = esqlate_queue_1.default(QueryRunner_1.getEsqlateQueueWorker(pool, lookupOid));
    const app = setupApp(persistence, serviceInformation, queue, QueryRunner_1.getDemandRunner(pool, lookupOid));
    app.listen(process.env.LISTEN_PORT, () => {
        logger_1.default(logger_1.Level.INFO, "STARTUP", "eslate-server listening on " + process.env.LISTEN_PORT);
    });
    writeResults(persistence, queue);
});
