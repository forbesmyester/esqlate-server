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
const assert = require("assert");
const fs_1 = __importDefault(require("fs"));
const json5_1 = __importDefault(require("json5"));
const path_1 = require("path");
const ajv_1 = __importDefault(require("ajv"));
const random_crypto_string_1 = __importDefault(require("random-crypto-string"));
const logger_1 = require("./logger");
const persistence_1 = require("./persistence");
const schemaRequestCreation = __importStar(require("esqlate-lib/res/schema-request-creation.json"));
// TODO: Move types to seperate files
const DEFINITION_DIRECTORY = process.env.DEFINITION_DIRECTORY;
function setRequestLocal(req, k, v) {
    const other = getRequestLocal(req);
    other[k] = v;
    Object.assign(req, { local: other });
}
function getRequestLocalKey(k, r) {
    const l = getRequestLocal(r);
    if (!l.hasOwnProperty(k)) {
        throw new logger_1.EsqlateErrorMissingLocal(`${k}`);
    }
    return l[k];
}
function getRequestLocal(r) {
    return r.local || {};
}
function captureRequestStart(req, _res, next) {
    setRequestLocal(req, "start", new Date().getTime());
    next();
}
exports.captureRequestStart = captureRequestStart;
function loadDefinition(req, _res, next) {
    assert(req.params.hasOwnProperty("definitionName"), "missing request param definitionName");
    const definitionName = persistence_1.safeDefinitionName(req.params.definitionName);
    let errCount = 0;
    function process(err, data) {
        if (errCount === -1) {
            return;
        }
        if ((err) && (errCount++ > 0)) {
            return next(new logger_1.EsqlateErrorMissingDefinition(`${definitionName}`));
        }
        if (err) {
            return errCount = errCount + 1;
        }
        let j;
        try {
            j = json5_1.default.parse(data);
        }
        catch (e) {
            return next(new logger_1.EsqlateErrorInvalidDefinition(`${definitionName}`));
        }
        setRequestLocal(req, "definition", j);
        next();
    }
    fs_1.default.readFile(path_1.join(DEFINITION_DIRECTORY, definitionName + ".json5"), { encoding: "utf8" }, process);
    fs_1.default.readFile(path_1.join(DEFINITION_DIRECTORY, definitionName + ".json"), { encoding: "utf8" }, process);
}
exports.loadDefinition = loadDefinition;
function logError(logger, err) {
    if (err instanceof logger_1.EsqlateError) {
        logger(logger_1.Level.INFO, err.code, err.message, err);
    }
}
function getCaptureRequestErrorHandler(logger) {
    return function captureRequestErrorHandler(err, _req, res, next) {
        if (err && (err instanceof logger_1.EsqlateError)) {
            switch (err.code) {
                case logger_1.EsqlateErrorEnum.InvalidRequestParameter:
                case logger_1.EsqlateErrorEnum.MissingVariable:
                case logger_1.EsqlateErrorEnum.InvalidRequestBody:
                    res.status(422).json({ error: err.message }).end();
                    return;
                case logger_1.EsqlateErrorEnum.NotFoundPersistenceError:
                case logger_1.EsqlateErrorEnum.MissingDefinition:
                    res.status(404).json({ error: err.message }).end();
                    return;
                case logger_1.EsqlateErrorEnum.MissingRequestParam:
                case logger_1.EsqlateErrorEnum.MissingLocal:
                case logger_1.EsqlateErrorEnum.InvalidDefinition:
                default:
                    res.status(500).json({ error: err.message }).end();
                    return;
            }
        }
        if (err) {
            logError(logger, err);
            return;
        }
    };
}
exports.getCaptureRequestErrorHandler = getCaptureRequestErrorHandler;
function getCaptureRequestEnd(logger) {
    return function captureRequestEnd(req, _res, next) {
        const local = getRequestLocal(req);
        const end = new Date().getTime();
        if (end - local.start > 1000) {
            logger(logger_1.Level.WARN, "REQTIME", `Request to ${req.method}:${req.path} took ${end - local.start}ms`);
            return next();
        }
        logger(logger_1.Level.INFO, "REQTIME", `Request to ${req.method}:${req.path} took ${end - local.start}ms`);
        next();
    };
}
exports.getCaptureRequestEnd = getCaptureRequestEnd;
function getDefinition(req, res, next) {
    res.json(getRequestLocalKey("definition", req));
    next();
}
exports.getDefinition = getDefinition;
function outstandingRequestId({ persistence }) {
    async function outstandingRequestIdSender(_req, res) {
        res.status(200);
        for await (const id of persistence.outstandingRequestId()) {
            res.write(JSON.stringify(id) + "\n");
        }
        res.end();
    }
    return function outstandingRequestIdImpl(req, res, next) {
        outstandingRequestIdSender(req, res)
            .then(() => next())
            .catch((err) => next(err));
    };
}
exports.outstandingRequestId = outstandingRequestId;
function getRequest({ persistence, serviceInformation: { getApiRoot } }) {
    return function getRequestImpl(req, res, next) {
        assert(req.params.hasOwnProperty("definitionName"), "Missing request param definitionName");
        assert(req.params.hasOwnProperty("requestId"), "Missing request param requestId");
        const definitionName = persistence_1.safeDefinitionName(req.params.definitionName);
        const requestId = persistence_1.safeId(req.params.requestId);
        persistence.getResultIdForRequest(definitionName, requestId)
            .then((resultId) => {
            if (resultId) {
                const location = path_1.join(getApiRoot(req), "result", definitionName, resultId);
                const redirect = parseInt("" + req.headers["x-no-redirect"], 10) ? false : true;
                res.setHeader("Location", location);
                res.status(redirect ? 301 : 200).json({ status: "complete", location });
                return next();
            }
            res.status(202).json({ status: "pending" });
            return next();
        })
            .catch((err) => {
            return next(err);
        });
    };
}
exports.getRequest = getRequest;
function runDemand({ serverVariableRequester, serviceInformation: { getApiRoot }, demandRunner }) {
    return async function runDemandImpl(req, res, next) {
        let args;
        try {
            args = getVariables({ getApiRoot }, serverVariableRequester, req);
        }
        catch (e) {
            return next(e);
        }
        const definition = getRequestLocalKey("definition", req);
        // TODO: Pass in server args propertly
        demandRunner(definition, [], args)
            .then((result) => {
            res.json(result);
            next();
        })
            .catch((err) => { next(err); });
    };
}
exports.runDemand = runDemand;
function getVariables({ getApiRoot }, serverVariableRequester, req) {
    function get() {
        const serverVariables = serverVariableRequester.listServerVariable(req).map((name) => ({ name, value: serverVariableRequester.getServerVariable(req, name) }));
        return serverVariables.reduce((acc, sv) => {
            const r = acc.filter((a) => !(("" + a.name) === ("" + sv.name)));
            r.push(sv);
            return r;
        }, req.body.arguments.concat([]));
    }
    function getMissingVariables(args) {
        const available = new Set(args.map((rb) => rb.name));
        return definition.parameters
            .filter((reqd) => {
            return !available.has(reqd.name);
        })
            .map(({ name }) => name);
    }
    assert(req.params.hasOwnProperty("definitionName"), "Missing request param definitionName");
    const definition = getRequestLocalKey("definition", req);
    const definitionName = persistence_1.safeDefinitionName(req.params.definitionName);
    const valid = ajvValidateRequestCreation(req.body);
    if (!valid) {
        const errors = ajvValidateRequestCreation.errors;
        const msg = path_1.join(getApiRoot(req), "request", definitionName) + ": " + JSON.stringify(errors);
        throw new logger_1.EsqlateErrorInvalidRequestBody(msg);
    }
    const variables = get();
    const missingVariables = getMissingVariables(variables);
    if (missingVariables.length) {
        const errorMsg = "Missing Variables: " + JSON.stringify(missingVariables);
        const msg = path_1.join(getApiRoot(req), "request", definitionName) + ": " + JSON.stringify(errorMsg);
        throw new logger_1.EsqlateErrorMissingVariables(msg);
    }
    return variables;
}
const ajv = new ajv_1.default();
const ajvValidateRequestCreation = ajv.compile(schemaRequestCreation);
function createRequest({ serverVariableRequester, persistence, queue, serviceInformation: { getApiRoot } }) {
    return function createRequestImpl(req, res, next) {
        let variables;
        try {
            variables = getVariables({ getApiRoot }, serverVariableRequester, req);
        }
        catch (e) {
            return next(e);
        }
        const definition = getRequestLocalKey("definition", req);
        const definitionName = persistence_1.safeDefinitionName(req.params.definitionName);
        random_crypto_string_1.default(8)
            .then((requestId) => {
            return persistence.createRequest(definitionName, requestId, variables).then(() => requestId);
        })
            .then((requestId) => {
            // TODO: Pass in server variables properly
            const queueItem = {
                definition,
                requestId,
                serverParameters: [],
                userParameters: variables,
            };
            queue.push(queueItem);
            return queueItem;
        })
            .then((qi) => {
            const loc = path_1.join(getApiRoot(req), "request", definitionName, qi.requestId);
            res.setHeader("Location", loc);
            res.status(202).json({ location: loc });
            next();
        })
            .catch((err) => { next(err); });
    };
}
exports.createRequest = createRequest;
function getResultCsv({ persistence }) {
    return function getResultImpl(req, res, next) {
        assert(req.params.hasOwnProperty("definitionName"), "Missing request param definitionName");
        assert(req.params.hasOwnProperty("resultId"), "Missing request param resultId");
        const definitionName = persistence_1.safeDefinitionName(req.params.definitionName);
        const resultId = persistence_1.safeId(req.params.resultId.replace(/\.csv$/, ""));
        persistence.getResultCsvStream(definitionName, resultId)
            .then((readStream) => {
            res.setHeader("content-type", "text/csv");
            res.setHeader("Content-disposition", `attachment; filename=${definitionName}-${resultId}.csv`);
            const stream = readStream.pipe(res);
            stream.on("finish", () => {
                res.end();
                return next();
            });
            stream.on("error", (err) => {
                res.send("WARNING: An error occurred during preperation of the CSV");
                return next(err);
            });
        })
            .catch((err) => { next(err); });
    };
}
exports.getResultCsv = getResultCsv;
function getResult({ persistence, serviceInformation: { getApiRoot } }) {
    return function getResultImpl(req, res, next) {
        assert(req.params.hasOwnProperty("definitionName"), "Missing request param definitionName");
        assert(req.params.hasOwnProperty("resultId"), "Missing request param resultId");
        const definitionName = persistence_1.safeDefinitionName(req.params.definitionName);
        const resultId = persistence_1.safeId(req.params.resultId);
        function mapper(otherFormat) {
            return {
                ...otherFormat,
                location: path_1.join(getApiRoot(req), "result", definitionName, resultId) + ".csv",
            };
        }
        persistence.getResult(definitionName, resultId)
            .then((result) => {
            if (result.status === "error") {
                res.json(result);
                return next();
            }
            const mapped = {
                ...result,
                status: result.full_format_urls ? "complete" : "preview",
                full_format_urls: (result.full_format_urls || []).map(mapper),
            };
            res.json(mapped);
            return next();
        })
            .catch((err) => { next(err); });
    };
}
exports.getResult = getResult;
