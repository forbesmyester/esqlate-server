"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const assert = require("assert");
const path_1 = require("path");
const functions_1 = require("./functions");
const logger_1 = require("./logger");
const persistence_1 = require("./persistence");
// TODO: Move types to seperate files
const DEFINITION_DIRECTORY = process.env.DEFINITION_DIRECTORY;
function setRequestLocal(req, k, v) {
    const other = getRequestLocal(req);
    other[k] = v;
    Object.assign(req, { local: other });
}
function getRequestLocal(r) {
    return r.local || {};
}
function captureRequestStart(req, _res, next) {
    setRequestLocal(req, "start", new Date().getTime());
    next();
}
exports.captureRequestStart = captureRequestStart;
function logError(logger, err) {
    if (err instanceof logger_1.EsqlateError) {
        return logger(logger_1.Level.INFO, err.code, err.message, err);
    }
    logger(logger_1.Level.ERROR, "UNKNOWN", err.message, err);
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
            res.status(500).json({ error: "UNEXPECTED ERROR: No Message" }).end();
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
function getGetDefinition({ loadDefinition }) {
    return function getDefinition(req, res, next) {
        loadDefinition(req.params.definitionName)
            .then((def) => {
            res.json(def);
            next();
        })
            .catch((err) => {
            next(err);
        });
    };
}
exports.getGetDefinition = getGetDefinition;
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
                const location = path_1.join(getApiRoot(), "result", definitionName, resultId);
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
function runDemand({ serverVariableRequester, serviceInformation: { getApiRoot }, demandRunner, loadDefinition }) {
    return async function runDemandImpl(req, res, next) {
        const definition = await loadDefinition(req.params.definitionName);
        let args;
        try {
            args = functions_1.getVariables({ getApiRoot }, serverVariableRequester, definition.parameters, req);
        }
        catch (e) {
            return next(e);
        }
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
function createRequest(createRequestDeps) {
    return function createRequestImpl(req, res, next) {
        functions_1.createRequest(createRequestDeps, req)
            .then((loc) => {
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
function esqlateResultEnsureFullFormatUrl({ getApiRoot }, definitionName, resultId, otherFormat) {
    return {
        ...otherFormat,
        location: path_1.join(getApiRoot(), "result", definitionName, resultId) + ".csv",
    };
}
exports.esqlateResultEnsureFullFormatUrl = esqlateResultEnsureFullFormatUrl;
function getResult({ persistence, serviceInformation }) {
    return function getResultImpl(req, res, next) {
        assert(req.params.hasOwnProperty("definitionName"), "Missing request param definitionName");
        assert(req.params.hasOwnProperty("resultId"), "Missing request param resultId");
        const definitionName = persistence_1.safeDefinitionName(req.params.definitionName);
        const resultId = persistence_1.safeId(req.params.resultId);
        const mapper = (result) => {
            return esqlateResultEnsureFullFormatUrl(serviceInformation, definitionName, resultId, result);
        };
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
