import assert = require("assert");
import { NextFunction, Request, Response } from "express";
import fs, {ReadStream} from "fs";
import JSON5 from "json5";
import { join as pathJoin } from "path";
import { createRequest as createRequestFunc, CreateRequestDeps, getLoadDefinition, getVariables, Input, NeedsDemandRunner, NeedsLoadDefinition, NeedsPersistence, NeedsServerVariableRequester, NeedsServiceInformation, ServiceInformation } from "./functions";

import Ajv from "ajv";
import { EsqlateArgument, EsqlateCompleteResultOtherFormat, EsqlateDefinition, EsqlateRequestCreation, EsqlateResult } from "esqlate-lib";
import { EsqlateQueue } from "esqlate-queue";
import randCryptoString from "random-crypto-string";

import { EsqlateError, EsqlateErrorEnum, EsqlateErrorInvalidDefinition, EsqlateErrorInvalidRequestBody, EsqlateErrorMissingDefinition, EsqlateErrorMissingLocal, EsqlateErrorMissingVariables, Level, Logger } from "./logger";
import { Persistence, safeDefinitionName, safeId } from "./persistence";
import { DemandRunner, QueueItem, ResultCreated } from "./QueryRunner";

// TODO: Move types to seperate files

const DEFINITION_DIRECTORY: string = process.env.DEFINITION_DIRECTORY as string;

interface Local { [k: string]: any; }

function setRequestLocal(req: Request, k: string, v: any) {
    const other: Local = getRequestLocal(req);
    other[k] = v;
    Object.assign(req, { local: other });
}

function getRequestLocal(r: Request): Local {
    return (r as Request & { local: Local }).local || {};
}

export function captureRequestStart(req: Request, _res: Response, next: NextFunction) {
    setRequestLocal(req, "start", new Date().getTime());
    next();
}

function logError(logger: Logger, err: Error) {

    if (err instanceof EsqlateError) {
        return logger(Level.INFO, err.code, err.message, err);
    }
    logger(Level.ERROR, "UNKNOWN", err.message, err);

}

export function getCaptureRequestErrorHandler(logger: Logger) {
    return function captureRequestErrorHandler(err: null | undefined | Error, _req: Request, res: Response, next: NextFunction) {
        if (err && (err instanceof EsqlateError)) {
            switch (err.code) {
                case EsqlateErrorEnum.InvalidRequestParameter:
                case EsqlateErrorEnum.MissingVariable:
                case EsqlateErrorEnum.InvalidRequestBody:
                    res.status(422).json({ error: err.message }).end();
                    return;
                case EsqlateErrorEnum.NotFoundPersistenceError:
                case EsqlateErrorEnum.MissingDefinition:
                    res.status(404).json({ error: err.message }).end();
                    return;
                case EsqlateErrorEnum.MissingRequestParam:
                case EsqlateErrorEnum.MissingLocal:
                case EsqlateErrorEnum.InvalidDefinition:
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

export function getCaptureRequestEnd(logger: Logger) {
    return function captureRequestEnd(req: Request, _res: Response, next: NextFunction) {
        const local = getRequestLocal(req);
        const end = new Date().getTime();
        if (end - local.start > 1000) {
            logger(Level.WARN, "REQTIME", `Request to ${req.method}:${req.path} took ${end - local.start}ms`);
            return next();
        }
        logger(Level.INFO, "REQTIME", `Request to ${req.method}:${req.path} took ${end - local.start}ms`);
        next();
    };
}


export function getGetDefinition({ loadDefinition }: NeedsLoadDefinition) {
    return function getDefinition(req: Request, res: Response, next: NextFunction) {
        loadDefinition(req.params.definitionName)
            .then((def) => {
                res.json(def);
                next();
            })
            .catch((err) => {
                next(err);
            });
    }
}


export function outstandingRequestId({ persistence }: NeedsPersistence) {

    async function outstandingRequestIdSender(_req: Request, res: Response) {

        res.status(200);
        for await (const id of persistence.outstandingRequestId()) {
            res.write(JSON.stringify(id) + "\n");
        }
        res.end();
    }

    return function outstandingRequestIdImpl(
        req: Request,
        res: Response,
        next: NextFunction,
    ) {
        outstandingRequestIdSender(req, res)
            .then(() => next())
            .catch((err) => next(err));
    };

}


export function getRequest({ persistence, serviceInformation: { getApiRoot } }: NeedsPersistence & NeedsServiceInformation) {

    return function getRequestImpl(req: Request, res: Response, next: NextFunction) {

        assert(req.params.hasOwnProperty("definitionName"), "Missing request param definitionName");

        assert(req.params.hasOwnProperty("requestId"), "Missing request param requestId");

        const definitionName = safeDefinitionName(req.params.definitionName);
        const requestId = safeId(req.params.requestId);

        persistence.getResultIdForRequest(definitionName, requestId)
            .then((resultId) => {
                if (resultId) {
                    const location = pathJoin(
                        getApiRoot(),
                        "result",
                        definitionName,
                        resultId,
                    );
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

export function runDemand({ serverVariableRequester, serviceInformation: { getApiRoot }, demandRunner, loadDefinition }: NeedsServiceInformation & NeedsDemandRunner & NeedsServerVariableRequester & NeedsLoadDefinition) {

    return async function runDemandImpl(
        req: Request & Input<EsqlateRequestCreation>,
        res: Response, next: NextFunction) {

        const definition: EsqlateDefinition = await loadDefinition(req.params.definitionName);

        let args: EsqlateArgument[];
        try {
            args = getVariables(
                { getApiRoot },
                serverVariableRequester,
                definition.parameters,
                req,
            );
        } catch (e) { return next(e); }

        // TODO: Pass in server args propertly
        demandRunner(definition, [], args)
            .then((result: EsqlateResult) => {
                res.json(result);
                next();
            })
            .catch((err: Error) => { next(err); });

    };

}


export function createRequest(createRequestDeps: CreateRequestDeps) {

    return function createRequestImpl(req: Request & Input<EsqlateRequestCreation>, res: Response, next: NextFunction) {

        createRequestFunc(createRequestDeps, req)
            .then((loc) => {
                res.setHeader("Location", loc);
                res.status(202).json({ location: loc });
                next();
            })
            .catch((err: Error) => { next(err); });

    };
}


export function getResultCsv({ persistence }: NeedsPersistence) {
    return function getResultImpl(req: Request, res: Response, next: NextFunction) {

        assert(req.params.hasOwnProperty("definitionName"), "Missing request param definitionName");

        assert(req.params.hasOwnProperty("resultId"), "Missing request param resultId");

        const definitionName = safeDefinitionName(req.params.definitionName);
        const resultId = safeId(req.params.resultId.replace(/\.csv$/, ""));

        persistence.getResultCsvStream(definitionName, resultId)
            .then((readStream: ReadStream) => {
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

export function esqlateResultEnsureFullFormatUrl({ getApiRoot }: ServiceInformation, definitionName: string, resultId: string, otherFormat: EsqlateCompleteResultOtherFormat): EsqlateCompleteResultOtherFormat {
    return {
        ...otherFormat,
        location: pathJoin(getApiRoot(), "result", definitionName, resultId) + ".csv",
    };
}

export function getResult({ persistence, serviceInformation }: NeedsPersistence & NeedsServiceInformation) {
    return function getResultImpl(req: Request, res: Response, next: NextFunction) {

        assert(req.params.hasOwnProperty("definitionName"), "Missing request param definitionName");
        assert(req.params.hasOwnProperty("resultId"), "Missing request param resultId");

        const definitionName = safeDefinitionName(req.params.definitionName);
        const resultId = safeId(req.params.resultId);

        const mapper = (result: EsqlateCompleteResultOtherFormat) => {
            return esqlateResultEnsureFullFormatUrl(serviceInformation, definitionName, resultId, result);
        }

        persistence.getResult(definitionName, resultId)
            .then((result: EsqlateResult) => {
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
