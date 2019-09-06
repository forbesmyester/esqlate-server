import assert = require("assert");
import { NextFunction, Request, Response } from "express";
import fs from "fs";
import { join as pathJoin } from "path";

import Ajv from "ajv";
import { EsqlateDefinition, EsqlateRequestCreationParameter, EsqlateRequestCreationParameterItem, EsqlateResult } from "esqlate-lib";
import { EsqlateQueue } from "esqlate-queue";
import randCryptoString from "random-crypto-string";

import { EsqlateError, EsqlateErrorEnum, EsqlateErrorInvalidDefinition, EsqlateErrorInvalidRequestBody, EsqlateErrorInvalidRequestParameter, EsqlateErrorMissingDefinition, EsqlateErrorMissingLocal, EsqlateErrorMissingVariables, EsqlateErrorNotFoundPersistence, Level, Logger } from "./logger";
import { Persistence } from "./persistence";
import { QueueItem, ResultCreated } from "./QueryRunner";

import * as schemaRequestCreation from "esqlate-lib/res/schema-request-creation-parameter.json";

// TODO: Move types to seperate files

const DEFINITION_DIRECTORY = "./node_modules/esqlate-lib/test/res/definition";

export interface ServiceInformation {
    getApiRoot: (req: Request) => string;
}

export interface ServerVariableRequester {
    listServerVariable: (req: Request) => string[];
    getServerVariable: (req: Request, name: string) => EsqlateRequestCreationParameterItem["field_value"];
}

interface NeedsPersistence { persistence: Persistence; }
interface NeedsServiceInformation { serviceInformation: ServiceInformation; }
interface NeedsQueue { queue: EsqlateQueue<QueueItem, ResultCreated>; }
interface NeedsServerVariableRequester { serverVariableRequester: ServerVariableRequester; }

interface Local { [k: string]: any; }

function setRequestLocal(req: Request, k: string, v: any) {
    const other: Local = getRequestLocal(req);
    other[k] = v;
    Object.assign(req, { local: other });
}

function getRequestLocalKey(k: string, r: Request): any {
    const l = getRequestLocal(r);
    if (!l.hasOwnProperty(k)) {
        throw new EsqlateErrorMissingLocal(`${k}`);
    }
    return l[k];
}

function getRequestLocal(r: Request): Local {
    return (r as Request & { local: Local }).local || {};
}

export function captureRequestStart(req: Request, _res: Response, next: NextFunction) {
    setRequestLocal(req, "start", new Date().getTime());
    next();
}

function safeRequestParameter(pathElement: string, pathValue: string) {
    const r = "" + pathValue;
    if (r.length > 512) {
        throw new EsqlateErrorInvalidRequestParameter(`Parameter ${pathElement} is too long`);
    }
    if (!r.match(/^[a-zA-Z0-9_\-]+$/)) {
        throw new EsqlateErrorInvalidRequestParameter(`Parameter ${pathElement} includes invalid characters ${r}`);
    }
    return r;

}

export function loadDefinition(req: Request, _res: Response, next: NextFunction) {

    assert(req.params.hasOwnProperty("definitionName"), "missing request param definitionName");

    const definitionName: string = safeRequestParameter(
        "definitionName",
        req.params.definitionName,
    );

    fs.readFile(
        pathJoin(DEFINITION_DIRECTORY, definitionName + ".json"),
        { encoding: "utf8" },
        (err, data) => {
            if (err) {
                return next(new EsqlateErrorMissingDefinition(`${definitionName}`));
            }
            let j: EsqlateDefinition;
            try {
                j = JSON.parse(data);
            } catch (e) {
                return next(new EsqlateErrorInvalidDefinition(`${definitionName}`));
            }
            setRequestLocal(req, "definition", j);
            next();
        },
    );
}

function logError(logger: Logger, err: Error) {

    if (err instanceof EsqlateError) {
        logger(Level.INFO, err.code, err.message, err);
    }

}

export function getCaptureRequestErrorHandler(logger: Logger) {
    return function captureRequestErrorHandler(err: null | undefined | Error, _req: Request, res: Response, next: NextFunction) {
        if (err && (err instanceof EsqlateError)) {
            logError(logger, err);
            switch (err.code) {
                case EsqlateErrorEnum.InvalidRequestParameter:
                case EsqlateErrorEnum.MissingVariable:
                case EsqlateErrorEnum.InvalidRequestBody:
                    res.status(422).json({ error: err.message });
                    return next();
                case EsqlateErrorEnum.NotFoundPersistenceError:
                case EsqlateErrorEnum.MissingDefinition:
                    res.status(404).json({ error: err.message });
                    return next();
                case EsqlateErrorEnum.MissingRequestParam:
                case EsqlateErrorEnum.MissingLocal:
                case EsqlateErrorEnum.InvalidDefinition:
                default:
                    res.status(500).json({ error: err.message });
                    return next(err);
            }
        }
        next(err);
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
export function getDefinition(req: Request, res: Response, next: NextFunction) {
    res.json(getRequestLocalKey("definition", req));
    next();
}


export function outstandingRequestId({ persistence }: NeedsPersistence) {

    async function outstandingRequestIdSender(_req: Request, res: Response) {

        res.status(200);
        for await (const id of persistence.outstandingRequestId()) {
            res.write(JSON.stringify(id) + "\n");
        }
        res.end();
    }

    return function outstandingRequestIdImpl(req: Request, res: Response, next: NextFunction) {
        outstandingRequestIdSender(req, res)
            .then(() => next())
            .catch((err) => next(err));
    };

}


export function getRequest({ persistence, serviceInformation: { getApiRoot } }: NeedsPersistence & NeedsServiceInformation) {
    return function getRequestImpl(req: Request, res: Response, next: NextFunction) {

        assert(req.params.hasOwnProperty("definitionName"), "Missing request param definitionName");

        assert(req.params.hasOwnProperty("requestId"), "Missing request param requestId");

        const definitionName = safeRequestParameter(
            "definitionName",
            req.params.definitionName,
        );
        const requestId = safeRequestParameter(
            "requestId",
            req.params.requestId,
        );

        persistence.getResultIdForRequest(definitionName, requestId)
            .then((resultId) => {
                if (resultId) {
                    const location = pathJoin(
                        getApiRoot(req),
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


export function createRequest({ serverVariableRequester, persistence, queue, serviceInformation: { getApiRoot } }: NeedsPersistence & NeedsServiceInformation & NeedsQueue & NeedsServerVariableRequester) {

    const ajv = new Ajv();
    const validate = ajv.compile(schemaRequestCreation);

    function getMissingVariables(definition: EsqlateDefinition, reqBody: EsqlateRequestCreationParameter) {
        const available = new Set(reqBody.map((rb) => rb.field_name));
        return definition.variables
            .filter((reqd) => {
                return !available.has(reqd.name);
            })
            .map(({ name }) => name);
    }

    function getVariables(req: Request): EsqlateRequestCreationParameter {
        const serverVariables: EsqlateRequestCreationParameter = serverVariableRequester.listServerVariable(req).map(
            (name) => ({ field_name: name, field_value: serverVariableRequester.getServerVariable(req, name) }),
        );

        return serverVariables.reduce(
            (acc: EsqlateRequestCreationParameter, sv) => {
                const r = acc.filter((a) => !(("" + a.field_name) === ("" + sv.field_name)));
                r.push(sv);
                return r;
            },
            req.body.concat([]),
        );
    }

    return function createRequestImpl(req: Request, res: Response, next: NextFunction) {

        assert(req.params.hasOwnProperty("definitionName"), "Missing request param definitionName");

        const definition: EsqlateDefinition = getRequestLocalKey("definition", req);
        const definitionName = safeRequestParameter(
            "definitionName",
            req.params.definitionName,
        );
        const variables = getVariables(req);
        const valid = validate(variables);

        if (!valid) {
            const errors = validate.errors;
            const msg = pathJoin(
                getApiRoot(req),
                "request",
                definitionName,
            ) + ": " + JSON.stringify(errors);
            return next(new EsqlateErrorInvalidRequestBody(msg));
        }

        const missingVariables = getMissingVariables(definition, variables);
        if (missingVariables.length) {
            const errorMsg = "Missing Variables: " + JSON.stringify(missingVariables);
            const msg = pathJoin(
                getApiRoot(req),
                "request",
                definitionName,
            ) + ": " + JSON.stringify(errorMsg);
            return next(new EsqlateErrorMissingVariables(msg));
        }

        randCryptoString(8)
            .then((requestId) => {
                return persistence.createRequest(
                    definitionName,
                    requestId,
                    variables,
                ).then(() => requestId);
            })
            .then((requestId) => {
                const queueItem: QueueItem = {
                    definition,
                    requestId,
                    serverParameters: [],
                    userParameters: variables,
                };
                queue.push(queueItem);
                return queueItem;
            })
            .then((qi: QueueItem) => {
                const loc = pathJoin(getApiRoot(req), "request", definitionName, qi.requestId);
                res.setHeader("Location", loc);
                res.status(202).json({ location: loc });
                next();
            })
            .catch((err: Error) => { next(err); });

    };
}


export function getResult({ persistence }: NeedsPersistence) {
    return function getResultImpl(req: Request, res: Response, next: NextFunction) {

        assert(req.params.hasOwnProperty("definitionName"), "Missing request param definitionName");

        assert(req.params.hasOwnProperty("resultId"), "Missing request param resultId");

        const definitionName = safeRequestParameter(
            "definitionName",
            req.params.definitionName,
        );
        const resultId = safeRequestParameter(
            "resultId",
            req.params.resultId,
        );

        persistence.getResult(definitionName, resultId)
            .then((result: EsqlateResult) => {
                res.json(result);
                next();
            })
            .catch((err) => { next(err); });
    };
}
