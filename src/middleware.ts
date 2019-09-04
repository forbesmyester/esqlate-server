import { NextFunction, Request, RequestHandler, Response, static as expressStatic } from "express";
import fs from "fs";
import { join as pathJoin } from "path";
import randCryptoString from "random-crypto-string";
import { Persistence, ResultExistance } from "./persistence";

import { EsqlateDefinition, EsqlateResult, normalize } from "esqlate-lib";
import { EsqlateError, EsqlateErrorEnum, EsqlateErrorInvalidDefinition, EsqlateErrorInvalidRequestBody, EsqlateErrorMissingDefinition, EsqlateErrorMissingLocal, EsqlateErrorMissingRequestParam, EsqlateErrorNotFoundPersistence, Level, Logger } from "./logger";
const DEFINITION_DIRECTORY = "./node_modules/esqlate-lib/test/res/definition";
import Ajv from "ajv";

import * as schemaRequestCreation from "../node_modules/esqlate-lib/res/schema-request-creation-parameter.json";

export interface ServiceInformation {
    getApiRoot: () => string;
}

export interface RequestDependencies { persistence: Persistence; serviceInformation: ServiceInformation; }

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

export function normalizeDefinition(req: Request, _res: Response, next: NextFunction) {

    if (!req.params.hasOwnProperty("definitionName")) {
        return next(new EsqlateErrorMissingRequestParam("definitionName"));
    }

    const definitionName: string = req.params.definitionName;
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
                case EsqlateErrorEnum.MissingDefinition:
                    res.status(404).json({});
                    return next();
                case EsqlateErrorEnum.MissingRequestParam:
                case EsqlateErrorEnum.MissingLocal:
                case EsqlateErrorEnum.InvalidDefinition:
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

export function getRequest({ persistence, serviceInformation: { getApiRoot } }: RequestDependencies) {
    return function getRequestImpl(req: Request, res: Response, next: NextFunction) {

        if (!req.params.hasOwnProperty("requestId")) {
            return next(new EsqlateErrorMissingRequestParam("requestId"));
        }

        persistence.resultExists(req.params.requestId)
            .then((exists) => {
                if (exists.status === ResultExistance.COMPLETE) {
                    res.status(301).json({
                        status: "complete",
                        location: getApiRoot() + exists.resultId,
                    });
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


export function createRequest({ persistence, serviceInformation: { getApiRoot } }: RequestDependencies) {

    const ajv = new Ajv();
    const validate = ajv.compile(schemaRequestCreation);


    return function createRequestImpl(req: Request, res: Response, next: NextFunction) {

        if (!req.params.hasOwnProperty("definitionName")) {
            return next(new EsqlateErrorMissingRequestParam("definitionName"));
        }

        const definitionName = req.params.definitionName;
        const valid = validate(req.body);
        if (!valid) {
            const msg = `${getApiRoot()}/request/${definitionName}`;
            const errors = validate.errors;
            res.status(422).json(errors);
            return next(new EsqlateErrorInvalidRequestBody(msg + ": " + JSON.stringify(errors)));
        }

        randCryptoString(8)
            .then((rand) => {
                persistence.createRequest(
                    rand,
                    definitionName,
                    req.body,
                );
                return rand;
            })
            .then((rand: string) => {
                const loc = `${getApiRoot()}/request/${definitionName}/${rand}`;
                res.setHeader("Location", loc);
                res.status(202).json({ location: loc });
                next();
            })
            .catch((err) => {
                next(err);
            });

    };
}


export function getResult({ persistence }: { persistence: Persistence }) {
    return function getResultImpl(req: Request, res: Response, next: NextFunction) {

        if (!req.params.hasOwnProperty("definitionName")) {
            return next(new EsqlateErrorMissingRequestParam("definitionName"));
        }
        if (!req.params.hasOwnProperty("requestId")) {
            return next(new EsqlateErrorMissingRequestParam("requestId"));
        }

        persistence.getResult(req.params.definitionName, req.params.requestId)
            .then((result: EsqlateResult) => {
                res.json(result);
                next();
            })
            .catch((err) => {
                if (err instanceof EsqlateErrorNotFoundPersistence) {
                    res.status(404).json({});
                    return next(null);
                }
                next(err);
            });
    };
}
