import bodyParser from "body-parser";
import express, { NextFunction, Request, Response, static as expressStatic } from "express";
import logger, { Level } from "./logger";
import { captureRequestStart, createRequest, getCaptureRequestEnd, getCaptureRequestErrorHandler, getDefinition, getRequest, getResult, normalizeDefinition, ServiceInformation } from "./middleware";
import nextWrap, { NextWrapDependencies } from "./nextWrap";
import { getFilesystemPersistence } from "./persistence";

const app = express();
app.use(bodyParser.json());

const nextWrapDependencies: NextWrapDependencies = {
    logger,
    setTimeout,
    clearTimeout,
};

const persistence = getFilesystemPersistence();
const serviceInformation: ServiceInformation = {
    getApiRoot: () => "/api/",
};

const nwCaptureRequestStart = nextWrap(nextWrapDependencies, 1000, captureRequestStart);
const nwCaptureRequestEnd = nextWrap(nextWrapDependencies, 1000, getCaptureRequestEnd(logger));
const nwNormalizeDefinition = nextWrap(nextWrapDependencies, 1000, normalizeDefinition);


app.use("/img", expressStatic("img"));


app.get(
    "/api/definition/:definitionName",
    nwCaptureRequestStart,
    nwNormalizeDefinition,
    nextWrap(nextWrapDependencies, 1000, getDefinition),
    nwCaptureRequestEnd,
    getCaptureRequestErrorHandler(logger),
);


app.post(
    "/api/request/:definitionName",
    nwCaptureRequestStart,
    nwNormalizeDefinition,
    nextWrap(nextWrapDependencies, 1000, createRequest({ persistence, serviceInformation })),
    nwCaptureRequestEnd,
    getCaptureRequestErrorHandler(logger),
);


app.get(
    "/api/request/:definitionName/:requestId",
    nwCaptureRequestStart,
    nwNormalizeDefinition,
    nextWrap(nextWrapDependencies, 1000, getRequest({ persistence, serviceInformation })),
    nwCaptureRequestEnd,
    getCaptureRequestErrorHandler(logger),
);


app.get(
    "/api/result/:definitionName/:resultId",
    nwCaptureRequestStart,
    nwNormalizeDefinition,
    nextWrap(nextWrapDependencies, 1000, getResult({ persistence })),
    nwCaptureRequestEnd,
    getCaptureRequestErrorHandler(logger),
);


app.listen(process.env.LISTEN_PORT, () => {
    if (!process.env.hasOwnProperty("LISTEN_PORT")) {
        logger(Level.FATAL, "STARTUP", "no LISTEN_PORT environmental variable defined");
    }
    logger(Level.INFO, "STARTUP", "eslate-server listening on " + process.env.LISTEN_PORT);
});

