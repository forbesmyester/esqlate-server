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
const assert = require("assert");
const json5_1 = __importDefault(require("json5"));
const path_1 = require("path");
const random_crypto_string_1 = __importDefault(require("random-crypto-string"));
const schemaRequestCreation = __importStar(require("esqlate-lib/res/schema-request-creation.json"));
const logger_1 = require("./logger");
const persistence_1 = require("./persistence");
const fs_1 = __importDefault(require("fs"));
const path_2 = __importDefault(require("path"));
const ajv = new ajv_1.default();
const schemaDefinition = __importStar(require("esqlate-lib/res/schema-definition.json"));
const ajvValidateDefinition = ajv.compile(schemaDefinition);
async function sequentialPromises(ar, f) {
    const ps = ar.map(f);
    const items = await Promise.all(ps);
    return items.filter((i) => i !== false);
}
const ajvValidateRequestCreation = ajv.compile(schemaRequestCreation);
function getVariables({ getApiRoot }, serverVariableRequester, definitionParameters, req) {
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
        return definitionParameters
            .filter((reqd) => {
            return !available.has(reqd.name);
        })
            .filter((emptyStringIsNull) => !emptyStringIsNull.empty_string_is_null)
            .map(({ name }) => name);
    }
    assert(req.params.hasOwnProperty("definitionName"), "Missing request param definitionName");
    const definitionName = req.params.definitionName;
    const valid = ajvValidateRequestCreation(req.body);
    if (!valid) {
        const errors = ajvValidateRequestCreation.errors;
        const msg = path_1.join(getApiRoot(), "request", definitionName) + ": " + JSON.stringify(errors);
        throw new logger_1.EsqlateErrorInvalidRequestBody(msg);
    }
    const variables = get();
    const missingVariables = getMissingVariables(variables);
    if (missingVariables.length) {
        const errorMsg = "Missing Variables: " + JSON.stringify(missingVariables);
        const msg = path_1.join(getApiRoot(), "request", definitionName) + ": " + JSON.stringify(errorMsg);
        throw new logger_1.EsqlateErrorMissingVariables(msg);
    }
    return variables;
}
exports.getVariables = getVariables;
function certifyDefinition(logger, loggerLevel, fullPath, data) {
    let json;
    try {
        json = json5_1.default.parse(data);
    }
    catch (e) {
        logger(loggerLevel, "DEFINITION", `Could not unserialize definition ${fullPath}`);
        throw new Error(`Could not unserialize definition ${fullPath}`);
    }
    const valid = ajvValidateDefinition(json);
    if (!valid) {
        const errors = ajvValidateDefinition.errors;
        logger(loggerLevel, "DEFINITION", `Definition ${fullPath} has errors ${JSON.stringify(errors)}`);
        throw new Error(`Definition ${fullPath} has errors ${JSON.stringify(errors)}`);
    }
    if (json.name !== path_2.default.basename(fullPath).replace(/\.json5?$/, "")) {
        logger(loggerLevel, "DEFINITION", `Definition ${fullPath} has different name to filename`);
        throw new Error(`Definition ${fullPath} has different name to filename`);
    }
    if ((path_2.default.basename(fullPath).substring(0, 1) !== "_") &&
        hasNoStaticParams(json)) {
        return { title: json.title, name: json.name };
    }
    return false;
}
function hasNoStaticParams(def) {
    return !def.parameters.some((param) => param.type === "static");
}
async function readDefinitionList({ logger }, { loggerLevel, knownDefinitions, definitionDirectory }) {
    function readFile(fullPath) {
        return new Promise((resolve, reject) => {
            fs_1.default.readFile(fullPath, { encoding: "utf8" }, (readFileErr, data) => {
                if (readFileErr) {
                    logger(loggerLevel, "DEFINITION", `Could not read definition ${fullPath}`);
                    return reject(`Could not read filename ${fullPath}`);
                }
                resolve({ data, fullPath });
            });
        });
    }
    function certify({ data, fullPath }) {
        return Promise.resolve(certifyDefinition(logger, loggerLevel, fullPath, data));
    }
    function myReadDir() {
        return new Promise((resolve, reject) => {
            fs_1.default.readdir(definitionDirectory, (readDirErr, filenames) => {
                if (readDirErr) {
                    logger(logger_1.Level.FATAL, "DEFINITION", "Could not list definition in (" + definitionDirectory + ")");
                    reject("Could not list definition");
                }
                resolve(filenames);
            });
        });
    }
    const fullPaths = (await myReadDir())
        .map((filename) => {
        return path_2.default.join(definitionDirectory, filename);
    })
        .filter((fp) => path_2.default.basename(fp).substring(0, 1) !== "_")
        .filter((fp) => knownDefinitions.indexOf(fp) === -1);
    const datas = await sequentialPromises(fullPaths, readFile);
    return sequentialPromises(datas, certify);
}
exports.readDefinitionList = readDefinitionList;
function getLoadDefinition(conf) {
    return function loadDefinition(definitionName) {
        let errCount = 0;
        let sent = false;
        return new Promise((resolve, reject) => {
            function process(err, data) {
                if (sent) {
                    return;
                }
                if ((err) && (errCount++ > 0) && (sent === false)) {
                    sent = true;
                    return reject(new logger_1.EsqlateErrorMissingDefinition(`${definitionName}`));
                }
                if (err) {
                    return;
                }
                let j;
                try {
                    j = json5_1.default.parse(data);
                }
                catch (e) {
                    errCount = errCount + 1;
                    sent = true;
                    return reject(new logger_1.EsqlateErrorInvalidDefinition(`${definitionName}`));
                }
                sent = true;
                resolve(j);
            }
            fs_1.default.readFile(path_1.join(conf.definitionDirectory, definitionName + ".json5"), { encoding: "utf8" }, process);
            fs_1.default.readFile(path_1.join(conf.definitionDirectory, definitionName + ".json"), { encoding: "utf8" }, process);
        });
    };
}
exports.getLoadDefinition = getLoadDefinition;
function createRequestSideEffects({ serverVariableRequester, loadDefinition, persistence, queue, serviceInformation: { getApiRoot } }, req) {
    let definition;
    let variables;
    const definitionName = persistence_1.safeDefinitionName(req.params.definitionName);
    return loadDefinition(definitionName)
        .then((def) => { definition = def; })
        .then(() => {
        variables = getVariables({ getApiRoot }, serverVariableRequester, definition.parameters, req);
    })
        .then(() => random_crypto_string_1.default(8))
        .then((requestId) => {
        return persistence.createRequest(definitionName, requestId, variables).then(() => ({ requestId, definition, variables }));
    });
}
exports.createRequestSideEffects = createRequestSideEffects;
function createRequestFile({ serverVariableRequester, loadDefinition, persistence, queue, serviceInformation: { getApiRoot } }, req) {
    let definition;
    let variables;
    const definitionName = persistence_1.safeDefinitionName(req.params.definitionName);
    return createRequestSideEffects({ serverVariableRequester, loadDefinition, persistence, queue, serviceInformation: { getApiRoot } }, req)
        .then(({ definition, variables, requestId }) => {
        return path_1.join(getApiRoot(), "request", definitionName, requestId);
    });
}
exports.createRequestFile = createRequestFile;
function createRequest({ serverVariableRequester, loadDefinition, persistence, queue, serviceInformation: { getApiRoot } }, req) {
    let definition;
    let variables;
    const definitionName = persistence_1.safeDefinitionName(req.params.definitionName);
    return createRequestSideEffects({ serverVariableRequester, loadDefinition, persistence, queue, serviceInformation: { getApiRoot } }, req)
        .then(({ definition, variables, requestId }) => {
        // TODO: Pass in server variables properly
        const queueItem = {
            definition,
            requestId,
            serverParameters: [],
            userParameters: variables,
        };
        queue.push(queueItem);
        return requestId;
    })
        .then((requestId) => {
        return path_1.join(getApiRoot(), "request", definitionName, requestId);
    });
}
exports.createRequest = createRequest;
function processCmdlineArgumentsToEsqlateArgument(args) {
    return args.reduce((acc, arg) => {
        if (arg.substring(0, 1) === "[") {
            try {
                const ar = JSON.parse(arg);
                if (ar.length < 1) {
                    throw new Error("TOO SHORT");
                }
                const toAdd = {
                    name: "" + ar[0],
                    value: ar[1],
                };
                acc.push(toAdd);
            }
            catch (e) {
                throw new Error(`Error processing parameter ${arg}.\n\nThe error was ${e.message}`);
            }
            return acc;
        }
        return acc;
    }, []);
}
exports.processCmdlineArgumentsToEsqlateArgument = processCmdlineArgumentsToEsqlateArgument;
