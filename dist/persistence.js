"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.FilesystemPersistence = exports.safeId = exports.safeDefinitionName = exports.ResultExistance = void 0;
const logger_1 = require("./logger");
const assert_1 = __importDefault(require("assert"));
const fs_1 = require("fs");
const json2csv = __importStar(require("json2csv"));
const path_1 = require("path");
var ResultExistance;
(function (ResultExistance) {
    ResultExistance["NOT_EXISTS"] = "pending";
    ResultExistance["COMPLETE"] = "complete";
})(ResultExistance = exports.ResultExistance || (exports.ResultExistance = {}));
function safeDefinitionName(s) {
    if (!("" + s).match(/^_?[a-z][a-z0-9_]{0,99}$/)) {
        throw new Error(`Path element '${s}' includes invalid characters`);
    }
    return ("" + s);
}
exports.safeDefinitionName = safeDefinitionName;
function safeId(s) {
    if (!("" + s).match(/^[a-zA-Z0-9_]{0,99}$/)) {
        throw new Error(`Path element '${s}' includes invalid characters`);
    }
    return ("" + s);
}
exports.safeId = safeId;
class FilesystemPersistence {
    constructor(storagePath) {
        this.storagePath = storagePath;
        this.pathSeperator1 = 2;
        this.pathSeperator2 = 8;
        fs_1.mkdir(storagePath, (err) => {
            if (err) {
                if (err.code === "EEXIST") {
                    return;
                }
                throw new Error("Could not set up FilesystemPersistence");
            }
        });
    }
    async createResult(definitionName, resultId, stream) {
        let json = {
            fields: [],
            rows: [],
            full_data_set: true,
            status: "complete",
        };
        const csvFilename = path_1.join.apply(null, this.getResultFilename(definitionName, resultId)).replace(/\.json$/, ".csv.incomplete");
        const csvFileHandle = await this.openFile(csvFilename);
        let createJsonP = null;
        try {
            for await (const value of stream()) {
                const toWrite = [...value.rows];
                if (!json.rows.length) {
                    const csvHeaders = value.fields.map(({ name }) => name);
                    toWrite.unshift(csvHeaders);
                    json = { ...json, ...value, full_data_set: true };
                }
                else {
                    if (!createJsonP) {
                        json = { ...json, full_data_set: false };
                        createJsonP = this.createJson(definitionName, resultId, json);
                    }
                }
                await this.writeFile(csvFileHandle, json2csv.parse(toWrite, { header: false, eol: "\r\n" }) + "\r\n");
            }
        }
        catch (e) {
            json = e;
        }
        return this.closeFile(csvFileHandle)
            .then(() => {
            return Promise.all([
                (json.status === "error") ?
                    Promise.resolve(false) :
                    this.renameFile(csvFilename, csvFilename.replace(/\.incomplete$/, "")),
                createJsonP || this.createJson(definitionName, resultId, json),
            ]);
        })
            .then(() => resultId);
    }
    createRequest(definitionName, requestId, values) {
        assert_1.default(requestId.length === this.pathSeperator2, `Request Ids must be ${this.pathSeperator2} characters long`);
        const filename = [
            this.storagePath,
            safeDefinitionName(definitionName),
            safeId(requestId).substring(0, this.pathSeperator1),
            safeId(requestId).substring(this.pathSeperator1),
            "request.json",
        ];
        return new Promise((resolve, reject) => {
            const writeData = {
                params: values,
                definition: definitionName,
            };
            this.mkFile("", filename, writeData, (err) => {
                if (err) {
                    return reject(err);
                }
                resolve(requestId);
            });
        });
    }
    async getResultCsvStream(definitionName, resultId) {
        const filename = path_1.join.apply(null, this.getResultFilename(definitionName, resultId));
        const csvFilename = filename.replace(/\.json$/, ".csv");
        const csvReadable = await this.access(csvFilename);
        if (!csvReadable) {
            throw new logger_1.EsqlateErrorNotFoundPersistence(`Could not load result ${definitionName}/${resultId}`);
        }
        return fs_1.createReadStream(csvFilename);
    }
    getResult(definitionName, resultId) {
        const filename = path_1.join.apply(null, this.getResultFilename(definitionName, resultId));
        const csvFilename = filename.replace(/\.json$/, ".csv");
        return Promise.all([this.readFile(filename), this.access(csvFilename)])
            .then(([filedata, csvAvailable]) => {
            const j = JSON.parse(filedata);
            if (csvAvailable) {
                return {
                    ...j,
                    full_format_urls: [{
                            type: "text/csv",
                            location: csvFilename.replace(/.*\//, ""),
                        }],
                };
            }
            return j;
        })
            .catch((err) => {
            if ((err) && (err.code === "ENOENT")) {
                throw new logger_1.EsqlateErrorNotFoundPersistence(`Could not load result ${definitionName}/${resultId}`);
            }
            throw err;
        });
    }
    async *outstandingRequestId() {
        for (const defname of await this.readdir(this.storagePath)) {
            for (const reqIdP1 of await this.readdir(path_1.join(this.storagePath, defname))) {
                for (const reqIdP2 of await this.readdir(path_1.join(this.storagePath, defname, reqIdP1))) {
                    const reqOb = await this.getResultIdForRequest(defname, reqIdP1 + reqIdP2);
                    if (reqOb) {
                        yield reqIdP1 + reqIdP2;
                    }
                }
            }
        }
    }
    getResultIdForRequest(definitionName, requestId) {
        const searchDirectory = path_1.join(this.storagePath, safeDefinitionName(definitionName), safeId(requestId).substring(0, this.pathSeperator1), safeId(requestId).substring(this.pathSeperator1));
        return new Promise((resolve, reject) => {
            fs_1.readdir(searchDirectory, (err, items) => {
                if (err) {
                    return reject(err);
                }
                resolve(items.reduce((acc, item) => {
                    const match = item.match(/^([a-zA-Z0-9]{0,99})\-result\.json$/);
                    if (match) {
                        return requestId + match[1];
                    }
                    return acc;
                }, null));
            });
        });
    }
    getResultFilename(definitionName, resultId) {
        const myResultId = safeId(resultId);
        assert_1.default(myResultId.length > this.pathSeperator2, "Result Ids must be at least 6 characters long");
        return [
            this.storagePath,
            safeDefinitionName(definitionName),
            myResultId.substring(0, this.pathSeperator1),
            myResultId.substring(this.pathSeperator1, this.pathSeperator2),
            `${myResultId.substring(this.pathSeperator2)}-result.json`,
        ];
    }
    atomicWrite(directory, basename, writeData, next) {
        const src = path_1.join(directory, "_" + basename);
        const dst = path_1.join(directory, basename);
        fs_1.writeFile(src, JSON.stringify(writeData), { encoding: "utf8" }, (err) => {
            if (err) {
                return next(err);
            }
            fs_1.rename(src, dst, (renameErr) => {
                if (renameErr) {
                    return next(renameErr);
                }
                next();
            });
        });
    }
    renameFile(src, dst) {
        return new Promise((resolve, reject) => {
            fs_1.rename(src, dst, (renameErr) => {
                if (renameErr) {
                    return reject(renameErr);
                }
                resolve(null);
            });
        });
    }
    mkFile(pre, [current, ...paths], data, next) {
        if (paths.length) {
            return fs_1.mkdir(path_1.join(pre, current), (err) => {
                if (err) {
                    if (err.code === "EEXIST") {
                        return this.mkFile(path_1.join(pre, current), paths, data, next);
                    }
                    return next(err);
                }
                return this.mkFile(path_1.join(pre, current), paths, data, next);
            });
        }
        this.atomicWrite(pre, current, data, next);
    }
    createJson(definitionName, resultId, value) {
        const resultFilename = this.getResultFilename(definitionName, resultId);
        return new Promise((resolve, reject) => {
            try {
                return this.mkFile("", resultFilename, value, (err) => {
                    if (err) {
                        return reject(err);
                    }
                    resolve(resultId);
                });
            }
            catch (e) {
                reject(e);
            }
        });
    }
    closeFile(fh) {
        return new Promise((resolve, reject) => {
            fs_1.close(fh, (err) => {
                if (err) {
                    return reject(err);
                }
                resolve(null);
            });
        });
    }
    openFile(filename) {
        return new Promise((resolve, reject) => {
            fs_1.open(filename, "w", (err, handle) => {
                if (err) {
                    return reject(err);
                }
                resolve(handle);
            });
        });
    }
    writeFile(fh, data) {
        return new Promise((resolve, reject) => {
            fs_1.write(fh, data, (err) => {
                if (err) {
                    return reject(err);
                }
                resolve(null);
            });
        });
    }
    readFile(s) {
        return new Promise((resolve, reject) => {
            fs_1.readFile(s, { encoding: "utf8" }, (err, data) => {
                if (err) {
                    return reject(err);
                }
                resolve(data);
            });
        });
    }
    access(s) {
        return new Promise((resolve) => {
            fs_1.access(s, (err) => {
                if (err) {
                    return resolve(false);
                }
                resolve(true);
            });
        });
    }
    readdir(s) {
        return new Promise((resolve, reject) => {
            fs_1.readdir(s, (err, files) => {
                if (err) {
                    return reject(err);
                }
                resolve(files);
            });
        });
    }
}
exports.FilesystemPersistence = FilesystemPersistence;
