import { EsqlateDefinition, EsqlateRequestCreationParameterItem, EsqlateResult } from "esqlate-lib";
import { readFile, rename, stat, writeFile } from "fs";
import { EsqlateErrorNotFoundPersistence } from "./logger";

export enum ResultExistance {
    NOT_EXISTS,
    COMPLETE,
}

export interface Persistence {
    resultExists(requestId: RequestId): Promise<ResultLocation>;
    createRequest(requestId: RequestId, definitionName: string, values: any): Promise<RequestId>;
    getResult(definitionName: EsqlateDefinition["name"], resultId: ResultId): Promise<EsqlateResult>;
}

interface ResultLocationNotExists {
    status: ResultExistance.NOT_EXISTS;
}

interface ResultLocationComplete {
    status: ResultExistance.COMPLETE;
    resultId: ResultId;
}

type ResultLocation = ResultLocationComplete | ResultLocationNotExists;

export type RequestId = string;
export type ResultId = string;

function filesystemResultExists(requestId: RequestId): Promise<ResultLocation> {

    return new Promise((resolve, reject) => {
        stat(`result-${requestId}.json`, (err) => {
            if ((err) && (err.code === "ENOENT")) {
                resolve({ status: ResultExistance.NOT_EXISTS });
            }
            if (err) { return reject(err); }
            resolve({ resultId: requestId, status: ResultExistance.COMPLETE });
        });
    });
}

function filesytemCreateRequest(requestId: RequestId, definitionName: EsqlateDefinition["name"], values: EsqlateRequestCreationParameterItem[]): Promise<RequestId> {
    // TODO: Validate!
    return new Promise((resolve, reject) => {
        const filename = `_request-${requestId}.json`;
        const writeData = {
            params: values,
            definition: definitionName,
        };
        writeFile(filename, JSON.stringify(writeData), { encoding: "utf8" }, (err) => {
            if (err) { return reject(err); }
            rename(filename, filename.substring(1), (renameErr) => {
                if (renameErr) { return reject(renameErr); }
                resolve(requestId);
            });
        });
    });
}

function filesystemGetResult(definitionName: EsqlateDefinition["name"], resultId: ResultId): Promise<EsqlateResult> {
    return new Promise((resolve, reject) => {
        const filename = `result-${resultId}.json`;
        readFile(filename, { encoding: "utf8" }, (err, data) => {
            if ((err) && (err.code === "ENOENT")) {
                reject(new EsqlateErrorNotFoundPersistence(`Could not load result ${definitionName}/${resultId}`));
            }
            if (err) {
                return reject(err);
            }
            resolve(JSON.parse(data));
        });
    });
}

export function getFilesystemPersistence(): Persistence {
    return {
        resultExists: filesystemResultExists,
        createRequest: filesytemCreateRequest,
        getResult: filesystemGetResult,
    };
}

