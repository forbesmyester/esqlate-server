import { EsqlateArgument, EsqlateDefinition, EsqlateResult } from "esqlate-lib";
import { EsqlateErrorNotFoundPersistence } from "./logger";


import assert from "assert";
import { mkdir, readdir, readFile, rename, writeFile } from "fs";
import { promises as fsPromises } from "fs";
import { join } from "path";

const { readdir: readdirP } = fsPromises;

export enum ResultExistance {
    NOT_EXISTS = "pending",
    COMPLETE = "complete",
}

export interface Persistence {
    outstandingRequestId(): AsyncIterableIterator<RequestId>;
    getResultIdForRequest(definitionName: EsqlateDefinition["name"], requestId: RequestId): Promise<ResultId | null>;
    createRequest(
        definitionName: EsqlateDefinition["name"],
        requestId: RequestId,
        values: EsqlateArgument[]): Promise<RequestId>;
    getResult(definitionName: EsqlateDefinition["name"], resultId: ResultId): Promise<EsqlateResult>;
    createResult(
        definitionName: EsqlateDefinition["name"],
        resultId: ResultId,
        values: EsqlateResult): Promise<ResultId>;
}

export type RequestId = string;
export type ResultId = string;

export interface RequestFileData {
    params: EsqlateArgument[];
    definition: string;
}

export interface LoadedRequstFileData {
    id: RequestId;
    requestFileData: RequestFileData;
}

export function safeDefinitionName(s: string) {
    if (!("" + s).match(/^_?[a-z][a-z0-9_]{0,99}$/)) {
        throw new Error(`Path element '${s}' includes invalid characters`);
    }
    return ("" + s);
}

export function safeId(s: string) {
    if (!("" + s).match(/^[a-zA-Z0-9_]{0,99}$/)) {
        throw new Error(`Path element '${s}' includes invalid characters`);
    }
    return ("" + s);
}

export class FilesystemPersistence implements Persistence {

    private pathSeperator1: number = 2;
    private pathSeperator2: number = 8;

    constructor(private storagePath: string) {
        mkdir(storagePath, (err) => {
            if (err) {
                if (err.code === "EEXIST") {
                    return;
                }
                throw new Error("Could not set up FilesystemPersistence");
            }
        });
    }


    public createResult(
            definitionName: EsqlateDefinition["name"],
            resultId: ResultId,
            values: EsqlateResult): Promise<ResultId> {

        const resultFilename = this.getResultFilename(definitionName, resultId);

        return new Promise((resolve, reject) => {
            return this.mkFile("", resultFilename, values, (err) => {
                if (err) { return reject(err); }
                resolve(resultId);
            });
        });
    }


    public createRequest(
            definitionName: EsqlateDefinition["name"],
            requestId: RequestId,
            values: EsqlateArgument[]): Promise<RequestId> {

        assert(requestId.length === this.pathSeperator2, `Request Ids must be ${this.pathSeperator2} characters long`);

        const filename = [
            this.storagePath,
            safeDefinitionName(definitionName),
            safeId(requestId).substring(0, this.pathSeperator1),
            safeId(requestId).substring(this.pathSeperator1),
            "request.json",
        ];

        return new Promise((resolve, reject) => {
            const writeData: RequestFileData = {
                params: values,
                definition: definitionName,
            };
            this.mkFile("", filename, writeData, (err) => {
                if (err) { return reject(err); }
                resolve(requestId);
            });
        });
    }


    public getResult(definitionName: EsqlateDefinition["name"], resultId: ResultId): Promise<EsqlateResult> {
        return new Promise((resolve, reject) => {
            const filename = join.apply(null, this.getResultFilename(definitionName, resultId));
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


    public async* outstandingRequestId() {
        for (const defname of await readdirP(this.storagePath)) {
            for (const reqIdP1 of await readdirP(join(this.storagePath, defname))) {
                for (const reqIdP2 of await readdirP(join(this.storagePath, defname, reqIdP1))) {
                    const reqOb = await this.getResultIdForRequest(defname, reqIdP1 + reqIdP2);
                    if (reqOb) {
                        yield reqIdP1 + reqIdP2;
                    }
                }
            }
        }
    }


    public getResultIdForRequest(
            definitionName: EsqlateDefinition["name"],
            requestId: RequestId): Promise<ResultId | null> {

        const searchDirectory = join(
            this.storagePath,
            safeDefinitionName(definitionName),
            safeId(requestId).substring(0, this.pathSeperator1),
            safeId(requestId).substring(this.pathSeperator1),
        );

        return new Promise((resolve, reject) => {
            readdir(searchDirectory, (err, items) => {

                if (err) { return reject(err); }

                resolve(items.reduce(
                    (acc: null | RequestId, item) => {
                        const match = item.match(/^([a-zA-Z0-9]{0,99})\-result\.json$/);
                        if (match) {
                            return requestId + match[1];
                        }
                        return acc;
                    },
                    null,
                ));

            });
        });
    }


    private getResultFilename(definitionName: string, resultId: string): string[] {

        const myResultId = safeId(resultId);
        assert(myResultId.length > this.pathSeperator2, "Result Ids must be at least 6 characters long");

        return [
            this.storagePath,
            safeDefinitionName(definitionName),
            myResultId.substring(0, this.pathSeperator1),
            myResultId.substring(this.pathSeperator1, this.pathSeperator2),
            `${myResultId.substring(this.pathSeperator2)}-result.json`,
        ];
    }


    private atomicWrite(directory: string, basename: string, writeData: any, next: (err?: Error) => void) {
        const src = join(directory, "_" + basename);
        const dst = join(directory, basename);
        writeFile(src, JSON.stringify(writeData), { encoding: "utf8" }, (err) => {
            if (err) { return next(err); }
            rename(src, dst, (renameErr) => {
                if (renameErr) { return next(renameErr); }
                next();
            });
        });
    }


    private mkFile(pre: string, [current, ...paths]: string[], data: any, next: (err?: Error) => void): void {
        if (paths.length) {
            return mkdir(join(pre, current), (err) => {
                if (err) {
                    if (err.code === "EEXIST") {
                        return this.mkFile(join(pre, current), paths, data, next);
                    }
                    return next(err);
                }
                return this.mkFile(join(pre, current), paths, data, next);
            });
        }
        this.atomicWrite(pre, current, data, next);
    }

}

