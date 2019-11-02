import { EsqlateArgument, EsqlateCompleteResult, EsqlateDefinition, EsqlateResult } from "esqlate-lib";
import { EsqlateErrorNotFoundPersistence } from "./logger";


import assert from "assert";
import {  close as fsClose, mkdir, open as fsOpen, promises as fsPromises, readdir, readFile, rename,  write as fsWrite, writeFile } from "fs";
import * as json2csv from "json2csv";
import { join } from "path";

import { DatabaseCursorResult } from "./QueryRunner";
// import * as csv from "csv-write-stream";

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
        values: () => AsyncIterableIterator<DatabaseCursorResult>): Promise<ResultId>;
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


    public async createResult(
            definitionName: EsqlateDefinition["name"],
            resultId: ResultId,
            stream: () => AsyncIterableIterator<DatabaseCursorResult>): Promise<ResultId> {

        let json: EsqlateCompleteResult = {
            fields: [],
            rows: [],
            complete_data_set: true,
            status: "complete",
        };

        const csvFilename = join.apply(
            null,
            this.getResultFilename(definitionName, resultId),
        ).replace(/\.json$/, ".csv.incomplete");

        const csvFileHandle: number = await this.openFile(csvFilename);
        try {

            for await (const value of stream()) {

                const toWrite = [...value.rows];
                if (!json.rows.length) {
                    const data = value.fields.map(({name}) => name);
                    toWrite.unshift(data);
                }
                await this.writeFile(
                    csvFileHandle,
                    json2csv.parse(toWrite, { header: false, eol: "\r\n" }) + "\r\n",
                );

                // On the first, record the first value with complete_data_set = true
                // On anything other than first loop, just set complete_data_set = false
                json = json.rows.length ?
                    {...json, complete_data_set: false } :
                    {...json, ...value, complete_data_set: true };
            }

        } catch (e) {
            await this.closeFile(csvFileHandle);
            throw e;
        }

        return this.closeFile(csvFileHandle).then(
            () => {
                return Promise.all([
                    this.renameFile(csvFilename, csvFilename.replace(/\.incomplete$/, "")),
                    this.createJson(definitionName, resultId, json),
                ]);
            })
            .then(() => resultId);

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


    private renameFile(src: string, dst: string): Promise<null> {
        return new Promise((resolve, reject) => {
            rename(src, dst, (renameErr) => {
                if (renameErr) { return reject(renameErr); }
                resolve(null);
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


    private createJson(
        definitionName: EsqlateDefinition["name"],
        resultId: ResultId,
        value: EsqlateCompleteResult,
    ): Promise<string> {

        const resultFilename = this.getResultFilename(definitionName, resultId);

        return new Promise((resolve, reject) => {
            try {
                return this.mkFile("", resultFilename, value, (err) => {
                    if (err) { return reject(err); }
                    resolve(resultId);
                });
            } catch (e) {
                reject(e);
            }
        });
    }


    private closeFile(fh: number): Promise<null> {
        return new Promise((resolve, reject) => {
            fsClose(fh, (err) => {
                if (err) { return reject(err); }
                resolve(null);
            });
        });
    }


    private openFile(filename: string): Promise<number> {
        return new Promise((resolve, reject) => {
            fsOpen(filename, "w", (err, handle) => {
                if (err) { return reject(err); }
                resolve(handle);
            });
        });
    }


    private writeFile(fh: number, data: string): Promise<null> {
        return new Promise((resolve, reject) => {
            fsWrite(fh, data, (err) => {
                if (err) { return reject(err); }
                resolve(null);
            });
        });
    }


}

