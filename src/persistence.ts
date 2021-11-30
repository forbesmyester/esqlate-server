import { EsqlateArgument, EsqlateDefinition, EsqlateResult, EsqlateSuccessResult } from "esqlate-lib";

import { EsqlateErrorNotFoundPersistence } from "./logger";
import { DatabaseCursorResult } from "./QueryRunner";

import assert from "assert";
import { access, close as fsClose, createReadStream, mkdir, open as fsOpen, readdir, readFile, ReadStream, rename,  write as fsWrite, writeFile } from "fs";
import * as json2csv from "json2csv";
import { join } from "path";
import {DefinitionList} from "./functions";


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
    getResultCsvStream(
        definitionName: EsqlateDefinition["name"],
        resultId: ResultId): Promise<ReadStream>;
    getRequest(definitionName: string, requestId: string): Promise<RequestFileData>;
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
        throw new Error(`safeDefinitionName: Path element '${s}' includes invalid characters`);
    }
    return ("" + s);
}

export function safeId(s: string) {
    // if (!("" + s).match(/^[a-zA-Z0-9_]{0,99}(\.((csv)|(json)))?$/)) {
    if (!("" + s).match(/^[a-zA-Z0-9_]{0,99}(\.((csv)|(json)))?$/)) {
        throw new Error(`safeId: Path element '${s}' includes invalid characters`);
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

        let json: EsqlateResult = {
            fields: [],
            rows: [],
            full_data_set: true,
            status: "complete",
        };

        const csvFilename = join.apply(
            null,
            this.getResultFilename(definitionName, resultId),
        ).replace(/\.json$/, ".csv.incomplete");

        const csvFileHandle: number = await this.openFile(csvFilename);
        let createJsonP: null | Promise<string> = null;
        try {

            for await (const value of stream()) {

                const toWrite = [...value.rows];
                if (!json.rows.length) {
                    const csvHeaders = value.fields.map(({name}) => name);
                    toWrite.unshift(csvHeaders);
                    json = {...json, ...value, full_data_set: true };
                } else {
                    if (!createJsonP) {
                        json = {...json, full_data_set: false };
                        createJsonP = this.createJson(definitionName, resultId, json);
                    }
                }

                await this.writeFile(
                    csvFileHandle,
                    json2csv.parse(toWrite, { header: false, eol: "\r\n" }) + "\r\n",
                );

            }

        } catch (e) {
            json = e;
        }
        return this.closeFile(csvFileHandle)
            .then(() => {
                return Promise.all([
                    (json.status === "error") ?
                        Promise.resolve(false) :
                        this.renameFile(
                            csvFilename,
                            csvFilename.replace(/\.incomplete$/, ""),
                        ),
                    createJsonP || this.createJson(definitionName, resultId, json),
                ]);
            })
            .then(() => resultId);
    }


    private getRequestFilename(definitionName: string, requestId: string): string[] {
        return [
            this.storagePath,
            safeDefinitionName(definitionName),
            safeId(requestId).substring(0, this.pathSeperator1),
            safeId(requestId).substring(this.pathSeperator1),
            "request.json",
        ];
    }


    public getRequest(definitionName: string, requestId: string): Promise<RequestFileData> {
        const fnArray = this.getRequestFilename(definitionName, requestId);
        return this.readFile(join(...fnArray)).then((s) => JSON.parse(s) as RequestFileData);
    }

    public createRequest(
            definitionName: EsqlateDefinition["name"],
            requestId: RequestId,
            values: EsqlateArgument[]): Promise<RequestId> {

        assert(requestId.length === this.pathSeperator2, `Request Ids must be ${this.pathSeperator2} characters long`);

        const filename = this.getRequestFilename(definitionName, requestId);

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


    public getResultCsvFilename(definitionName: string, resultId: string): string {
        const filename = join.apply(null, this.getResultFilename(definitionName, resultId));
        return filename.replace(/\.json$/, ".csv");
    }


    public async getResultCsvStream(
        definitionName: EsqlateDefinition["name"],
        resultId: ResultId): Promise<ReadStream> {

        const csvFilename = this.getResultCsvFilename(definitionName, resultId);

        const csvReadable = await this.access(csvFilename);
        if (!csvReadable) {
            throw new EsqlateErrorNotFoundPersistence(`Could not load result ${definitionName}/${resultId}`);
        }
        return createReadStream(csvFilename);
    }


    public getResult(definitionName: EsqlateDefinition["name"], resultId: ResultId): Promise<EsqlateResult> {
        const filename = join.apply(null, this.getResultFilename(definitionName, resultId));
        const csvFilename = filename.replace(/\.json$/, ".csv");

        return Promise.all([this.readFile(filename), this.access(csvFilename)])
            .then(([filedata, csvAvailable]: [string, boolean]) => {
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
                    throw new EsqlateErrorNotFoundPersistence(`Could not load result ${definitionName}/${resultId}`);
                }
                throw err;
            });
    }


    public async* outstandingRequestId() {
        for (const defname of await this.readdir(this.storagePath)) {
            for (const reqIdP1 of await this.readdir(join(this.storagePath, defname))) {
                for (const reqIdP2 of await this.readdir(join(this.storagePath, defname, reqIdP1))) {
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
        value: EsqlateResult,
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


    private readFile(s: string): Promise<string> {
        return new Promise((resolve, reject) => {
            readFile(s, { encoding: "utf8" }, (err, data) => {
                if (err) { return reject(err); }
                resolve(data);
            });
        });
    }


    private access(s: string): Promise<boolean> {
        return new Promise((resolve) => {
            access(s, (err) => {
                if (err) { return resolve(false); }
                resolve(true);
            });
        });
    }


    protected readdir(s: string): Promise<string[]> {
        return new Promise((resolve, reject) => {
            readdir(s, (err, files) => {
                if (err) { return reject(err); }
                resolve(files);
            });
        });
    }


    public async getQueue(definitionNames: string[]): Promise<{definition: string; id: string}[]> {

        interface ListingItem {
            parentDir: string;
            child: string;
        }

        interface ListingItems {
            parentDir: ListingItem["parentDir"];
            child: ListingItem["child"][];
        }

        function isDefinitionName(maybeDirname: ListingItem) {
            return definitionNames.some((dn) => dn == maybeDirname.child)
        }

        function flatten<X>(x: X[][]): X[] {
            return ([] as X[]).concat(...x);
        }

        function listingItemToFullPath(li: ListingItem): string {
            return join(li.parentDir, li.child);
        }

        // const readDirs: (dirs: string[]) => Promise<ListingItem[]> = (dirs: string[]) => {
        function readDirsImpl(readdir: (s: string) => Promise<string[]>, dirs: string[]): Promise<ListingItem[]> {
            let proms = Promise.all(dirs.map(
                (d) => {
                    return readdir(d)
                        .then((files) => {
                            return files.map((f) => {
                                return { parentDir: d, child: f };
                            });
                        })
                        .catch((e) => {
                            if (e.code == "ENOENT") { return [] as ListingItem[]; }
                            if (e.code == "ENOTDIR") { return [] as ListingItem[]; }
                            return [] as ListingItem[];
                            // throw e;
                        });
                }
            ));
            return proms.then((ps) => flatten(ps));
        }

        function groupListingItem(items: ListingItem[]): ListingItems[] {
            let m: Map<ListingItem["parentDir"], ListingItem[]> = new Map();
            for (const item of items) {
                let ar = m.get(item.parentDir) || [];
                ar.push(item);
                m.set(item.parentDir, ar);
            }
            let r: ListingItems[] = [];
            for (const [parentDir, items] of m) {
                let toAdd: ListingItems = { parentDir, child: [] };
                for (const item of items) {
                    toAdd.child.push(item.child);
                }
                r.push(toAdd);
            }
            return r;
        }

        function noRequest({child}: ListingItems): boolean {
            return child.indexOf("request.json") > -1;
        }

        function hasResults({child}: ListingItems): boolean {
            let ids: Set<string> = new Set();
            for (const c of child) {
                let end = '';
                if (c.substr(-11) == '-result.csv') {
                    end = '-result.csv';
                }
                if (c.substr(-12) == '-result.json') {
                    end = '-result.json';
                }
                let start = c.substr(0, c.length - end.length)
                if (ids.has(start)) {
                    return true;
                }
                if (end) {
                    ids.add(start);
                }
            }
            return false;
        }

        function not<A>(f: (a: A) => boolean): (a: A) => boolean {
            return function(a: A) {
                return !f(a);
            };
        }

        function extractRequestId(acc: {definition: string; id: string}[], {parentDir}: ListingItems): {definition: string; id: string}[] {
            let m = parentDir.match(/([^\/]{1,128})\/([^\/]{1,128})\/([^\/]{1,128})$/);
            if (!m) { return acc; }
            try {
                acc.push({definition: m[1], id: safeId(m[2] + m[3])});
            } catch (_e) {
                // pass through
            }
            return acc;
        }

        const readDirs = readDirsImpl.bind(null, this.readdir);

        const definitions: ListingItem[] = await readDirs([this.storagePath]);
        const idPart1: ListingItem[] = await readDirs(
            definitions.filter(isDefinitionName).map(listingItemToFullPath)
        );
        const idpart2: ListingItem[] = await readDirs(
            idPart1.map(listingItemToFullPath)
        );
        const files: ListingItem[] = await readDirs(
            idpart2.map(listingItemToFullPath)
        );

        return groupListingItem(files)
            .filter(noRequest)
            .filter(not(hasResults))
            .reduce(extractRequestId, []);
    }


}

