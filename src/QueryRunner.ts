import { EsqlateArgument, EsqlateDefinition, EsqlateResult } from "esqlate-lib";
import { EsqlateQueue, EsqlateQueueWorker } from "esqlate-queue";

import { Logger } from "./logger";
import MySQLQueryRunner from "./MySQLQueryRunner";
import PostgreSQLQueryRunner from "./PostgreSQLQueryRunner";


import { ResultId } from "./persistence";


export interface ResultFieldSpec {
    name: string;
    type: string;
}


type ResultRow = any[];


export interface DatabaseCursorResult {
    fields: ResultFieldSpec[];
    rows: ResultRow[];
}


export interface ResultCreated {
    definitionName: EsqlateDefinition["name"];
    resultId: ResultId;
    result: () => AsyncIterableIterator<DatabaseCursorResult>;
}


export interface QueueItem {
    definition: EsqlateDefinition;
    requestId: string;
    serverParameters: EsqlateArgument[];
    userParameters: EsqlateArgument[];
}


export type DemandRunner = (
    definition: EsqlateDefinition,
    serverParameters: EsqlateArgument[],
    userParameters: EsqlateArgument[],
) => Promise<EsqlateResult>;

export type PoolCloser = () => Promise<void>;

export interface DatabaseInterface {
    queue: EsqlateQueue<QueueItem, ResultCreated>;
    demand: DemandRunner;
    closePool: PoolCloser;
    worker: EsqlateQueueWorker<QueueItem, ResultCreated>;
}


export enum DatabaseType {
    PostgreSQL = 1,
    MySQL = 2,
}


export function getQueryRunner(dbType: DatabaseType, parallelism: number = 1, logger: Logger): Promise<DatabaseInterface> {
    switch (dbType) {
        case DatabaseType.PostgreSQL:
            return PostgreSQLQueryRunner(parallelism, logger);
        case DatabaseType.MySQL:
            return MySQLQueryRunner(parallelism, logger);
    }
}
