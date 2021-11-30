import * as yargs from "yargs";

import open from "open";
import {EsqlateRequestCreation, EsqlateDefinition, EsqlateCompleteResultOtherFormat, EsqlateArgument, EsqlateResult} from "esqlate-lib";

import { createRequestFile, CreateRequestDeps, getLoadDefinition, Input, processCmdlineArgumentsToEsqlateArgument, readDefinitionList, ServerVariableRequester } from "./functions";
import { stderrLogger as logger, Level } from "./logger";
import { FilesystemPersistence, RequestFileData, ResultId } from "./persistence";
import { DatabaseType, getQueryRunner, QueueItem, DatabaseInterface, ResultCreated } from "./QueryRunner";
import {join, resolve} from "path";
import {esqlateResultEnsureFullFormatUrl} from "./middleware";
import { createInterface } from 'readline';
import { spawn } from 'child_process';
import { join as pathJoin } from "path";
import {createReadStream} from "fs";

const DEFINITION_DIRECTORY: string = process.env.DEFINITION_DIRECTORY || (process.cwd() + "/example_definition");
const loadDefinition = getLoadDefinition({ definitionDirectory: DEFINITION_DIRECTORY });
const serverVariableRequester: ServerVariableRequester = {
    listServerVariable: (_req: Input<any>) => {
        return [];
    },
    getServerVariable: (_req: Input<any>, _name: string) => {
        return "";
    },
};
const persistence = new FilesystemPersistence("persistence");
const serviceInformation = { getApiRoot: () => "/" };


function printJSON(j: any): Promise<void> {
    return new Promise((resolve, reject) => {
        process.stdout.write(JSON.stringify(j) + "\n", (err) => {
            if (err) { return reject(err); }
            resolve();
        });
    });
}


function getSubCommand(args: yargs.Arguments): string {
    return args._ && args._[0];
}


function output(promise: Promise<any>) {
    return promise
        .then((lst) => printJSON(lst))
        .then(() => { process.exit(0); })
        .catch((e) => {
            printJSON({ error: e.message }).finally(() => { process.exit(1); });
        });
}


class CmdRunner {

    private queryRunner: null | DatabaseInterface;

    constructor(private databaseType: null | DatabaseType) {
        this.queryRunner = null;
    }

    async getQueryRunner(): Promise<DatabaseInterface> {
        if (this.databaseType === null) {
            throw new Error("No database type set");
        }
        if (this.queryRunner) {
            return Promise.resolve(this.queryRunner);
        }
        this.queryRunner = await getQueryRunner(this.databaseType, 10, logger);
        return this.queryRunner;
    }

    definitionList() {
        const deps = { logger };
        const conf = {
            loggerLevel: Level.WARN,
            knownDefinitions: [],
            definitionDirectory: DEFINITION_DIRECTORY,
        };

        return readDefinitionList(deps, conf);
    }

    definition(name: string) {
        return loadDefinition(name);
    }


    async process(queryRunner: DatabaseInterface, definitionName: string, requestId: string) {

        const requestFileData = await persistence.getRequest(
            definitionName,
            requestId
        );
        const definition = await loadDefinition(definitionName);
        const resultCreated = await queryRunner.worker({
            definition,
            requestId,
            serverParameters: [],
            userParameters: requestFileData.params
        });

        const resultId = await persistence.createResult(
            resultCreated.definitionName,
            resultCreated.resultId,
            resultCreated.result
        );

        return `${serviceInformation.getApiRoot()}result/${definitionName}/${resultId}`;
    }

    async request(definitionName: string, args: EsqlateArgument[], alsoProcess: boolean) {

        const queryRunner: DatabaseInterface = await this.getQueryRunner();

        const deps: CreateRequestDeps = {
            serverVariableRequester,
            persistence,
            queue: queryRunner.queue,
            serviceInformation,
            loadDefinition,
        };

        let location = await createRequestFile(
            deps,
            {
                params: { definitionName },
                body: { arguments: args }
            }
        );

        if (alsoProcess) {
            setTimeout(
                () => {
                    this.process(queryRunner, definitionName, location.replace(/.*\//, ''));
                },
                0
            );
        }

        return { location };
    }

    async demand(definitionName: string, args: EsqlateArgument[]): Promise<EsqlateResult> {

        const queryRunner: DatabaseInterface = await this.getQueryRunner();

        return queryRunner.demand(
            await loadDefinition(definitionName),
            [],
            args
        );
    }

    async requestStatus(definitionName: string, requestId: string) {

        const resultId: ResultId | null = await persistence.getResultIdForRequest(
            definitionName,
            requestId
        );

        if (resultId === null) {
            return { status: "pending" };
        }

        const location = pathJoin(
            serviceInformation.getApiRoot(),
            "result",
            definitionName,
            resultId
        );

        return { status: "complete", location };
    }

    async result(definitionName: string, resultId: string) {

        const mapper = (result: EsqlateCompleteResultOtherFormat) => {
            return esqlateResultEnsureFullFormatUrl(
                serviceInformation,
                definitionName,
                resultId,
                result
            );
        }

        const result = await persistence.getResult(definitionName, resultId)

        if (result.status == "error") {
            return result;
        }
        return {
            ...result,
            status: result.full_format_urls ? "complete" : "preview",
            full_format_urls: (result.full_format_urls || []).map(mapper),
        };

    }

    async download(definitionName: string, resultIdWithExtension: string, stream: boolean = false) {

        const resultId = resultIdWithExtension.replace(/\..*/, '');

        const result = await persistence.getResult(
            definitionName,
            resultId.replace(/\..*/, '')
        );

        if (result.status != "complete") {
            return null;
        }

        const fullFilename = resolve(persistence.getResultCsvFilename(
            definitionName,
            resultId
        ));

        const doc = { type: "text/csv", filename: fullFilename };

        if (stream) {
            let s = createReadStream(fullFilename);
            s.pipe(process.stdout);
            await new Promise((resolve, reject) => {
                let done = false;
                s.on('finish', () => {
                    if (!done) {
                        done = true;
                        resolve();
                    }
                });
                s.on('error', () => {
                    if (!done) {
                        done = true;
                        reject();
                    }
                });
            });
            return;
        }

        open(fullFilename)
            .then(() => {
                return doc;
            });

        return doc;
    }

}


const argv = yargs
    .command("serve", "Listen on STDIN and output to STDOUT")
    .command("definition-list", "Lists all available definition")
    .command(
        "definition",
        "Gets the details of an individual definition",
        {
            n: { alias: "name", demandOption: true, type: "string" },
        }
    )
    .command(
        "request",
        "Start a request for a query",
        {
            n: { alias: "name", demandOption: true, type: "string" },
            t: { alias: "type-of-db", default: "postgres", choices: ["postgres", "mysql"] },
            p: { alias: "param", array: true }
        }
    )
    .command(
        "result",
        "Gets the result preview and full file status",
        {
            i: { alias: "id", demandOption: true, type: "string" },
            n: { alias: "name", demandOption: true, type: "string" },
        }
    )
    .command(
        "request-status",
        "Start a request for a query",
        {
            i: { alias: "id", demandOption: true, type: "string" },
            n: { alias: "name", demandOption: true, type: "string" },
        }
    )
    .command(
        "process",
        "Process a particular request id",
        {
            i: { alias: "id", demandOption: true, type: "string" },
            n: { alias: "name", demandOption: true, type: "string" },
        }
    )
    .command(
        "demand",
        "Start a request for a query",
        {
            n: { alias: "name", demandOption: true, type: "string" },
            t: { alias: "type-of-db", default: "postgres", choices: ["postgres", "mysql"] },
            p: { alias: "param", array: true }
        }
    )
    .command("queue", "Lists all requests that still need processing")
    .check((args: yargs.Arguments) => {
        const validCommands = ["serve", "definition-list", "definition", "request", "queue", "process", "request-status", "result", "demand"];
        if (validCommands.indexOf(getSubCommand(args)) > -1) {
            return true;
        }
        throw new Error("Not a valid sub command");
    })
    .argv;


if (getSubCommand(argv) === "serve") {

    const rl = createInterface({
        input: process.stdin,
        crlfDelay: Infinity
    });

    const databaseType = argv["type-of-db"] !== "mysql" ?
        DatabaseType.PostgreSQL :
        DatabaseType.MySQL;
    const cmdRunner = new CmdRunner(databaseType);

    function getSpawnSpec(method: string, path: string, data: { [k: string]: string | number }): null | { route: string, func: () => Promise<any>} {

        function getMatch(method: string, path: string): null | [string, RegExpMatchArray] {
            let patterns = new Map([
                [ 'GET: definition-list', ['GET', /^\/definition$/] ],
                [ 'GET: definition', ['GET', /^\/definition\/(_?[a-z][a-z0-9_]{0,99})$/] ],
                [ 'POST: request', ['POST', /^\/request\/(_?[a-z][a-z0-9_]{0,99})$/] ],
                [ 'POST: demand', ['POST', /^\/demand\/(_?[a-z][a-z0-9_]{0,99})$/] ],
                [ 'GET: request-status', ['GET', /^\/request\/(_?[a-z][a-z0-9_]{0,99})\/([^ \/]{0,4096})$/] ],
    // if (!("" + s).match(/^[a-zA-Z0-9_]{0,99}(\.((csv)|(json)))?$/)) {
                [ 'GET: download', ['GET', /^\/result\/(_?[a-z][a-z0-9_]{0,99})\/([a-zA-Z0-9_]{0,99}(\.((csv)|(json))))$/] ],
                [ 'GET: result', ['GET', /^\/result\/(_?[a-z][a-z0-9_]{0,99})\/([^ \/]{0,4096})$/] ],
            ]);

            for (let [patName, [patMethod, patPattern]] of patterns) {
                if (method !== patMethod) { continue; }
                let m = path.match(patPattern);
                if (m) {
                    return [patName, m];
                }
            }
            return null;
        }

        const temp = getMatch(method, path);
        if (temp === null) { return null; }
        const [route, matchData] = temp;

        switch (route) {
            case 'GET: definition-list':
                return { route, func: cmdRunner.definitionList };
            case 'GET: definition':
                return {
                    route,
                    func: cmdRunner.definition.bind(
                        cmdRunner,
                        decodeURIComponent(matchData[1])
                    )
                };
            case 'POST: request':
                return {
                    route,
                    func: cmdRunner.request.bind(
                        cmdRunner,
                        decodeURIComponent(matchData[1]),
                        (data as EsqlateRequestCreation).arguments,
                        true
                    )
                };
            case 'POST: demand':
                return {
                    route,
                    func: cmdRunner.demand.bind(
                        cmdRunner,
                        decodeURIComponent(matchData[1]),
                        (data as EsqlateRequestCreation).arguments
                    )
                };
            case 'GET: request-status':
                return {
                    route,
                    func: cmdRunner.requestStatus.bind(
                        cmdRunner,
                        decodeURIComponent(matchData[1]),
                        decodeURIComponent(matchData[2])
                    )
                };
            case 'GET: result':
                return {
                    route,
                    func: cmdRunner.result.bind(
                        cmdRunner,
                        decodeURIComponent(matchData[1]),
                        decodeURIComponent(matchData[2])
                    )
                };
            case 'GET: download':
                return {
                    route,
                    func: cmdRunner.download.bind(
                        cmdRunner,
                        decodeURIComponent(matchData[1]),
                        decodeURIComponent(matchData[2])
                    )
                };
        }

        return null;
    }

    rl.on('line', async (line) => {
        const m = line.match(/^REQUEST\: *([0-9]+)\: *([A-Z]+)\: *([^\ ]+) (.*)/);
        if (!m) {
            process.stderr.write("ERROR: Line not recognized: " + line);
            return;
        }

        const id = parseInt(m[1], 10);
        if (isNaN(id)) {
            process.stderr.write("ERROR: Request Id is not valid: " + m[1]);
            return;
        }

        const spawnSpec = getSpawnSpec(m[2], m[3], JSON.parse(m[4]));

        if (spawnSpec === null) {
            let lines = [
                `RESPONSE: ${id}: STATUS: 404`,
                `RESPONSE: ${id}: END`,
            ];
            process.stdout.write(lines.join("\n") + "\n");
            return;
        }

        let lines: string[] = [];
        try {
            lines = [
                `RESPONSE: ${id}: HEADERS: {"Content-Type": "application/json"}`,
                `RESPONSE: ${id}: STATUS: 200`,
                `RESPONSE: ${id}: BODY:${JSON.stringify(await spawnSpec.func()) || ""}`,
                `RESPONSE: ${id}: END`,
            ];
        } catch (e) {
            lines = [
                `RESPONSE: ${id}: HEADERS: {"Content-Type": "application/json"}`,
                `RESPONSE: ${id}: STATUS: 500`,
                `RESPONSE: ${id}: END`,
            ];
            process.stderr.write(`ERROR: ${e.message}\n  LINE: ${line}\n  ROUTE: ${spawnSpec.route}\n\n`);
        }
        process.stdout.write(lines.join("\n") + "\n");
    });
}

if (getSubCommand(argv) === "definition-list") {
    const cmdRunner = new CmdRunner(null);
    output(cmdRunner.definitionList());
}

if (getSubCommand(argv) === "definition") {
    const cmdRunner = new CmdRunner(null);
    output(cmdRunner.definition("" + (argv as any).name));
}

if (getSubCommand(argv) === "request") {
    const databaseType = argv["type-of-db"] !== "mysql" ?
        DatabaseType.PostgreSQL :
        DatabaseType.MySQL;
    const cmdRunner = new CmdRunner(databaseType);
    const args = processCmdlineArgumentsToEsqlateArgument((argv.param || []) as string[]);
    const result = cmdRunner.request("" + (argv as any).name, args, false);
    output(result);
}


if (getSubCommand(argv) == "demand") {

    const databaseType = argv["type-of-db"] !== "mysql" ?
        DatabaseType.PostgreSQL :
        DatabaseType.MySQL;
    const cmdRunner = new CmdRunner(databaseType);
    const args = processCmdlineArgumentsToEsqlateArgument((argv.param || []) as string[]);
    const result = cmdRunner.demand("" + (argv as any).name, args);
    output(result);

}


if (getSubCommand(argv) == "queue") {

    const deps = { logger };
    const conf = {
        loggerLevel: Level.WARN,
        knownDefinitions: [],
        definitionDirectory: DEFINITION_DIRECTORY,
    };

    const queue = readDefinitionList(deps, conf)
        .then((definitionsListItems) => definitionsListItems.map(({name}) => name))
        .then((definitionNames) => persistence.getQueue(definitionNames));

    output(queue);

}

// const queue: EsqlateQueue<QueueItem, ResultCreated>;

if (getSubCommand(argv) == "process") {

    const databaseType = (process.env.hasOwnProperty("DATABASE_TYPE") &&
        (("" + process.env.DATABASE_TYPE).toLowerCase() === "mysql")) ?
            DatabaseType.MySQL :
            DatabaseType.PostgreSQL;

    const databaseParallelism = parseInt(process.env.DATABASE_PARALLELISM || "5", 10) || 5;

    const requestId = (argv as any).id;
    const definitionName = "" + (argv as any).name;

    async function doIt() {
        const cmdRunner = new CmdRunner(databaseType);
        return await cmdRunner.process(await cmdRunner.getQueryRunner(), definitionName, requestId);
    }

    try {
        output(doIt());
    } catch (e) {
        logger(Level.WARN, "QUEUE", e.message);
    }

}

if (getSubCommand(argv) === "request-status") {
    const requestId = (argv as any).id;
    const definitionName = "" + (argv as any).name;
    const cmdRunner = new CmdRunner(null);
    const result = cmdRunner.requestStatus(definitionName, requestId);
    output(result);
}


if (getSubCommand(argv) === "result") {
    const resultId = (argv as any).id;
    const definitionName = "" + (argv as any).name;
    const cmdRunner = new CmdRunner(null);
    let result;
    if (resultId.match(/\.((csv)|(json))$/)) {
        result = cmdRunner.download(definitionName, resultId, true);
    } else {
        result = cmdRunner.result(definitionName, resultId);
    }
    output(result);
}
