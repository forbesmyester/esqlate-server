import { yargs } from "yargs";
import { readDefinitionList } from "./functions";
import logger, { Level } from "./logger";

function printJSON(j: any): Promise<void> {
    return new Promise((resolve, reject) => {
        process.stdout.write(JSON.stringify(j) + "\n", (err) => {
            if (err) { return reject(err); }
            resolve();
        });
    });
}

const DEFINITION_DIRECTORY: string = process.env.DEFINITION_DIRECTORY || (__dirname + "/example_definition");
const deps = { logger };
const conf = { loggerLevel: Level.WARN, knownDefinitions: [], definitionDirectory: DEFINITION_DIRECTORY };

readDefinitionList(deps, conf)
    .then((lst) => printJSON(lst))
        .then(() => { process.exit(0); })
        .catch((e) => {
            printJSON({ error: e.message })
                .finally(() => {
                    process.exit(1);
                });
        });
