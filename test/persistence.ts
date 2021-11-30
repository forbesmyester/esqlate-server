import test from 'tape';
import { FilesystemPersistence } from "../src/persistence";

class ErrorWithCode extends Error {
    public code: string;
    constructor(message: string, code: string) {
        super(message);
        this.code = code;
    }
}

test("FilesystemPersistence.getQueueItems()", async (assert) => {

    let enoentRaised = false;
    let enotdirRaised = false;

    class TestFilesystemPersistence extends FilesystemPersistence {
        readdir(p: string): Promise<string[]> {

            const answers: {[k: string]: string[]} = {
                "persistence_for_unit_tests": [
                    'random_file.txt',
                    'customer_country_count',
                    'customer_search_by_name_only',
                ],
                "persistence_for_unit_tests/customer_country_count": ["oK", "oB"],
                "persistence_for_unit_tests/customer_search_by_name_only": ["za", "ab"],
                "persistence_for_unit_tests/customer_country_count/oK": ["123"],
                "persistence_for_unit_tests/customer_search_by_name_only/ab": ["234", "345"],
                "persistence_for_unit_tests/customer_search_by_name_only/za": [],
                "persistence_for_unit_tests/customer_search_by_name_only/zb": ["README.md"],
                "persistence_for_unit_tests/customer_country_count/oK/123": ["request.json"],
                "persistence_for_unit_tests/customer_search_by_name_only/ab/234": [
                    "request.json",
                    "AAAA-result.json",
                    "AAAA-result.csv"
                ],
                "persistence_for_unit_tests/customer_search_by_name_only/ab/345": [
                    "request.json",
                    "BBBB-result.json",
                    "CCCC-result.csv"
                ],
            }

            let answer: string[] | undefined = answers[p];
            if (typeof answer == 'undefined') {
                enoentRaised = true;
                return Promise.reject(new ErrorWithCode(`Not Found (${p})`, "ENOENT"));
            }

            if (answer.length == 0) {
                enotdirRaised = true;
                return Promise.reject(new ErrorWithCode(`Not File (${p})`, "ENOTDIR"));
            }
            return Promise.resolve(answer);
        }
    }

    const definitionNames = [
        'customer_country_count',
        'customer_search_by_name_only',
        'order_details',
        'order_list'
    ];

    const persistence = new TestFilesystemPersistence('persistence_for_unit_tests');
    const items = await persistence.getQueue(definitionNames);

    assert.is(enotdirRaised, true);
    assert.is(enotdirRaised, true);
    assert.deepEqual(items, ["oK123", "ab345"]);


    assert.end();
});

