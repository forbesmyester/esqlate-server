import test from 'tape';

import { processCmdlineArgumentsToEsqlateArgument } from "../src/functions";

test('processCmdlineArgumentsToEsqlateArgument', (assert) => {

    assert.deepEqual(
        processCmdlineArgumentsToEsqlateArgument(['["a",1]']),
        [
            { name: "a", value: 1 }
        ]
        
    );
    assert.end();

});
