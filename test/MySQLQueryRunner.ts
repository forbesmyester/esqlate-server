import test from 'tape';
import { EsqlateArgument, EsqlateResult, EsqlateStatementNormalized } from "esqlate-lib";
import { getQuery } from '../src/MySQLQueryRunner';

test('getQuery', (assert) => {
    assert.plan(1);
    const ns: EsqlateStatementNormalized = [
        `
        SELECT
            id,
            CONCAT('id', ': ', 'first_name', ' ', 'last_name') AS disp
        FROM customers
        INNER JOIN credit ON credit.customer_id = customer.id
            WHERE credit.amount >= `,
        { name: "amount", type: "integer" },
        ` AND credit.amount <= `,
        { name: "amount", type: "integer" },
        ` AND sales_rep = `,
        { name: "user_id", type: "server" },
    ];
    const sp: EsqlateArgument[] = [{ "name": "user_id", "value": "4fs6a3" }];
    const p: EsqlateArgument[] = [{ "name": "amount", "value": 4 }];
    const r = getQuery(ns, sp, p);

    const expectedText =
        `
        SELECT
            id,
            CONCAT('id', ': ', 'first_name', ' ', 'last_name') AS disp
        FROM customers
        INNER JOIN credit ON credit.customer_id = customer.id
            WHERE credit.amount >= ? AND credit.amount <= ? AND sales_rep = ?`;

    const expectedValues = [4, 4, "4fs6a3"];
    assert.deepEqual(
        r,
        { text: expectedText, paramValues: expectedValues },
    );
    assert.end();
});
