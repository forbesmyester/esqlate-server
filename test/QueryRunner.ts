import test from 'tape';
import { EsqlateRequestCreationParameter, EsqlateResult, EsqlateStatementNormalized } from "esqlate-lib";
import { format, getQuery, pgQuery, PgQuery } from '../src/QueryRunner';
import { EsqlateErrorSqlExecution } from "../src/logger";
import { QueryResult } from "pg";

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
        { name: "amount", variable_type: "integer" },
        ` AND sales_rep = `,
        { name: "user_id", variable_type: "server" },
    ];
    const sp: EsqlateRequestCreationParameter = [{"field_name": "user_id", "field_value": "4fs6a3"}];
    const p: EsqlateRequestCreationParameter = [{"field_name": "amount", "field_value": 4}];
    const r = getQuery(ns, sp, p);

    const expectedText: PgQuery["text"] =
        `
        SELECT
            id,
            CONCAT('id', ': ', 'first_name', ' ', 'last_name') AS disp
        FROM customers
        INNER JOIN credit ON credit.customer_id = customer.id
            WHERE credit.amount >= $1 AND sales_rep = $2`;

    const expectedValues: PgQuery["values"] = [4, "4fs6a3"];
    assert.deepEqual(
        { text: expectedText, values: expectedValues },
        r,
    );
    assert.end();
});

test("format", (assert) => {

    let dtitnCallCount = 0

    function dataTypeIDToName(dataTypeID: number) {
        dtitnCallCount++;
        switch (dataTypeID) {
            case 1043:
                return "varchar";
            case 1184:
                return "timestamptz";
        }
        throw new Error(`Unexpected dataTypeID ${dataTypeID}`);
    }

    const sqlResult: Promise<QueryResult> = Promise.resolve({
        rows: [
            { birth: "2019-09-12T17:15:07.237Z", name: "Joe" },
            { birth: "2019-08-21T02:33:5l.426Z", name: "Jack" },
            { name: "Jane", birth: "2018-09-13T12:11:28.352Z" },
        ],
        command: "SELECT",
        rowCount: 2,
        oid: -1,
        fields: [
            {
                name: "name",
                tableID: 0,
                columnID: 0,
                dataTypeID: 1043,
                dataTypeSize: -1,
                dataTypeModifier: -1,
                format: "text",
            },
            {
                name: "birth",
                tableID: 0,
                columnID: 0,
                dataTypeID: 1184,
                dataTypeSize: 8,
                dataTypeModifier: -1,
                format: "text",
            },
        ]
    });

    const expected: EsqlateResult = {
        fields: [
            { "field_name": "name", "field_type": "varchar" },
            { "field_name": "birth", "field_type": "timestamptz" },
        ],
        rows: [
            [ "Joe", "2019-09-12T17:15:07.237Z" ],
            [ "Jack", "2019-08-21T02:33:5l.426Z" ],
            [ "Jane", "2018-09-13T12:11:28.352Z" ],
        ],
        status: "complete"
    }

    assert.plan(2);
    format(dataTypeIDToName, sqlResult)
        .then((result) => {
            assert.deepEqual(result, expected);
            assert.is(dtitnCallCount, 2);
            assert.end();
        })
        .catch(() => assert.fail());

});

test("format error", (assert) => {

    function dataTypeIDToName(_dataTypeID: number) {
        return "";
    }

    class MyBadPsqlError extends Error {
        public severity: string;
        public code: string;
        public position: string;
        constructor(msg: string, s: string, c: string, p: string) {
            super(msg);
            this.severity = s;
            this.code = c;
            this.position = p;
        }
    }

    const sqlResult: Promise<QueryResult> = Promise.reject(
        new MyBadPsqlError(
            "Went wrong",
             "Major",
             "362",
             "13",
        )
    );

    assert.plan(1);
    format(dataTypeIDToName, sqlResult)
        .then((result) => {
            const ed = { severity: "Major", code: "362", position: "13" };
            const msg = "Went wrong - debugging information: " + JSON.stringify(ed);
            assert.deepEqual(
                result,
                {
                    status: "error",
                    message: msg
                }
            );
            assert.end();
        })
        .catch(() => {
            assert.fail();
        });

});


test('pgQuery', (assert) => {

    const input: EsqlateStatementNormalized = [
            "insert into orders (customer_id, ref, total_credit, available_credit, sales_rep)\n  values (",
            {
                name: "customer_id",
                variable_type: "select",
                statement: "select id, concat('id', ': ', name) as disp from customers",
                display_field: "disp",
                value_field: "id"
            },
            ", 'big $ person', ",
            { name: "credit", variable_type: "integer" },
            ", ",
            { name: "credit", variable_type: "integer" },
            ", ",
            { name: "sales_rep", variable_type: "server" },
            ")"
        ];

    const expected = {
        text: "insert into orders (customer_id, ref, total_credit, available_credit, sales_rep)\n  values ($1, 'big $$ person', $2, $2, $3)",
        values: [29, 5, 3]
    };


    assert.deepEqual(
        pgQuery(input, { customer_id: 29, credit: 5, sales_rep: 3 }),
        expected
    );

    assert.end();

});
