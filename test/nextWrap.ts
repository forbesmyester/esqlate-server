import test from 'tape';
import { Request, Response, NextFunction } from 'express';
import { NextWrapDependencies } from '../src/nextWrap';
import nextWrap from '../src/nextWrap';

test('nextwrap', (assert) => {
    let req: Partial<Request> = { path: '/hi', method: 'POST', body: 'req' };
    let res: Partial<Response> = { charset: 'res' };

    const deps: NextWrapDependencies = {
        logger: (lvl, comp, msg) => {
            assert.is("WARN", lvl);
            assert.is("REQTIME", comp);
            assert.is("Request to POST:/hi using bob(): Did not respond within 1234 seconds", msg);
        },
        setTimeout: (f: Function, n: number) => {
            assert.is(n, 1234000);
            f();
            return 2345;
        },
        clearTimeout: (n: number) => {
            assert.is(n, 2345);
        }
    };
    let handler = nextWrap(
        deps,
        1234,
        function bob(req: Request, res: Response, next: NextFunction) {
            assert.is(req.body, 'req');
            assert.is(res.charset, 'res');
            next();
        }
    );

    assert.plan(8);
    handler(<Request>req, <Response>res, (err?: Error) => {
        assert.is(undefined, err);
    });
    assert.end();
});

