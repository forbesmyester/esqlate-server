import { ErrorRequestHandler, NextFunction, Request, RequestHandler, Response  } from "express";
import { Level, Logger } from "./logger";

export interface NextWrapDependencies {
    logger: Logger;
    setTimeout: (f: (...args: any) => void, n: number) => number;
    clearTimeout: (n: number) => void;
}

export default function nextWrap(deps: NextWrapDependencies, ttl: number, f: RequestHandler): RequestHandler {
    return (req: Request, res: Response, next: NextFunction) => {
        const t = deps.setTimeout(() => {
            deps.logger(
                Level.WARN,
                "REQTIME",
                `Request to ${req.method}:${req.path} using ${f.name}(): Did not respond within ${ttl} seconds`,
            );
        }, ttl * 1000);
        f(req, res, (err?: Error) => {
            deps.clearTimeout(t);
            if (err !== undefined) {
                // if (!res.finished) {
                //     res.status(500).end();
                // }
                return next(err);
            }
            next();
        });
    };
}
