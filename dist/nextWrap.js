"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const logger_1 = require("./logger");
function nextWrap(deps, ttl, f) {
    return (req, res, next) => {
        const t = deps.setTimeout(() => {
            deps.logger(logger_1.Level.WARN, "REQTIME", `Request to ${req.method}:${req.path} using ${f.name}(): Did not respond within ${ttl} seconds`);
        }, ttl * 1000);
        f(req, res, (err) => {
            deps.clearTimeout(t);
            if (err !== undefined) {
                return next(err);
            }
            next();
        });
    };
}
exports.default = nextWrap;
