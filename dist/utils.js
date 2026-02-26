"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.log = log;
exports.atomicWrite = atomicWrite;
exports.exec = exec;
const node_child_process_1 = require("node:child_process");
const node_fs_1 = __importDefault(require("node:fs"));
const config_1 = require("./config");
function log(level, msg) {
    const prefix = {
        info: "[info]",
        warn: "[warn]",
        error: "[error]",
        done: "[done]",
        fail: "[fail]",
    };
    const out = level === "error" || level === "fail" ? process.stderr : process.stdout;
    out.write(`${prefix[level]} ${msg}\n`);
}
/** Atomic file write: write to temp file then rename. */
function atomicWrite(filePath, data) {
    const tmp = `${filePath}.tmp`;
    node_fs_1.default.writeFileSync(tmp, data, "utf-8");
    node_fs_1.default.renameSync(tmp, filePath);
}
function exec(cmd, password) {
    const opts = {
        env: { ...process.env, PGPASSWORD: password },
        stdio: "pipe",
        timeout: config_1.CONFIG.execTimeoutMs ?? 10 * 60 * 1000,
    };
    (0, node_child_process_1.execSync)(cmd, opts);
}
