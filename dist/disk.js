"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getFreeBytesOnDir = getFreeBytesOnDir;
exports.assertDiskSpace = assertDiskSpace;
const node_child_process_1 = require("node:child_process");
function getFreeBytesOnDir(dir) {
    const out = (0, node_child_process_1.execSync)(`df -k "${dir}"`, { encoding: "utf-8" });
    const line = out.trim().split("\n")[1];
    const available = parseInt(line.trim().split(/\s+/)[3], 10);
    return available * 1024;
}
function assertDiskSpace(dir, minBytes = 512 * 1024 * 1024) {
    const free = getFreeBytesOnDir(dir);
    if (free < minBytes) {
        throw new Error(`Not enough disk space in ${dir}: ${Math.round(free / 1024 / 1024)} MB free, need at least ${Math.round(minBytes / 1024 / 1024)} MB`);
    }
}
