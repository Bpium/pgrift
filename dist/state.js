"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.loadState = loadState;
exports.saveState = saveState;
const node_fs_1 = __importDefault(require("node:fs"));
const config_1 = require("./config");
const utils_1 = require("./utils");
function loadState() {
    if (node_fs_1.default.existsSync(config_1.CONFIG.stateFile)) {
        try {
            return JSON.parse(node_fs_1.default.readFileSync(config_1.CONFIG.stateFile, "utf-8"));
        }
        catch {
            (0, utils_1.log)("warn", "state file is corrupted, starting fresh");
        }
    }
    return {
        completed: [],
        failed: [],
        startedAt: new Date().toISOString(),
        lastUpdated: new Date().toISOString(),
    };
}
function saveState(state) {
    state.lastUpdated = new Date().toISOString();
    (0, utils_1.atomicWrite)(config_1.CONFIG.stateFile, JSON.stringify(state, null, 2));
}
