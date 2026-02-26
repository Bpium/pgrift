"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.runMigration = runMigration;
const node_fs_1 = __importDefault(require("node:fs"));
const readline = __importStar(require("node:readline"));
const config_1 = require("./config");
const db_1 = require("./db");
const migrate_tenant_1 = require("./migrate-tenant");
const state_1 = require("./state");
const utils_1 = require("./utils");
const verify_migration_1 = require("./verify-migration");
async function runBatch(tenants, state) {
    const results = await Promise.allSettled(tenants.map(async (db) => {
        await (0, migrate_tenant_1.migrateTenant)(db);
        const { ok, reasons } = await (0, verify_migration_1.verifyMigration)(db);
        if (!ok) {
            throw new Error(`verification failed: ${reasons.join(" | ")}`);
        }
        return db;
    }));
    for (let i = 0; i < results.length; i++) {
        const db = tenants[i];
        const result = results[i];
        if (result.status === "fulfilled") {
            state.completed.push(db);
            (0, utils_1.log)("done", `${db} (${state.completed.length} total)`);
        }
        else {
            const message = result.reason instanceof Error ? result.reason.message : String(result.reason);
            const existing = state.failed.find((f) => f.db === db);
            if (existing) {
                existing.attempts++;
                existing.error = message;
            }
            else {
                state.failed.push({ db, error: message, attempts: 1 });
            }
            (0, utils_1.log)("fail", `${db}: ${message.slice(0, 200)}`);
        }
    }
}
function askConfirm(prompt) {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    return new Promise((resolve) => {
        rl.question(prompt, (answer) => {
            rl.close();
            resolve(answer.trim().toUpperCase() === "Y");
        });
    });
}
async function runMigration() {
    const confirmed = await askConfirm("Run migration? (Y/N): ");
    if (!confirmed) {
        (0, utils_1.log)("info", "Aborted.");
        return;
    }
    node_fs_1.default.mkdirSync(config_1.CONFIG.dumpDir, { recursive: true });
    await (0, db_1.ensureTargetDatabase)();
    const state = (0, state_1.loadState)();
    const allTenants = await (0, db_1.getTenants)();
    const completed = new Set(state.completed);
    const remaining = allTenants.filter((db) => !completed.has(db));
    (0, utils_1.log)("info", `total: ${allTenants.length} | done: ${state.completed.length} | ` +
        `remaining: ${remaining.length} | failed: ${state.failed.length}`);
    if (remaining.length === 0) {
        (0, utils_1.log)("info", "nothing to migrate");
        return;
    }
    const batches = [];
    for (let i = 0; i < remaining.length; i += config_1.CONFIG.concurrency) {
        batches.push(remaining.slice(i, i + config_1.CONFIG.concurrency));
    }
    const startTime = Date.now();
    const initialDone = state.completed.length;
    for (let i = 0; i < batches.length; i++) {
        const elapsed = Math.round((Date.now() - startTime) / 1000);
        const migratedSoFar = state.completed.length - initialDone;
        const pct = Math.round((state.completed.length / allTenants.length) * 100);
        const remainingCount = remaining.length - migratedSoFar;
        const eta = migratedSoFar > 0 ? Math.round((elapsed / migratedSoFar) * remainingCount) : "?";
        (0, utils_1.log)("info", `batch ${i + 1}/${batches.length} | ${pct}% | ${elapsed}s elapsed | ETA ~${eta}s`);
        await runBatch(batches[i], state);
        (0, state_1.saveState)(state);
    }
    const totalTime = Math.round((Date.now() - startTime) / 1000);
    (0, utils_1.log)("info", `completed: ${state.completed.length} | failed: ${state.failed.length} | time: ${totalTime}s`);
    if (state.failed.length > 0) {
        (0, utils_1.log)("warn", "failed tenants:");
        for (const f of state.failed)
            (0, utils_1.log)("fail", `  ${f.db} (${f.attempts} attempts): ${f.error.slice(0, 200)}`);
    }
    (0, utils_1.atomicWrite)("./migration-report.json", JSON.stringify({
        ...state,
        totalDatabases: allTenants.length,
        totalTimeSeconds: totalTime,
    }, null, 2));
}
