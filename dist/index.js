"use strict";
/**
 * pgrift – PostgreSQL multi-tenant migration (separate DBs → single DB with schemas)
 *
 * Entry points:
 * - migrate.ts  → runMigration() from ./src/runner
 * - verify.ts   → uses getTenants from ./src/db, CONFIG from ./src/config
 * - cleanup.ts  → uses CONFIG from ./src/config
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.verifyMigration = exports.tableChecksum = exports.log = exports.exec = exports.atomicWrite = exports.saveState = exports.loadState = exports.runMigration = exports.migrateTenant = exports.withClient = exports.getTenants = exports.ensureTargetDatabase = exports.CONFIG = exports.buildConnectionString = void 0;
var config_1 = require("./config");
Object.defineProperty(exports, "buildConnectionString", { enumerable: true, get: function () { return config_1.buildConnectionString; } });
Object.defineProperty(exports, "CONFIG", { enumerable: true, get: function () { return config_1.CONFIG; } });
var db_1 = require("./db");
Object.defineProperty(exports, "ensureTargetDatabase", { enumerable: true, get: function () { return db_1.ensureTargetDatabase; } });
Object.defineProperty(exports, "getTenants", { enumerable: true, get: function () { return db_1.getTenants; } });
Object.defineProperty(exports, "withClient", { enumerable: true, get: function () { return db_1.withClient; } });
var migrate_tenant_1 = require("./migrate-tenant");
Object.defineProperty(exports, "migrateTenant", { enumerable: true, get: function () { return migrate_tenant_1.migrateTenant; } });
var runner_1 = require("./runner");
Object.defineProperty(exports, "runMigration", { enumerable: true, get: function () { return runner_1.runMigration; } });
var state_1 = require("./state");
Object.defineProperty(exports, "loadState", { enumerable: true, get: function () { return state_1.loadState; } });
Object.defineProperty(exports, "saveState", { enumerable: true, get: function () { return state_1.saveState; } });
var utils_1 = require("./utils");
Object.defineProperty(exports, "atomicWrite", { enumerable: true, get: function () { return utils_1.atomicWrite; } });
Object.defineProperty(exports, "exec", { enumerable: true, get: function () { return utils_1.exec; } });
Object.defineProperty(exports, "log", { enumerable: true, get: function () { return utils_1.log; } });
var verify_migration_1 = require("./verify-migration");
Object.defineProperty(exports, "tableChecksum", { enumerable: true, get: function () { return verify_migration_1.tableChecksum; } });
Object.defineProperty(exports, "verifyMigration", { enumerable: true, get: function () { return verify_migration_1.verifyMigration; } });
