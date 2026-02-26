/**
 * pgrift – PostgreSQL multi-tenant migration (separate DBs → single DB with schemas)
 *
 * Entry points:
 * - migrate.ts  → runMigration() from ./src/runner
 * - verify.ts   → uses getTenants from ./src/db, CONFIG from ./src/config
 * - cleanup.ts  → uses CONFIG from ./src/config
 */

export { buildConnectionString, CONFIG } from "./config";
export { ensureTargetDatabase, getTenants, withClient } from "./db";
export { migrateTenant } from "./migrate-tenant";
export { runMigration } from "./runner";
export { loadState, saveState } from "./state";
export { FailedEntry, State } from "./types";
export { atomicWrite, exec, log } from "./utils";
export { tableChecksum, verifyMigration } from "./verify-migration";
