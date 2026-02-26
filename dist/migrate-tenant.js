"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.migrateTenant = migrateTenant;
const node_fs_1 = __importDefault(require("node:fs"));
const node_path_1 = __importDefault(require("node:path"));
const config_1 = require("./config");
const db_1 = require("./db");
const disk_1 = require("./disk");
const utils_1 = require("./utils");
async function migrateTenant(dbName) {
    const finalDumpFile = node_path_1.default.join(config_1.CONFIG.dumpDir, `${dbName}.final.dump`);
    const src = config_1.CONFIG.source;
    const tgt = config_1.CONFIG.target;
    const srcPw = String(src.password ?? "");
    const tgtPw = String(tgt.password ?? "");
    const dbNameEsc = dbName.replace(/"/g, '""');
    (0, disk_1.assertDiskSpace)(config_1.CONFIG.dumpDir);
    const extensions = await (0, db_1.withClient)({ ...src, database: dbName }, async (client) => {
        const { rows } = await client.query(`SELECT extname FROM pg_extension WHERE extname != 'plpgsql' ORDER BY extname`);
        return rows.map((r) => r.extname);
    });
    let rollbackNeeded = false;
    try {
        (0, utils_1.log)("info", `  [${dbName}] terminating connections...`);
        await (0, db_1.withClient)({ ...src, database: "postgres" }, async (client) => {
            await client.query(`SELECT pg_terminate_backend(pid)
         FROM pg_stat_activity
         WHERE datname = $1 AND pid <> pg_backend_pid()`, [dbName]);
        });
        (0, utils_1.log)("info", `  [${dbName}] renaming schema in source...`);
        await (0, db_1.withClient)({ ...src, database: dbName }, async (client) => {
            await client.query(`ALTER SCHEMA public RENAME TO "${dbNameEsc}"`);
            await client.query(`CREATE SCHEMA public`);
            await client.query(`ALTER DATABASE "${dbNameEsc}" SET search_path = '"${dbNameEsc}"'`);
        });
        rollbackNeeded = true;
        (0, utils_1.log)("info", `  [${dbName}] dumping renamed schema...`);
        (0, utils_1.exec)([
            "pg_dump",
            `-h "${src.host}"`,
            `-p ${src.port}`,
            `-U "${src.user}"`,
            `-d "${dbName}"`,
            `-n "${dbName}"`,
            `--no-owner`,
            `--no-acl`,
            `-f "${finalDumpFile}"`,
        ].join(" "), srcPw);
        (0, utils_1.log)("info", `  [${dbName}] rolling back source schema...`);
        await (0, db_1.withClient)({ ...src, database: dbName }, async (client) => {
            await client.query(`ALTER DATABASE "${dbNameEsc}" RESET search_path`);
            await client.query(`DROP SCHEMA public`);
            await client.query(`ALTER SCHEMA "${dbNameEsc}" RENAME TO public`);
        });
        rollbackNeeded = false;
        (0, utils_1.log)("info", `  [${dbName}] restoring to target database...`);
        await (0, db_1.withClient)(tgt, async (client) => {
            await client.query(`DROP SCHEMA IF EXISTS "${dbNameEsc}" CASCADE`);
            for (const extname of extensions) {
                try {
                    await client.query(`CREATE EXTENSION IF NOT EXISTS "${extname}" WITH SCHEMA public`);
                }
                catch {
                    // extension may already exist
                }
            }
        });
        let dumpSql = node_fs_1.default.readFileSync(finalDumpFile, "utf-8");
        const opclassSchema = `${dbName}.gin_trgm_ops`;
        if (dumpSql.includes(opclassSchema)) {
            dumpSql = dumpSql.split(opclassSchema).join("public.gin_trgm_ops");
        }
        node_fs_1.default.writeFileSync(finalDumpFile, dumpSql, "utf-8");
        (0, utils_1.exec)([
            "psql",
            `-h "${tgt.host}"`,
            `-p ${tgt.port}`,
            `-U "${tgt.user}"`,
            `-d "${tgt.database}"`,
            `-v ON_ERROR_STOP=1`,
            `-f "${finalDumpFile}"`,
        ].join(" "), tgtPw);
        (0, utils_1.log)("info", `  [${dbName}] migration completed`);
    }
    finally {
        if (rollbackNeeded) {
            (0, utils_1.log)("warn", `  [${dbName}] rollback after error...`);
            try {
                await (0, db_1.withClient)({ ...src, database: dbName }, async (client) => {
                    await client.query(`ALTER DATABASE "${dbNameEsc}" RESET search_path`);
                    await client.query(`DROP SCHEMA IF EXISTS public`);
                    await client.query(`ALTER SCHEMA "${dbNameEsc}" RENAME TO public`);
                });
            }
            catch (err) {
                (0, utils_1.log)("error", `  [${dbName}] rollback failed: ${err}`);
            }
        }
        if (node_fs_1.default.existsSync(finalDumpFile))
            node_fs_1.default.unlinkSync(finalDumpFile);
    }
}
