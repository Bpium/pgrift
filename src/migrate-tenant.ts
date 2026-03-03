import fs from "node:fs";
import path from "node:path";
import type { ClientConfig } from "pg";
import { CONFIG } from "./config";
import { withClient } from "./db";
import { assertDiskSpace } from "./disk";
import { exec, log } from "./utils";

export async function migrateTenant(dbName: string, sourceOverride?: ClientConfig): Promise<void> {
  const finalDumpFile = path.join(CONFIG.dumpDir, `${dbName}.final.dump`);

  const src = sourceOverride ?? CONFIG.source;
  const tgt = CONFIG.target;

  const srcPw = String(src.password ?? "");
  const tgtPw = String(tgt.password ?? "");
  const dbNameEsc = dbName.replace(/"/g, '""');

  assertDiskSpace(CONFIG.dumpDir);

  const extensions = await withClient({ ...src, database: dbName }, async (client) => {
    const { rows } = await client.query<{ extname: string }>(
      `SELECT extname FROM pg_extension WHERE extname != 'plpgsql' ORDER BY extname`,
    );
    return rows.map((r) => r.extname);
  });

  let rollbackNeeded = false;
  try {
    log("info", `  [${dbName}] terminating connections...`);
    await withClient({ ...src, database: "postgres" }, async (client) => {
      await client.query(
        `SELECT pg_terminate_backend(pid)
         FROM pg_stat_activity
         WHERE datname = $1 AND pid <> pg_backend_pid()`,
        [dbName],
      );
    });

    log("info", `  [${dbName}] renaming schema in source...`);
    await withClient({ ...src, database: dbName }, async (client) => {
      await client.query(`ALTER SCHEMA public RENAME TO "${dbNameEsc}"`);
      await client.query(`CREATE SCHEMA public`);
      await client.query(`ALTER DATABASE "${dbNameEsc}" SET search_path = '"${dbNameEsc}"'`);
    });
    rollbackNeeded = true;

    log("info", `  [${dbName}] dumping renamed schema...`);
    exec(
      [
        "pg_dump",
        `-h "${src.host}"`,
        `-p ${src.port}`,
        `-U "${src.user}"`,
        `-d "${dbName}"`,
        `-n "${dbName}"`,
        `--no-owner`,
        `--no-acl`,
        `-f "${finalDumpFile}"`,
      ].join(" "),
      srcPw,
    );

    log("info", `  [${dbName}] rolling back source schema...`);
    await withClient({ ...src, database: dbName }, async (client) => {
      await client.query(`ALTER DATABASE "${dbNameEsc}" RESET search_path`);
      await client.query(`DROP SCHEMA public`);
      await client.query(`ALTER SCHEMA "${dbNameEsc}" RENAME TO public`);
    });
    rollbackNeeded = false;

    log("info", `  [${dbName}] restoring to target database  ${tgt.database}...`);

    await withClient(tgt, async (client) => {
      await client.query(`DROP SCHEMA IF EXISTS "${dbNameEsc}" CASCADE`);

      for (const extname of extensions) {
        try {
          await client.query(`CREATE EXTENSION IF NOT EXISTS "${extname}" WITH SCHEMA public`);
        } catch {
          // extension may already exist
        }
      }
    });

    let dumpSql = fs.readFileSync(finalDumpFile, "utf-8");
    const opclassSchema = `${dbName}.gin_trgm_ops`;

    if (dumpSql.includes(opclassSchema)) {
      dumpSql = dumpSql.split(opclassSchema).join("public.gin_trgm_ops");
    }
    fs.writeFileSync(finalDumpFile, dumpSql, "utf-8");

    exec(
      [
        "psql",
        `-h "${tgt.host}"`,
        `-p ${tgt.port}`,
        `-U "${tgt.user}"`,
        `-d "${tgt.database}"`,
        `-v ON_ERROR_STOP=1`,
        `-f "${finalDumpFile}"`,
      ].join(" "),
      tgtPw,
    );

    log("info", `  [${dbName}] migration completed`);
  } finally {
    if (rollbackNeeded) {
      log("warn", `  [${dbName}] rollback after error...`);
      try {
        await withClient({ ...src, database: dbName }, async (client) => {
          await client.query(`ALTER DATABASE "${dbNameEsc}" RESET search_path`);
          await client.query(`DROP SCHEMA IF EXISTS public`);
          await client.query(`ALTER SCHEMA "${dbNameEsc}" RENAME TO public`);
        });
      } catch (err) {
        log("error", `  [${dbName}] rollback failed: ${err}`);
      }
    }
    if (fs.existsSync(finalDumpFile)) fs.unlinkSync(finalDumpFile);
  }
}
