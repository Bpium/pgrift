import fs from "node:fs";
import path from "node:path";
import type { ClientConfig } from "pg";
import { CONFIG } from "./config";
import { withClient } from "./db";
import { assertDiskSpace } from "./disk";
import { exec, log } from "./utils";

// ---------------------------------------------------------------------------
// Connection flag helpers
// ---------------------------------------------------------------------------

/** Returns -h/-p/-U/-d flags for pg_dump / psql. Safe on all platforms (no & in shell). */
function pgFlags(cfg: ClientConfig, database?: string): string {
  const db = database ?? (cfg.database as string);
  return [`-h "${cfg.host}"`, `-p ${cfg.port}`, `-U "${cfg.user}"`, `-d "${db}"`].join(" ");
}

// ---------------------------------------------------------------------------
// Dump-file rewrite helpers  (strategy: "rewrite")
// ---------------------------------------------------------------------------

/**
 * Rewrites a pg_dump SQL file: replaces all references to the "public" schema
 * with the target schema name (dbName).
 *
 * Safely skips COPY data blocks to avoid corrupting row data.
 */
function rewriteSchemaInDump(sql: string, dbNameEsc: string): string {
  const lines = sql.split("\n");
  const out: string[] = [];
  let inCopy = false;

  for (const line of lines) {
    // Detect start of COPY data block
    if (!inCopy && line.startsWith("COPY ") && line.includes(" FROM stdin;")) {
      // Rewrite the COPY statement itself (contains schema prefix)
      out.push(rewriteLine(line, dbNameEsc));
      inCopy = true;
      continue;
    }

    // Detect end of COPY data block
    if (inCopy) {
      out.push(line); // raw data — don't touch
      if (line === "\\.") {
        inCopy = false;
      }
      continue;
    }

    // Regular SQL line — apply replacements
    out.push(rewriteLine(line, dbNameEsc));
  }

  return out.join("\n");
}

/** Escapes a string for safe use inside a RegExp pattern. */
function escapeRegex(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Apply schema-name replacements to a single SQL line. */
function rewriteLine(line: string, dbNameEsc: string): string {
  let result = line;

  // CREATE SCHEMA public; → CREATE SCHEMA "dbName";
  result = result.replace(/\bCREATE SCHEMA public\b/g, `CREATE SCHEMA "${dbNameEsc}"`);

  // ALTER SCHEMA public → ALTER SCHEMA "dbName" (covers OWNER TO, etc.)
  result = result.replace(/\bALTER SCHEMA public\b/g, `ALTER SCHEMA "${dbNameEsc}"`);

  // COMMENT ON SCHEMA public → COMMENT ON SCHEMA "dbName"
  result = result.replace(/\bCOMMENT ON SCHEMA public\b/g, `COMMENT ON SCHEMA "${dbNameEsc}"`);

  // SET search_path = public, pg_catalog; → SET search_path = "dbName", pg_catalog;
  result = result.replace(/search_path = public,/g, `search_path = "${dbNameEsc}",`);

  // Schema-qualified names: public.tablename → "dbName".tablename
  result = result.replace(/\bpublic\./g, `"${dbNameEsc}".`);

  // Fix opclass references that should stay in public schema
  // e.g. "dbName".gin_trgm_ops → public.gin_trgm_ops
  result = result.replace(
    new RegExp(`"${escapeRegex(dbNameEsc)}"\\.gin_trgm_ops`, "g"),
    "public.gin_trgm_ops",
  );

  return result;
}

// ---------------------------------------------------------------------------
// Source read-only helper
// ---------------------------------------------------------------------------

/**
 * Sets the source database to read-only after the dump is complete.
 * This prevents new writes from landing in the old DB while the
 * tenant is being switched to the new schema-based location.
 *
 * Uses try/catch so a permission error (e.g. managed PG restrictions)
 * is logged as a warning and never blocks the migration.
 */
async function setSourceReadonly(src: ClientConfig, dbName: string, dbNameEsc: string): Promise<void> {
  try {
    await withClient({ ...src, database: dbName }, async (client) => {
      await client.query(`ALTER DATABASE "${dbNameEsc}" SET default_transaction_read_only = true`);
    });
    log("info", `  [${dbName}] source database set to read-only`);
  } catch (err) {
    log("warn", `  [${dbName}] could not set source to read-only (skipped): ${err instanceof Error ? err.message : String(err)}`);
  }
}

// ---------------------------------------------------------------------------
// Dump strategies
// ---------------------------------------------------------------------------

/**
 * Strategy "rewrite" (default):
 * Dumps public schema as-is, then rewrites all schema references in the dump file.
 * Does NOT modify the source database — works on managed platforms without schema ownership.
 */
async function dumpWithRewrite(
  dbName: string,
  dbNameEsc: string,
  src: ClientConfig,
  srcPw: string,
  finalDumpFile: string,
): Promise<void> {
  // Set source to read-only BEFORE dumping so no new writes can sneak in
  // between the snapshot and the verification step
  if (CONFIG.sourceReadonly) {
    await setSourceReadonly(src, dbName, dbNameEsc);
  }

  log("info", `  [${dbName}] dumping public schema...`);
  exec(
    ["pg_dump", pgFlags(src, dbName), `-n public`, `--no-owner`, `--no-acl`, `-f "${finalDumpFile}"`].join(" "),
    srcPw,
  );

  log("info", `  [${dbName}] rewriting schema references in dump...`);
  const rawDump = fs.readFileSync(finalDumpFile, "utf-8");
  const rewritten = rewriteSchemaInDump(rawDump, dbNameEsc);
  fs.writeFileSync(finalDumpFile, rewritten, "utf-8");
}

/**
 * Strategy "rename":
 * Renames public schema in source → dumps → rolls back.
 * Requires the DB user to be the owner of the public schema.
 */
async function dumpWithRename(
  dbName: string,
  dbNameEsc: string,
  src: ClientConfig,
  srcPw: string,
  finalDumpFile: string,
): Promise<void> {
  let rollbackNeeded = false;

  try {
    log("info", `  [${dbName}] renaming schema in source...`);
    await withClient({ ...src, database: dbName }, async (client) => {
      await client.query(`ALTER SCHEMA public RENAME TO "${dbNameEsc}"`);
      await client.query(`CREATE SCHEMA public`);
      await client.query(`ALTER DATABASE "${dbNameEsc}" SET search_path = '"${dbNameEsc}"'`);
    });
    rollbackNeeded = true;

    // Set source to read-only BEFORE dumping so no new writes can sneak in
    if (CONFIG.sourceReadonly) {
      await setSourceReadonly(src, dbName, dbNameEsc);
    }

    log("info", `  [${dbName}] dumping renamed schema...`);
    exec(
      [
        "pg_dump",
        pgFlags(src, dbName),
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

    // Fix opclass references in dump
    let dumpSql = fs.readFileSync(finalDumpFile, "utf-8");
    const opclassSchema = `${dbName}.gin_trgm_ops`;
    if (dumpSql.includes(opclassSchema)) {
      dumpSql = dumpSql.split(opclassSchema).join("public.gin_trgm_ops");
    }
    fs.writeFileSync(finalDumpFile, dumpSql, "utf-8");
  } catch (err) {
    if (rollbackNeeded) {
      log("warn", `  [${dbName}] rollback after error...`);
      try {
        await withClient({ ...src, database: dbName }, async (client) => {
          await client.query(`ALTER DATABASE "${dbNameEsc}" RESET search_path`);
          await client.query(`DROP SCHEMA IF EXISTS public`);
          await client.query(`ALTER SCHEMA "${dbNameEsc}" RENAME TO public`);
        });
      } catch (rbErr) {
        log("error", `  [${dbName}] rollback failed: ${rbErr}`);
      }
    }
    throw err;
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

export async function migrateTenant(dbName: string, sourceOverride?: ClientConfig): Promise<void> {
  const finalDumpFile = path.join(CONFIG.dumpDir, `${dbName}.final.dump`);

  const src = sourceOverride ?? CONFIG.source;
  const tgt = CONFIG.target;

  const srcPw = String(src.password ?? "");
  const tgtPw = String(tgt.password ?? "");
  const dbNameEsc = dbName.replace(/"/g, '""');

  assertDiskSpace(CONFIG.dumpDir);

  // Collect extensions from source DB
  const extensions = await withClient({ ...src, database: dbName }, async (client) => {
    const { rows } = await client.query<{ extname: string }>(
      `SELECT extname FROM pg_extension WHERE extname != 'plpgsql' ORDER BY extname`,
    );
    return rows.map((r) => r.extname);
  });

  try {
    // 1. Terminate connections for a consistent dump
    log("info", `  [${dbName}] terminating connections...`);
    await withClient({ ...src, database: dbName }, async (client) => {
      await client.query(
        `SELECT pg_terminate_backend(pid)
         FROM pg_stat_activity
         WHERE datname = $1 AND pid <> pg_backend_pid()`,
        [dbName],
      );
    });

    // 2. Dump + schema rename (strategy-dependent)
    if (CONFIG.schemaRenameStrategy === "rename") {
      await dumpWithRename(dbName, dbNameEsc, src, srcPw, finalDumpFile);
    } else {
      await dumpWithRewrite(dbName, dbNameEsc, src, srcPw, finalDumpFile);
    }

    // 3. Prepare target DB: drop old schema (if exists), ensure extensions
    log("info", `  [${dbName}] restoring to target database ${tgt.database}...`);
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

    // 4. Restore dump into target
    exec(
      ["psql", pgFlags(tgt), `-v ON_ERROR_STOP=1`, `-f "${finalDumpFile}"`].join(" "),
      tgtPw,
    );

    log("info", `  [${dbName}] migration completed`);
  } finally {
    // No rollback for "rewrite" strategy — source DB was never modified.
    // Rollback for "rename" strategy is handled inside dumpWithRename.
    if (fs.existsSync(finalDumpFile)) fs.unlinkSync(finalDumpFile);
  }
}
