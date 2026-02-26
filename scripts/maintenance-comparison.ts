import { Client } from "pg";
import pMap from "p-map";
import fs from "fs";

const tenants: string[] = JSON.parse(
  fs.readFileSync("./scripts/db-list.json", "utf8"),
);
const connectionString = `postgres://postgres:postgres@localhost:5434`;
const SCHEMA_DB_NAME = process.env.SCHEMA_DB_NAME || "tenants";
const SCHEMA_PREFIX = process.env.SCHEMA_PREFIX ?? "bench_db";
const MODE = process.env.MODE ?? "dbs"; // "dbs" | "schemas"
const OPS_PER_TENANT = parseInt(process.env.OPS_PER_TENANT ?? "50000", 10);
const LOAD_CONCURRENCY = parseInt(process.env.LOAD_CONCURRENCY ?? "50", 10);
const WAIT_AFTER_LOAD_MS = parseInt(
  process.env.WAIT_AFTER_LOAD_MS ?? "360000",
  10,
);

// ============================================
// TYPES
// ============================================

interface VacuumStatsSummary {
  vacuum_count: number;
  autovacuum_count: number;
  n_dead_tup: number;
}

interface BgwriterStats {
  checkpoints_timed: number;
  checkpoints_req: number;
  checkpoint_write_time: number;
  checkpoint_sync_time: number;
  buffers_checkpoint: number;
  buffers_clean: number;
  buffers_backend: number;
  buffers_alloc: number;
}

// ============================================
// HELPERS
// ============================================

const LINE = "─";
const W = 58;

function section(title: string): void {
  console.log(
    `\n${LINE.repeat(2)} ${title} ${LINE.repeat(
      Math.max(0, W - title.length - 4),
    )}`,
  );
}

function line(key: string, value: string | number): void {
  console.log(`  ${key.padEnd(28)} ${value}`);
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function getExistingDatabases(): Promise<Set<string>> {
  try {
    const client = new Client({
      connectionString: connectionString,
    });
    await client.connect();

    const { rows } = await client.query(`
      SELECT datname 
      FROM pg_database 
      WHERE datname NOT LIKE 'pg_%' 
        AND datname NOT IN ('postgres', 'template0', 'template1')
    `);

    await client.end();

    return new Set(rows.map((r) => r.datname));
  } catch (err) {
    console.error("Failed to get existing databases:", err);
    return new Set();
  }
}

async function getExistingSchemas(): Promise<Set<string>> {
  try {
    const client = new Client({
      connectionString: `${connectionString}/${SCHEMA_DB_NAME}`,
    });
    await client.connect();

    const { rows } = await client.query(`
      SELECT schema_name
      FROM information_schema.schemata
      WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'public')
    `);

    await client.end();

    return new Set(rows.map((r: any) => r.schema_name));
  } catch (err) {
    console.error("Failed to get existing schemas:", err);
    return new Set();
  }
}

// ============================================
// MAINTENANCE METRICS
// ============================================

async function getVacuumStatsForDatabases(
  dbNames: string[],
): Promise<VacuumStatsSummary> {
  if (dbNames.length === 0) {
    return { vacuum_count: 0, autovacuum_count: 0, n_dead_tup: 0 };
  }

  const perDb = await pMap(
    dbNames,
    async (db) => {
      const client = new Client({
        connectionString: `${connectionString}/${db}`,
      });
      await client.connect();

      const { rows } = await client.query<{
        vacuum_count: string | null;
        autovacuum_count: string | null;
        n_dead_tup: string | null;
      }>(
        `
        SELECT 
          COALESCE(sum(vacuum_count), 0)::bigint AS vacuum_count,
          COALESCE(sum(autovacuum_count), 0)::bigint AS autovacuum_count,
          COALESCE(sum(n_dead_tup), 0)::bigint AS n_dead_tup
        FROM pg_stat_all_tables
        WHERE relname = 'users_data'
      `,
      );

      await client.end();

      const r = rows[0];
      return {
        vacuum_count: Number(r?.vacuum_count ?? 0),
        autovacuum_count: Number(r?.autovacuum_count ?? 0),
        n_dead_tup: Number(r?.n_dead_tup ?? 0),
      };
    },
    { concurrency: 20 },
  );

  return perDb.reduce<VacuumStatsSummary>(
    (acc, cur) => ({
      vacuum_count: acc.vacuum_count + cur.vacuum_count,
      autovacuum_count: acc.autovacuum_count + cur.autovacuum_count,
      n_dead_tup: acc.n_dead_tup + cur.n_dead_tup,
    }),
    { vacuum_count: 0, autovacuum_count: 0, n_dead_tup: 0 },
  );
}

async function getVacuumStatsForSchemas(
  schemas: string[],
): Promise<VacuumStatsSummary> {
  if (schemas.length === 0) {
    return { vacuum_count: 0, autovacuum_count: 0, n_dead_tup: 0 };
  }

  const client = new Client({
    connectionString: `${connectionString}/${SCHEMA_DB_NAME}`,
  });
  await client.connect();

  const { rows } = await client.query<{
    vacuum_count: string | null;
    autovacuum_count: string | null;
    n_dead_tup: string | null;
  }>(
    `
    SELECT 
      COALESCE(sum(vacuum_count), 0)::bigint AS vacuum_count,
      COALESCE(sum(autovacuum_count), 0)::bigint AS autovacuum_count,
      COALESCE(sum(n_dead_tup), 0)::bigint AS n_dead_tup
    FROM pg_stat_all_tables
    WHERE schemaname = ANY($1::text[])
      AND relname = 'users_data'
  `,
    [schemas],
  );

  await client.end();

  const r = rows[0];
  return {
    vacuum_count: Number(r?.vacuum_count ?? 0),
    autovacuum_count: Number(r?.autovacuum_count ?? 0),
    n_dead_tup: Number(r?.n_dead_tup ?? 0),
  };
}

async function getBgwriterStats(): Promise<BgwriterStats> {
  const client = new Client({
    connectionString: connectionString,
  });
  await client.connect();

  const { rows } = await client.query<BgwriterStats>(`
    SELECT
      checkpoints_timed,
      checkpoints_req,
      checkpoint_write_time,
      checkpoint_sync_time,
      buffers_checkpoint,
      buffers_clean,
      buffers_backend,
      buffers_alloc
    FROM pg_stat_bgwriter
  `);

  await client.end();

  return rows[0];
}

async function resetBgwriterStats(): Promise<void> {
  const client = new Client({
    connectionString: connectionString,
  });
  await client.connect();
  await client.query(`SELECT pg_stat_reset_shared('bgwriter')`);
  await client.end();
}

async function forceCheckpoint(label: string): Promise<void> {
  const client = new Client({
    connectionString: connectionString,
  });
  await client.connect();
  console.log(`\nForcing CHECKPOINT (${label})...`);
  const start = Date.now();
  await client.query("CHECKPOINT");
  const duration = (Date.now() - start) / 1000;
  line("CHECKPOINT duration (sec)", duration.toFixed(2));
  await client.end();
}

function printVacuumDelta(
  label: string,
  before: VacuumStatsSummary,
  after: VacuumStatsSummary,
): void {
  section(`VACUUM: ${label}`);
  line("Total VACUUM (delta)", after.vacuum_count - before.vacuum_count);
  line(
    "Total autovacuum (delta)",
    after.autovacuum_count - before.autovacuum_count,
  );
  line("Dead tuples (delta)", after.n_dead_tup - before.n_dead_tup);
}

function printCheckpointDelta(
  label: string,
  before: BgwriterStats,
  after: BgwriterStats,
): void {
  section(`CHECKPOINT: ${label}`);
  line(
    "checkpoints_timed (delta)",
    after.checkpoints_timed - before.checkpoints_timed,
  );
  line(
    "checkpoints_req (delta)",
    after.checkpoints_req - before.checkpoints_req,
  );
  line(
    "write_time_ms (delta)",
    after.checkpoint_write_time - before.checkpoint_write_time,
  );
  line(
    "sync_time_ms (delta)",
    after.checkpoint_sync_time - before.checkpoint_sync_time,
  );
  line(
    "buffers_checkpoint (delta)",
    after.buffers_checkpoint - before.buffers_checkpoint,
  );
  line(
    "buffers_backend (delta)",
    after.buffers_backend - before.buffers_backend,
  );
  line("buffers_clean (delta)", after.buffers_clean - before.buffers_clean);
  line("buffers_alloc (delta)", after.buffers_alloc - before.buffers_alloc);
}

// ============================================
// LOAD GENERATOR
// ============================================

async function writeLoadSeparateDB(
  dbName: string,
  opsPerTenant: number,
): Promise<void> {
  const client = new Client({
    connectionString: `${connectionString}/${dbName}`,
  });

  try {
    await client.connect();

    for (let i = 0; i < opsPerTenant; i++) {
      const op = Math.random();

      if (op < 0.34) {
        // INSERT
        const newId = Math.floor(Math.random() * 1_000_000_000) + 1;
        await client.query(
          `INSERT INTO users_data ("dbId", "name") VALUES ($1, $2) ON CONFLICT DO NOTHING`,
          [newId, `maintenance_ins_${Date.now()}`],
        );
      } else if (op < 0.67) {
        // UPDATE
        const randomId = Math.floor(Math.random() * 10000) + 1;
        await client.query(
          `UPDATE users_data SET "name" = $1 WHERE "dbId" = $2`,
          [`maintenance_upd_${Date.now()}`, randomId],
        );
      } else {
        // DELETE
        const randomId = Math.floor(Math.random() * 10000) + 1;
        await client.query(`DELETE FROM users_data WHERE "dbId" = $1`, [
          randomId,
        ]);
      }
    }
  } catch (err) {
    console.error(
      `Write load error for ${dbName}:`,
      err instanceof Error ? err.message : String(err),
    );
  } finally {
    try {
      await client.end();
    } catch {}
  }
}

async function writeLoadSchema(
  schemaName: string,
  opsPerTenant: number,
): Promise<void> {
  const client = new Client({
    connectionString: `${connectionString}/${SCHEMA_DB_NAME}`,
  });

  try {
    await client.connect();

    for (let i = 0; i < opsPerTenant; i++) {
      const op = Math.random();

      if (op < 0.34) {
        // INSERT
        const newId = Math.floor(Math.random() * 1_000_000_000) + 1;
        await client.query(
          `INSERT INTO "${schemaName}".users_data ("dbId", "name") VALUES ($1, $2) ON CONFLICT DO NOTHING`,
          [newId, `maintenance_ins_${Date.now()}`],
        );
      } else if (op < 0.67) {
        // UPDATE
        const randomId = Math.floor(Math.random() * 10000) + 1;
        await client.query(
          `UPDATE "${schemaName}".users_data SET "name" = $1 WHERE "dbId" = $2`,
          [`maintenance_upd_${Date.now()}`, randomId],
        );
      } else {
        // DELETE
        const randomId = Math.floor(Math.random() * 10000) + 1;
        await client.query(
          `DELETE FROM "${schemaName}".users_data WHERE "dbId" = $1`,
          [randomId],
        );
      }
    }
  } catch (err) {
    console.error(
      `Write load error for schema ${schemaName}:`,
      err instanceof Error ? err.message : String(err),
    );
  } finally {
    try {
      await client.end();
    } catch {}
  }
}

// ============================================
// MAIN
// ============================================

async function runDbMode(): Promise<void> {
  const existingDatabases = await getExistingDatabases();
  const validDatabases = tenants.filter((db) => existingDatabases.has(db));

  if (validDatabases.length === 0) {
    console.error(
      "\nNo matching databases from db-list.json were found in cluster.",
    );
    process.exit(1);
  }

  const testDatabases = validDatabases.slice(0, 1000);
  console.log(
    `\nValid databases: ${validDatabases.length}/${tenants.length}. Using: ${testDatabases.length} for the test.`,
  );

  // ---------- BASELINE ----------
  console.log(
    "\n[DB MODE] Collecting baseline maintenance stats (VACUUM + bgwriter)...",
  );
  await resetBgwriterStats();
  const vacuumBefore = await getVacuumStatsForDatabases(testDatabases);
  const bgwriterBefore = await getBgwriterStats();

  // ---------- LOAD ----------
  section("WRITE LOAD on separate DBs");
  console.log(
    `  Running write-heavy workload to generate dead tuples and WAL (opsPerTenant=${OPS_PER_TENANT}, concurrency=${LOAD_CONCURRENCY})...`,
  );
  const loadStart = Date.now();
  await pMap(testDatabases, (db) => writeLoadSeparateDB(db, OPS_PER_TENANT), {
    concurrency: LOAD_CONCURRENCY,
  });
  const loadDuration = (Date.now() - loadStart) / 1000;
  line("Load duration (sec)", loadDuration.toFixed(1));

  console.log(
    `\nWaiting ${(WAIT_AFTER_LOAD_MS / 1000).toFixed(
      1,
    )}s to let autovacuum and checkpoints do some work...`,
  );
  await sleep(WAIT_AFTER_LOAD_MS);

  await forceCheckpoint("Separate DBs");

  // ---------- AFTER ----------
  console.log("\n[DB MODE] Collecting maintenance stats after load...");
  const vacuumAfter = await getVacuumStatsForDatabases(testDatabases);
  const bgwriterAfter = await getBgwriterStats();

  printVacuumDelta("Separate DBs", vacuumBefore, vacuumAfter);
  printCheckpointDelta(
    "Cluster (with separate DBs load)",
    bgwriterBefore,
    bgwriterAfter,
  );

  console.log(
    "\nDone. Use these deltas to compare maintenance cost for separate-DB architecture.",
  );
}

async function runSchemasMode(): Promise<void> {
  const existingSchemas = await getExistingSchemas();
  const validSchemas = Array.from(existingSchemas).filter((item) =>
    item.startsWith(SCHEMA_PREFIX),
  );

  if (validSchemas.length === 0) {
    console.log(
      "\nNo schemas found for tenants list (check SCHEMA_DB_NAME / SCHEMA_PREFIX). Skipping schema test.",
    );
    return;
  }

  const testSchemas = validSchemas.slice(0, 1000);
  console.log(
    `\nValid schemas: ${validSchemas.length}/${tenants.length}. Using: ${testSchemas.length} for the test (DB = ${SCHEMA_DB_NAME}, prefix = '${SCHEMA_PREFIX}').`,
  );

  console.log(
    "\n[SCHEMA MODE] Collecting baseline maintenance stats (VACUUM + bgwriter) for schemas...",
  );
  await resetBgwriterStats();
  const vacuumSchemasBefore = await getVacuumStatsForSchemas(testSchemas);
  const bgwriterSchemasBefore = await getBgwriterStats();

  section("WRITE LOAD on schemas");
  console.log(
    `  Running write-heavy workload (INSERT/UPDATE/DELETE) on schemas to generate dead tuples and WAL (opsPerTenant=${OPS_PER_TENANT}, concurrency=${LOAD_CONCURRENCY})...`,
  );
  const loadStartSchemas = Date.now();
  await pMap(testSchemas, (schema) => writeLoadSchema(schema, OPS_PER_TENANT), {
    concurrency: LOAD_CONCURRENCY,
  });
  const loadDurationSchemas = (Date.now() - loadStartSchemas) / 1000;
  line("Load duration (sec)", loadDurationSchemas.toFixed(1));

  console.log(
    `\nWaiting ${(WAIT_AFTER_LOAD_MS / 1000).toFixed(
      1,
    )}s to let autovacuum and checkpoints do some work (schemas)...`,
  );
  await sleep(WAIT_AFTER_LOAD_MS);

  await forceCheckpoint(`Schemas on ${SCHEMA_DB_NAME}`);

  console.log(
    "\n[SCHEMA MODE] Collecting maintenance stats after load (schemas)...",
  );
  const vacuumSchemasAfter = await getVacuumStatsForSchemas(testSchemas);
  const bgwriterSchemasAfter = await getBgwriterStats();

  printVacuumDelta("Schemas", vacuumSchemasBefore, vacuumSchemasAfter);
  printCheckpointDelta(
    `Cluster (with schemas load on ${SCHEMA_DB_NAME})`,
    bgwriterSchemasBefore,
    bgwriterSchemasAfter,
  );

  console.log(
    "\nDone. Use these deltas to compare maintenance cost for separate-DB vs schema architecture.",
  );
}

async function main(): Promise<void> {
  const width = 70;
  console.log("\n" + "═".repeat(width));
  console.log("  MAINTENANCE TEST: VACUUM & CHECKPOINT (DBs vs Schemas)");
  console.log("═".repeat(width));
  console.log(
    `Mode: ${MODE}  |  opsPerTenant=${OPS_PER_TENANT}  concurrency=${LOAD_CONCURRENCY}`,
  );

  if (MODE === "dbs") {
    await runDbMode();
  } else if (MODE === "schemas") {
    await runSchemasMode();
  } else {
    console.error(`Unknown MODE='${MODE}'. Use 'dbs' or 'schemas'.`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});
