import { execSync } from "child_process";
import { Client } from "pg";
import fs from "fs";
import path from "path";
import { CONFIG } from "./config";
import { State, FailedEntry } from "./types";


function loadState(): State {
  if (fs.existsSync(CONFIG.stateFile)) {
    return JSON.parse(fs.readFileSync(CONFIG.stateFile, "utf-8"));
  }
  return {
    completed: [],
    failed: [],
    startedAt: new Date().toISOString(),
    lastUpdated: new Date().toISOString(),
  };
}

function saveState(state: State): void {
  state.lastUpdated = new Date().toISOString();
  fs.writeFileSync(CONFIG.stateFile, JSON.stringify(state, null, 2));
}

async function ensureTargetDatabase(): Promise<void> {
  const client = new Client({ ...CONFIG.target, database: "postgres" });
  await client.connect();

  const { rows } = await client.query(
    "SELECT 1 FROM pg_database WHERE datname = $1",
    [CONFIG.target.database]
  );

  if (rows.length === 0) {
    await client.query(`CREATE DATABASE "${CONFIG.target.database}"`);
    console.log(`Created database: ${CONFIG.target.database}`);
  }

  await client.end();
}

async function getTenants(): Promise<string[]> {
  const client = new Client({ ...CONFIG.source, database: "postgres" });
  await client.connect();

  const placeholders = CONFIG.excludeDatabases
    .map((_, i) => `$${i + 1}`)
    .join(", ");

  const { rows } = await client.query(
    `SELECT datname FROM pg_database
     WHERE datname NOT IN (${placeholders})
     AND datname NOT LIKE 'pg_%'
     ORDER BY datname`,
    CONFIG.excludeDatabases
  );

  await client.end();

  let tenants = rows.map((r: any) => r.datname as string);

  if (CONFIG.filterPrefix) {
    tenants = tenants.filter((db) => db.startsWith(CONFIG.filterPrefix!));
  }

  return tenants;
}

async function createSchema(schemaName: string): Promise<void> {
  const client = new Client(CONFIG.target);
  await client.connect();
  await client.query(`CREATE SCHEMA IF NOT EXISTS "${schemaName}"`);
  await client.end();
}

async function migrateTenant(dbName: string): Promise<void> {
  const dumpFile = path.join(CONFIG.dumpDir, `${dbName}.sql`);
  const src = CONFIG.source;
  const tgt = CONFIG.target;

  // Dump in plain text format so we can rewrite the schema name
  const srcPassword = typeof src.password === "string" ? src.password : "";
  execSync(
    `pg_dump -Fp -h ${src.host} -p ${src.port} -U ${src.user} -d "${dbName}" -f "${dumpFile}"`,
    { env: { ...process.env, PGPASSWORD: srcPassword }, stdio: "pipe" }
  );

  // Rewrite all references from "public" schema to the tenant schema name.
  // Covers SET search_path, schema-qualified identifiers, and COPY paths.
  let sql = fs.readFileSync(dumpFile, "utf-8");
  sql = sql
    .replace(/SET search_path = public/g, `SET search_path = "${dbName}"`)
    .replace(/^(CREATE|ALTER|COMMENT ON|GRANT|REVOKE)(.+?) public\./gm, `$1$2 "${dbName}".`)
    .replace(/\bpublic\./g, `"${dbName}".`);
  fs.writeFileSync(dumpFile, sql);

  await createSchema(dbName);

  const tgtPassword = typeof tgt.password === "string" ? tgt.password : "";
  execSync(
    `psql -h ${tgt.host} -p ${tgt.port} -U ${tgt.user} -d "${tgt.database}" -f "${dumpFile}"`,
    { env: { ...process.env, PGPASSWORD: tgtPassword }, stdio: "pipe" }
  );

  fs.unlinkSync(dumpFile);
}

async function verifyMigration(dbName: string): Promise<boolean> {
  const sourceClient = new Client({ ...CONFIG.source, database: dbName });
  const targetClient = new Client(CONFIG.target);

  await sourceClient.connect();
  await targetClient.connect();

  try {
    const { rows: srcTables } = await sourceClient.query<{ table_name: string }>(
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
       ORDER BY table_name`
    );

    const { rows: tgtTables } = await targetClient.query<{ table_name: string }>(
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = $1 AND table_type = 'BASE TABLE'
       ORDER BY table_name`,
      [dbName]
    );

    if (srcTables.length !== tgtTables.length) {
      console.warn(
        `[verify] ${dbName}: table count mismatch (src: ${srcTables.length}, tgt: ${tgtTables.length})`
      );
      return false;
    }

    // Check row counts for each table
    for (const { table_name } of srcTables) {
      const { rows: srcCount } = await sourceClient.query(
        `SELECT COUNT(*) AS cnt FROM public."${table_name}"`
      );
      const { rows: tgtCount } = await targetClient.query(
        `SELECT COUNT(*) AS cnt FROM "${dbName}"."${table_name}"`
      );

      if (parseInt(srcCount[0].cnt) !== parseInt(tgtCount[0].cnt)) {
        console.warn(
          `[verify] ${dbName}.${table_name}: row count mismatch ` +
          `(src: ${srcCount[0].cnt}, tgt: ${tgtCount[0].cnt})`
        );
        return false;
      }
    }

    return true;
  } finally {
    await sourceClient.end();
    await targetClient.end();
  }
}

async function runBatch(tenants: string[], state: State): Promise<void> {
  const results = await Promise.allSettled(
    tenants.map(async (db) => {
      await migrateTenant(db);

      const ok = await verifyMigration(db);
      if (!ok) {
        throw new Error("verification failed: table or row count mismatch");
      }

      return db;
    })
  );

  for (let i = 0; i < results.length; i++) {
    const db = tenants[i];
    const result = results[i];

    if (result.status === "fulfilled") {
      state.completed.push(db);
      console.log(`[done] ${db} (${state.completed.length} total)`);
    } else {
      const message = result.reason?.message ?? String(result.reason);
      const existing = state.failed.find((f) => f.db === db);
      if (existing) {
        existing.attempts++;
        existing.error = message;
      } else {
        state.failed.push({ db, error: message, attempts: 1 });
      }
      console.error(`[fail] ${db}: ${message.slice(0, 120)}`);
    }
  }
}

async function main(): Promise<void> {
  fs.mkdirSync(CONFIG.dumpDir, { recursive: true });

  await ensureTargetDatabase();

  const state = loadState();
  const allTenants = await getTenants();
  const completed = new Set(state.completed);
  const remaining = allTenants.filter((db) => !completed.has(db));

  console.log(
    `total: ${allTenants.length} | done: ${state.completed.length} | ` +
    `remaining: ${remaining.length} | failed: ${state.failed.length}`
  );

  if (remaining.length === 0) {
    console.log("nothing to migrate");
    return;
  }

  const batches: string[][] = [];
  for (let i = 0; i < remaining.length; i += CONFIG.concurrency) {
    batches.push(remaining.slice(i, i + CONFIG.concurrency));
  }

  const startTime = Date.now();
  let totalCompleted = state.completed.length;

  for (let i = 0; i < batches.length; i++) {
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    const migratedSoFar = state.completed.length - totalCompleted;
    const pct = Math.round((state.completed.length / allTenants.length) * 100);
    const eta =
      migratedSoFar > 0
        ? Math.round((elapsed / migratedSoFar) * (remaining.length - migratedSoFar))
        : "?";

    console.log(`\nbatch ${i + 1}/${batches.length} | ${pct}% | ${elapsed}s elapsed | ETA ~${eta}s`);

    await runBatch(batches[i], state);
    saveState(state);
  }

  const totalTime = Math.round((Date.now() - startTime) / 1000);

  console.log(
    `\ncompleted: ${state.completed.length} | failed: ${state.failed.length} | time: ${totalTime}s`
  );

  if (state.failed.length > 0) {
    console.log("\nfailed:");
    state.failed.forEach((f) =>
      console.log(`  ${f.db} (${f.attempts} attempts): ${f.error.slice(0, 120)}`)
    );
  }

  fs.writeFileSync(
    "./migration-report.json",
    JSON.stringify(
      { ...state, totalDatabases: allTenants.length, totalTimeSeconds: totalTime },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error("fatal:", err);
  process.exit(1);
});
