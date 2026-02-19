import { execSync } from "child_process";
import { Client } from "pg";
import fs from "fs";
import path from "path";

const CONFIG = {
  source: {
    host: "localhost",
    port: 5432,
    user: "postgres",
    password: "your_password",
  },
  target: {
    host: "localhost",
    port: 5432,
    user: "postgres",
    password: "your_password",
    database: "tenants",
  },
  dumpDir: "/tmp/pg_migration_dumps",
  stateFile: "./migration-state.json",
  concurrency: 10,
  excludeDatabases: ["postgres", "template0", "template1", "tenants"],
  filterPrefix: "db-" as string | null,
};

interface FailedEntry {
  db: string;
  error: string;
  attempts: number;
}

interface State {
  completed: string[];
  failed: FailedEntry[];
  startedAt: string;
  lastUpdated: string;
}

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

  const { rows } = await client.query("SELECT 1 FROM pg_database WHERE datname = $1", [CONFIG.target.database]);

  if (rows.length === 0) {
    await client.query(`CREATE DATABASE "${CONFIG.target.database}"`);
    console.log(`Created database: ${CONFIG.target.database}`);
  }

  await client.end();
}

async function getTenants(): Promise<string[]> {
  const client = new Client({ ...CONFIG.source, database: "postgres" });
  await client.connect();

  const placeholders = CONFIG.excludeDatabases.map((_, i) => `$${i + 1}`).join(", ");

  const { rows } = await client.query(
    `SELECT datname FROM pg_database
     WHERE datname NOT IN (${placeholders})
     AND datname NOT LIKE 'pg_%'
     ORDER BY datname`,
    CONFIG.excludeDatabases,
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
  const dumpFile = path.join(CONFIG.dumpDir, `${dbName}.dump`);
  const src = CONFIG.source;
  const tgt = CONFIG.target;

  execSync(`pg_dump -Fc -h ${src.host} -p ${src.port} -U ${src.user} -d "${dbName}" -f "${dumpFile}"`, {
    env: { ...process.env, PGPASSWORD: src.password },
    stdio: "pipe",
  });

  await createSchema(dbName);

  execSync(
    `pg_restore \
      -h ${tgt.host} -p ${tgt.port} -U ${tgt.user} \
      -d "${tgt.database}" \
      --schema="${dbName}" \
      --no-owner \
      --no-privileges \
      --single-transaction \
      "${dumpFile}"`,
    { env: { ...process.env, PGPASSWORD: tgt.password }, stdio: "pipe" },
  );

  if (fs.existsSync(dumpFile)) {
    fs.unlinkSync(dumpFile);
  }
}

async function verifyMigration(dbName: string): Promise<boolean> {
  const sourceClient = new Client({ ...CONFIG.source, database: dbName });
  const targetClient = new Client(CONFIG.target);

  await sourceClient.connect();
  await targetClient.connect();

  try {
    const { rows: srcRows } = await sourceClient.query(
      "SELECT COUNT(*) AS cnt FROM information_schema.tables WHERE table_schema = 'public'",
    );
    const { rows: tgtRows } = await targetClient.query(
      "SELECT COUNT(*) AS cnt FROM information_schema.tables WHERE table_schema = $1",
      [dbName],
    );

    const srcCount = parseInt(srcRows[0].cnt);
    const tgtCount = parseInt(tgtRows[0].cnt);

    if (srcCount !== tgtCount) {
      console.warn(`[verify] ${dbName}: table count mismatch (src: ${srcCount}, tgt: ${tgtCount})`);
      return false;
    }

    return true;
  } finally {
    await sourceClient.end();
    await targetClient.end();
  }
}

async function runBatch(tenants: string[], state: State): Promise<void> {
  await Promise.all(
    tenants.map(async (db) => {
      try {
        await migrateTenant(db);
        state.completed.push(db);
        console.log(`[done] ${db} (${state.completed.length} total)`);
      } catch (err: any) {
        const existing = state.failed.find((f) => f.db === db);
        if (existing) {
          existing.attempts++;
          existing.error = err.message;
        } else {
          state.failed.push({ db, error: err.message, attempts: 1 });
        }
        console.error(`[fail] ${db}: ${err.message.slice(0, 100)}`);
      }
    }),
  );
}

async function main(): Promise<void> {
  fs.mkdirSync(CONFIG.dumpDir, { recursive: true });

  await ensureTargetDatabase();

  const state = loadState();
  const allTenants = await getTenants();
  const completed = new Set(state.completed);
  const remaining = allTenants.filter((db) => !completed.has(db));

  console.log(
    `total: ${allTenants.length} | done: ${state.completed.length} | remaining: ${remaining.length} | failed: ${state.failed.length}`,
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

  for (let i = 0; i < batches.length; i++) {
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    const pct = Math.round((state.completed.length / allTenants.length) * 100);
    const eta = state.completed.length > 0 ? Math.round((elapsed / state.completed.length) * remaining.length) : "?";

    console.log(`\nbatch ${i + 1}/${batches.length} | ${pct}% | ${elapsed}s elapsed | ETA ${eta}s`);

    await runBatch(batches[i], state);
    saveState(state);
  }

  const totalTime = Math.round((Date.now() - startTime) / 1000);

  console.log(`\ncompleted: ${state.completed.length} | failed: ${state.failed.length} | time: ${totalTime}s`);

  if (state.failed.length > 0) {
    console.log("\nfailed:");
    state.failed.forEach((f) => console.log(`  ${f.db} (${f.attempts} attempts): ${f.error.slice(0, 100)}`));
  }

  fs.writeFileSync(
    "./migration-report.json",
    JSON.stringify({ ...state, totalDatabases: allTenants.length, totalTimeSeconds: totalTime }, null, 2),
  );
}

main().catch((err) => {
  console.error("fatal:", err);
  process.exit(1);
});
