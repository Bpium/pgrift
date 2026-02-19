import { Client } from "pg";
import { CONFIG } from "./config";

interface VerifyResult {
  db: string;
  status: "ok" | "fail";
  reason?: string;
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
    CONFIG.excludeDatabases
  );
  await client.end();

  let tenants = rows.map((r: any) => r.datname as string);
  if (CONFIG.filterPrefix) {
    tenants = tenants.filter((db) => db.startsWith(CONFIG.filterPrefix!));
  }
  return tenants;
}

async function verifyTenant(dbName: string): Promise<VerifyResult> {
  const sourceClient = new Client({ ...CONFIG.source, database: dbName });
  const targetClient = new Client(CONFIG.target);

  await sourceClient.connect();
  await targetClient.connect();

  try {
    // 1. Сравниваем список таблиц
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

    const srcNames = srcTables.map((r) => r.table_name);
    const tgtNames = tgtTables.map((r) => r.table_name);

    const missing = srcNames.filter((t) => !tgtNames.includes(t));
    if (missing.length > 0) {
      return { db: dbName, status: "fail", reason: `missing tables: ${missing.join(", ")}` };
    }

    const extra = tgtNames.filter((t) => !srcNames.includes(t));
    if (extra.length > 0) {
      return { db: dbName, status: "fail", reason: `unexpected tables: ${extra.join(", ")}` };
    }

    // 2. Сравниваем количество строк в каждой таблице
    for (const table of srcNames) {
      const { rows: s } = await sourceClient.query(
        `SELECT COUNT(*) AS cnt FROM public."${table}"`
      );
      const { rows: t } = await targetClient.query(
        `SELECT COUNT(*) AS cnt FROM "${dbName}"."${table}"`
      );

      const srcCnt = parseInt(s[0].cnt);
      const tgtCnt = parseInt(t[0].cnt);

      if (srcCnt !== tgtCnt) {
        return {
          db: dbName,
          status: "fail",
          reason: `${table}: row count mismatch (src: ${srcCnt}, tgt: ${tgtCnt})`,
        };
      }
    }

    return { db: dbName, status: "ok" };
  } finally {
    await sourceClient.end();
    await targetClient.end();
  }
}

async function main(): Promise<void> {
  // Можно передать конкретные базы аргументом: npx ts-node verify.ts db-company1 db-company2
  const args = process.argv.slice(2);
  const tenants = args.length > 0 ? args : await getTenants();

  console.log(`Verifying ${tenants.length} tenant(s)...\n`);

  const results: VerifyResult[] = [];

  // Прогоняем последовательно, чтобы не нагружать базу
  for (const db of tenants) {
    process.stdout.write(`  ${db} ... `);
    const result = await verifyTenant(db);
    results.push(result);
    console.log(result.status === "ok" ? "✓" : `✗ ${result.reason}`);
  }

  const failed = results.filter((r) => r.status === "fail");
  console.log(`\n${results.length - failed.length}/${results.length} passed`);

  if (failed.length > 0) {
    console.log("\nFailed:");
    failed.forEach((r) => console.log(`  ${r.db}: ${r.reason}`));
    process.exit(1);
  }
}

main().catch((err) => {
  console.error("fatal:", err);
  process.exit(1);
});
