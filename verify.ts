import { Client } from "pg";
import { CONFIG } from "./src/config";
import { getTenants } from "./src/db";

interface VerifyResult {
  db: string;
  status: "ok" | "fail";
  reason?: string;
}

async function verifyTenant(dbName: string): Promise<VerifyResult> {
  const sourceClient = new Client({ ...CONFIG.source, database: dbName });
  const targetClient = new Client(CONFIG.target);

  await sourceClient.connect();
  await targetClient.connect();

  try {
    // 1. Compare table lists
    const { rows: srcTables } = await sourceClient.query<{ table_name: string }>(
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
       ORDER BY table_name`,
    );

    const { rows: tgtTables } = await targetClient.query<{ table_name: string }>(
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = $1 AND table_type = 'BASE TABLE'
       ORDER BY table_name`,
      [dbName],
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

    // 2. Compare row counts per table
    for (const table of srcNames) {
      const { rows: s } = await sourceClient.query(`SELECT COUNT(*) AS cnt FROM public."${table}"`);
      const { rows: t } = await targetClient.query(
        `SELECT COUNT(*) AS cnt FROM "${dbName}"."${table}"`,
      );

      const srcCnt = parseInt(s[0].cnt, 10);
      const tgtCnt = parseInt(t[0].cnt, 10);

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
  // Optional: pass specific DBs as args: npx ts-node verify.ts db-company1 db-company2
  const args = process.argv.slice(2);
  const tenants = args.length > 0 ? args : await getTenants();

  console.log(`Verifying ${tenants.length} tenant(s)...\n`);

  const results: VerifyResult[] = [];

  // Run sequentially to avoid overloading the database
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
    for (const r of failed) console.log(`  ${r.db}: ${r.reason}`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error("fatal:", err);
  process.exit(1);
});
