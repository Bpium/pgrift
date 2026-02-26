import fs from "node:fs";
import pMap from "p-map";
import { Client } from "pg";
import { buildConnectionString, CONFIG } from "../src/config";

const tenants = JSON.parse(fs.readFileSync(CONFIG.dbListPath, "utf8"));

async function testDb(dbName) {
  const client = new Client({
    connectionString: buildConnectionString(CONFIG.source, dbName),
  });

  const start = performance.now();
  await client.connect();

  const res = await client.query(`SELECT COUNT(*) FROM "${CONFIG.benchTable}"`);
  await client.end();

  const duration = performance.now() - start;

  return {
    db: dbName,
    rows: res.rows[0].count,
    ms: duration,
  };
}

async function main() {
  const results = await pMap(tenants, (db) => testDb(db), { concurrency: 50 });

  console.table(results.slice(0, 10));
}

main();
