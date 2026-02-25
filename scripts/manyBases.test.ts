import { Client } from "pg";
import pMap from "p-map";
import fs from "fs";

const tenants = JSON.parse(fs.readFileSync("./scripts/db-list.json", "utf8"));

async function testDb(dbName) {
  const client = new Client({
    connectionString: `postgres://postgres:4321@localhost:5432/${dbName}`,
  });

  const start = performance.now();
  await client.connect();

  const res = await client.query(`SELECT COUNT(*) FROM users_data`);
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
