// import { Client } from "pg";
// import pMap from "p-map";
// import fs from "fs";

// const tenants = JSON.parse(fs.readFileSync("./scripts/db-list.json", "utf8"));

// async function testSchema(schemaName) {
//   const client = new Client({
//     connectionString: `postgres://postgres:4321@localhost:5432/tenants`,
//   });

//   const start = performance.now();

//   await client.connect();

//   // Set search_path to work with the schema
//   //   await client.query(`SET search_path TO "${schemaName}"`);

//   const res = await client.query(`SELECT COUNT(*) FROM "${schemaName}".users_data`);

//   await client.end();

//   const duration = performance.now() - start;

//   return {
//     schema: schemaName,
//     rows: res.rows[0].count,
//     ms: duration,
//   };
// }

// async function main() {
//   const results = await pMap(tenants, (schema) => testSchema(schema), { concurrency: 50 });

//   console.table(results.slice(0, 10));

//   // Stats
//   const avgMs = results.reduce((sum, r) => sum + r.ms, 0) / results.length;
//   const maxMs = Math.max(...results.map((r) => r.ms));
//   const minMs = Math.min(...results.map((r) => r.ms));

//   console.log("\nStatistics:");
//   console.log(`Average: ${avgMs.toFixed(2)}ms`);
//   console.log(`Min: ${minMs.toFixed(2)}ms`);
//   console.log(`Max: ${maxMs.toFixed(2)}ms`);
//   console.log(`Total schemas tested: ${results.length}`);

//   // Save full results
//   fs.writeFileSync("./schema-performance-results.json", JSON.stringify(results, null, 2));
// }

// main().catch((err) => {
//   console.error("Error:", err);
//   process.exit(1);
// });

import { Client } from "pg";
import pMap from "p-map";
import fs from "fs";

const tenants = JSON.parse(fs.readFileSync("./scripts/db-list.json", "utf8"));

async function checkDatabase() {
  const client = new Client({
    connectionString: `postgres://postgres:4321@localhost:5432/tenants`,
  });

  await client.connect();

  // Проверяем существующие схемы
  const { rows: schemas } = await client.query(`
    SELECT schema_name 
    FROM information_schema.schemata 
    WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'public')
    ORDER BY schema_name
  `);

  console.log(`\nTotal schemas found: ${schemas.length}`);
  console.log(
    "First 10 schemas:",
    schemas.slice(0, 10).map((s) => s.schema_name),
  );

  // Проверяем есть ли схема из нашего списка
  const firstTenant = tenants[0];
  const { rows: checkSchema } = await client.query(
    `
    SELECT schema_name 
    FROM information_schema.schemata 
    WHERE schema_name = $1
  `,
    [firstTenant],
  );

  console.log(`\nChecking if '${firstTenant}' exists:`, checkSchema.length > 0 ? "YES" : "NO");

  // Если схема существует, проверяем таблицы
  if (checkSchema.length > 0) {
    const { rows: tables } = await client.query(
      `
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = $1
    `,
      [firstTenant],
    );

    console.log(
      `Tables in '${firstTenant}':`,
      tables.map((t) => t.table_name),
    );
  }

  await client.end();
}

async function testSchema(schemaName) {
  const client = new Client({
    connectionString: `postgres://postgres:4321@localhost:5432/tenants`,
  });

  try {
    const start = performance.now();

    await client.connect();

    const res = await client.query(`SELECT COUNT(*) FROM "${schemaName}".users_data`);

    await client.end();

    const duration = performance.now() - start;

    return {
      schema: schemaName,
      rows: res.rows[0].count,
      ms: duration,
    };
  } catch (err) {
    await client.end();
    return {
      schema: schemaName,
      rows: null,
      ms: null,
      error: err.message,
    };
  }
}

async function main() {
  // Сначала диагностика
  await checkDatabase();

  console.log("\n\nStarting performance tests...\n");

  const results = await pMap(tenants, (schema) => testSchema(schema), { concurrency: 50 });

  // Разделяем успешные и неудачные
  const successful = results.filter((r) => r.error === undefined);
  const failed = results.filter((r) => r.error !== undefined);

  console.log(`\nSuccessful: ${successful.length}, Failed: ${failed.length}`);

  if (successful.length > 0) {
    console.table(successful.slice(0, 10));

    const avgMs = successful.filter((r) => r.ms !== null).reduce((sum, r) => sum + r.ms, 0) / successful.length;
    const maxMs = Math.max(...successful.filter((r) => r.ms !== null).map((r) => r.ms));
    const minMs = Math.min(...successful.filter((r) => r.ms !== null).map((r) => r.ms));

    console.log("\nStatistics:");
    console.log(`Average: ${avgMs.toFixed(2)}ms`);
    console.log(`Min: ${minMs.toFixed(2)}ms`);
    console.log(`Max: ${maxMs.toFixed(2)}ms`);
    console.log(`Total schemas tested: ${successful.length}`);
  }

  if (failed.length > 0) {
    console.log("\nFailed schemas (first 5):");
    console.table(failed.slice(0, 5));
  }

  fs.writeFileSync("./schema-performance-results.json", JSON.stringify(results, null, 2));
}

main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});
