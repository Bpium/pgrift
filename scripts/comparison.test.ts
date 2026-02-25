import { Client } from "pg";
import pMap from "p-map";
import fs from "fs";

const tenants = JSON.parse(fs.readFileSync("./scripts/db-list.json", "utf8"));

// ============================================
// CONFIGURATION
// ============================================

const CONNECTION_CONFIG = {
  SEPARATE_DB: (dbName) => `postgres://postgres:4321@localhost:5432/${dbName}`,
  TENANTS_DB: "postgres://postgres:4321@localhost:5432/tenants",
};

const QUERIES = {
  DISCARD_ALL: "DISCARD ALL",
  TRACK_IO_TIMING: "SET track_io_timing = ON",
  SEQ_SCAN: (tableName) => `SELECT * FROM ${tableName} WHERE "dbId" > 0`,
  RANDOM_QUERY: (tableName) => `SELECT * FROM ${tableName} WHERE "dbId" = $1`,
  WRITE_TEST: (tableName) =>
    `INSERT INTO ${tableName} ("dbId", "name") VALUES ($1, $2) ON CONFLICT ("dbId") DO UPDATE SET "name" = EXCLUDED."name"`,
  EXPLAIN_ANALYZE: (tableName) => `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM ${tableName} WHERE "dbId" > 0`,
};

// ============================================
// SEPARATE DATABASES
// ============================================

async function testSequentialScan_SeparateDB(dbName) {
  const client = new Client({ connectionString: CONNECTION_CONFIG.SEPARATE_DB(dbName) });

  try {
    await client.connect();
    await client.query(QUERIES.DISCARD_ALL);

    const start = performance.now();
    const res = await client.query(QUERIES.SEQ_SCAN("users_data"));
    const duration = performance.now() - start;

    await client.end();

    return {
      name: dbName,
      type: "separate_db",
      test: "seq_scan",
      rows: res.rowCount,
      ms: duration,
    };
  } catch (err) {
    await client.end();
    return { name: dbName, type: "separate_db", test: "seq_scan", error: err.message };
  }
}

async function testRandomIO_SeparateDB(dbName) {
  const client = new Client({ connectionString: CONNECTION_CONFIG.SEPARATE_DB(dbName) });

  try {
    await client.connect();

    const start = performance.now();
    for (let i = 0; i < 100; i++) {
      const randomId = Math.floor(Math.random() * 10000) + 1;
      await client.query(QUERIES.RANDOM_QUERY("users_data"), [randomId]);
    }
    const duration = performance.now() - start;

    await client.end();

    return {
      name: dbName,
      type: "separate_db",
      test: "random_io",
      queries: 100,
      ms: duration,
      avg_per_query: duration / 100,
    };
  } catch (err) {
    await client.end();
    return { name: dbName, type: "separate_db", test: "random_io", error: err.message };
  }
}

async function testWriteIO_SeparateDB(dbName) {
  const client = new Client({ connectionString: CONNECTION_CONFIG.SEPARATE_DB(dbName) });

  try {
    await client.connect();
    await client.query("BEGIN");

    const start = performance.now();
    for (let i = 0; i < 100; i++) {
      await client.query(QUERIES.WRITE_TEST("users_data"), [100000 + i, `test_data_${i}`]);
    }
    const duration = performance.now() - start;

    await client.query("ROLLBACK");
    await client.end();

    return {
      name: dbName,
      type: "separate_db",
      test: "write_io",
      inserts: 100,
      ms: duration,
      avg_per_insert: duration / 100,
    };
  } catch (err) {
    await client.end();
    return { name: dbName, type: "separate_db", test: "write_io", error: err.message };
  }
}

async function testIOStats_SeparateDB(dbName) {
  const client = new Client({ connectionString: CONNECTION_CONFIG.SEPARATE_DB(dbName) });

  try {
    await client.connect();
    await client.query(QUERIES.TRACK_IO_TIMING);

    const start = performance.now();
    const res = await client.query(QUERIES.EXPLAIN_ANALYZE("users_data"));
    const duration = performance.now() - start;

    const plan = res.rows[0]["QUERY PLAN"][0];
    const stats = plan["Plan"];

    await client.end();

    return {
      name: dbName,
      type: "separate_db",
      test: "io_stats",
      ms: duration,
      shared_hit_blocks: stats["Shared Hit Blocks"] || 0,
      shared_read_blocks: stats["Shared Read Blocks"] || 0,
      io_read_time: stats["I/O Read Time"] || 0,
      rows: stats["Actual Rows"] || 0,
    };
  } catch (err) {
    await client.end();
    return { name: dbName, type: "separate_db", test: "io_stats", error: err.message };
  }
}

// ============================================
// SCHEMAS IN SINGLE DATABASE
// ============================================

async function testSequentialScan_Schema(schemaName) {
  const client = new Client({ connectionString: CONNECTION_CONFIG.TENANTS_DB });

  try {
    await client.connect();
    await client.query(QUERIES.DISCARD_ALL);

    const start = performance.now();
    const res = await client.query(QUERIES.SEQ_SCAN(`"${schemaName}".users_data`));
    const duration = performance.now() - start;

    await client.end();

    return {
      name: schemaName,
      type: "schema",
      test: "seq_scan",
      rows: res.rowCount,
      ms: duration,
    };
  } catch (err) {
    await client.end();
    return { name: schemaName, type: "schema", test: "seq_scan", error: err.message };
  }
}

async function testRandomIO_Schema(schemaName) {
  const client = new Client({ connectionString: CONNECTION_CONFIG.TENANTS_DB });

  try {
    await client.connect();

    const start = performance.now();
    for (let i = 0; i < 100; i++) {
      const randomId = Math.floor(Math.random() * 10000) + 1;
      await client.query(QUERIES.RANDOM_QUERY(`"${schemaName}".users_data`), [randomId]);
    }
    const duration = performance.now() - start;

    await client.end();

    return {
      name: schemaName,
      type: "schema",
      test: "random_io",
      queries: 100,
      ms: duration,
      avg_per_query: duration / 100,
    };
  } catch (err) {
    await client.end();
    return { name: schemaName, type: "schema", test: "random_io", error: err.message };
  }
}

async function testWriteIO_Schema(schemaName) {
  const client = new Client({ connectionString: CONNECTION_CONFIG.TENANTS_DB });

  try {
    await client.connect();
    await client.query("BEGIN");

    const start = performance.now();
    for (let i = 0; i < 100; i++) {
      await client.query(QUERIES.WRITE_TEST(`"${schemaName}".users_data`), [100000 + i, `test_data_${i}`]);
    }
    const duration = performance.now() - start;

    await client.query("ROLLBACK");
    await client.end();

    return {
      name: schemaName,
      type: "schema",
      test: "write_io",
      inserts: 100,
      ms: duration,
      avg_per_insert: duration / 100,
    };
  } catch (err) {
    await client.end();
    return { name: schemaName, type: "schema", test: "write_io", error: err.message };
  }
}

async function testIOStats_Schema(schemaName) {
  const client = new Client({ connectionString: CONNECTION_CONFIG.TENANTS_DB });

  try {
    await client.connect();
    await client.query(QUERIES.TRACK_IO_TIMING);

    const start = performance.now();
    const res = await client.query(QUERIES.EXPLAIN_ANALYZE(`"${schemaName}".users_data`));
    const duration = performance.now() - start;

    const plan = res.rows[0]["QUERY PLAN"][0];
    const stats = plan["Plan"];

    await client.end();

    return {
      name: schemaName,
      type: "schema",
      test: "io_stats",
      ms: duration,
      shared_hit_blocks: stats["Shared Hit Blocks"] || 0,
      shared_read_blocks: stats["Shared Read Blocks"] || 0,
      io_read_time: stats["I/O Read Time"] || 0,
      rows: stats["Actual Rows"] || 0,
    };
  } catch (err) {
    await client.end();
    return { name: schemaName, type: "schema", test: "io_stats", error: err.message };
  }
}

// ============================================
// COMPARISON LOGIC
// ============================================

function compareResults(separateDBResults, schemaResults, testName) {
  const dbSuccess = separateDBResults.filter((r) => !r.error);
  const schemaSuccess = schemaResults.filter((r) => !r.error);

  if (dbSuccess.length === 0 || schemaSuccess.length === 0) {
    console.log(`Not enough data for ${testName}`);
    return;
  }

  const dbAvg = dbSuccess.reduce((sum, r) => sum + r.ms, 0) / dbSuccess.length;
  const schemaAvg = schemaSuccess.reduce((sum, r) => sum + r.ms, 0) / schemaSuccess.length;

  const diff = ((dbAvg - schemaAvg) / dbAvg) * 100;
  const winner = schemaAvg < dbAvg ? "SCHEMA" : "SEPARATE_DB";

  console.log(`${testName}:`);
  console.log(`  Separate DBs: ${dbAvg.toFixed(2)}ms`);
  console.log(`  Schemas:      ${schemaAvg.toFixed(2)}ms`);
  console.log(`  Winner:       ${winner} (${Math.abs(diff).toFixed(1)}% ${diff > 0 ? "faster" : "slower"})`);
  console.log();
}

function compareIOStats(separateDBResults, schemaResults) {
  const dbSuccess = separateDBResults.filter((r) => !r.error);
  const schemaSuccess = schemaResults.filter((r) => !r.error);

  if (dbSuccess.length === 0 || schemaSuccess.length === 0) {
    console.log("Not enough data for IO stats");
    return;
  }

  const dbHit = dbSuccess.reduce((sum, r) => sum + r.shared_hit_blocks, 0);
  const dbRead = dbSuccess.reduce((sum, r) => sum + r.shared_read_blocks, 0);
  const dbHitRatio = (dbHit / (dbHit + dbRead)) * 100;

  const schemaHit = schemaSuccess.reduce((sum, r) => sum + r.shared_hit_blocks, 0);
  const schemaRead = schemaSuccess.reduce((sum, r) => sum + r.shared_read_blocks, 0);
  const schemaHitRatio = (schemaHit / (schemaHit + schemaRead)) * 100;

  console.log("IO Statistics:");
  console.log("  Separate DBs:");
  console.log(`    Cache Hit Ratio: ${dbHitRatio.toFixed(2)}%`);
  console.log(`    Disk Reads:      ${dbRead} blocks`);
  console.log(`    Cache Hits:      ${dbHit} blocks`);
  console.log("  Schemas:");
  console.log(`    Cache Hit Ratio: ${schemaHitRatio.toFixed(2)}%`);
  console.log(`    Disk Reads:      ${schemaRead} blocks`);
  console.log(`    Cache Hits:      ${schemaHit} blocks`);
  console.log(`  Winner:            ${schemaHitRatio > dbHitRatio ? "SCHEMA" : "SEPARATE_DB"} (better cache)`);
  console.log();
}

// ============================================
// MAIN
// ============================================

async function main() {
  console.log("IO PERFORMANCE COMPARISON");
  console.log("Separate DBs vs Schemas in One DB");
  console.log("=".repeat(50));
  console.log();

  const testTenants = tenants.slice(0, 50);
  console.log(`Testing ${testTenants.length} tenants...`);
  console.log();

  // Sequential Scan Test
  console.log("1. Sequential Scan Test (full table read)");
  const seqDB = await pMap(testTenants, testSequentialScan_SeparateDB, { concurrency: 10 });
  const seqSchema = await pMap(testTenants, testSequentialScan_Schema, { concurrency: 10 });
  compareResults(seqDB, seqSchema, "Sequential Scan");

  // Random IO Test
  console.log("2. Random IO Test (100 random queries per tenant)");
  const randomDB = await pMap(testTenants, testRandomIO_SeparateDB, { concurrency: 10 });
  const randomSchema = await pMap(testTenants, testRandomIO_Schema, { concurrency: 10 });
  compareResults(randomDB, randomSchema, "Random IO");

  // Write IO Test
  console.log("3. Write IO Test (100 inserts per tenant)");
  const writeDB = await pMap(testTenants, testWriteIO_SeparateDB, { concurrency: 5 });
  const writeSchema = await pMap(testTenants, testWriteIO_Schema, { concurrency: 5 });
  compareResults(writeDB, writeSchema, "Write IO");

  // IO Statistics
  console.log("4. Buffer/IO Statistics");
  const ioStatsDB = await pMap(testTenants, testIOStats_SeparateDB, { concurrency: 10 });
  const ioStatsSchema = await pMap(testTenants, testIOStats_Schema, { concurrency: 10 });
  compareIOStats(ioStatsDB, ioStatsSchema);

  // Summary
  console.log("SUMMARY");
  console.log("=".repeat(50));
  console.log();

  const allResults = {
    sequential_scan: { separate_db: seqDB, schema: seqSchema },
    random_io: { separate_db: randomDB, schema: randomSchema },
    write_io: { separate_db: writeDB, schema: writeSchema },
    io_stats: { separate_db: ioStatsDB, schema: ioStatsSchema },
  };

  fs.writeFileSync("./io-comparison-results.json", JSON.stringify(allResults, null, 2));
  console.log("Results saved to io-comparison-results.json");
}

main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});
