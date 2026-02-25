import { Client } from "pg";
import pMap from "p-map";
import fs from "fs";
import os from "os";

const tenants: string[] = JSON.parse(fs.readFileSync("./scripts/db-list.json", "utf8"));

// ============================================
// TYPES
// ============================================

interface SystemStats {
  cpu_count: number;
  cpu_usage: string[];
  memory_total_gb: string;
  memory_used_gb: string;
  memory_free_gb: string;
  memory_usage_pct: string;
}

interface PostgresStats {
  connections?: string;
  total_transactions?: string;
  disk_blocks_read?: string;
  cache_blocks_hit?: string;
  cache_hit_ratio?: string;
  db_size?: string;
  error?: string;
}

interface LoadTestResult {
  name: string;
  type: "separate_db" | "schema";
  queries_executed: number;
  errors: number;
  avg_latency: number;
  p95_latency: number;
  p99_latency: number;
  error?: string;
}

interface ConnectionPoolResult {
  type: "separate_db" | "schema";
  total_connections: number;
  duration_ms: number;
  queries_per_sec: string;
  system_before: SystemStats;
  system_after: SystemStats;
}

// ============================================
// VALIDATION
// ============================================

async function getExistingSchemas(): Promise<Set<string>> {
  try {
    const client = new Client({
      connectionString: `postgres://postgres:4321@localhost:5432/tenants`,
    });
    await client.connect();

    const { rows } = await client.query(`
      SELECT schema_name 
      FROM information_schema.schemata 
      WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'public')
    `);

    await client.end();

    return new Set(rows.map((r) => r.schema_name));
  } catch (err) {
    console.error("⚠️  Failed to get existing schemas:", err);
    return new Set();
  }
}

async function getExistingDatabases(): Promise<Set<string>> {
  try {
    const client = new Client({
      connectionString: `postgres://postgres:4321@localhost:5432/postgres`,
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
    console.error("⚠️  Failed to get existing databases:", err);
    return new Set();
  }
}

// ============================================
// SYSTEM MONITORING
// ============================================

function getSystemStats(): SystemStats {
  const cpus = os.cpus();
  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const usedMem = totalMem - freeMem;

  return {
    cpu_count: cpus.length,
    cpu_usage: cpus.map((cpu) => {
      const total = Object.values(cpu.times).reduce((a, b) => a + b, 0);
      const idle = cpu.times.idle;
      return ((((total - idle) / total) * 100) as number).toFixed(1);
    }),
    memory_total_gb: (totalMem / 1024 / 1024 / 1024).toFixed(2),
    memory_used_gb: (usedMem / 1024 / 1024 / 1024).toFixed(2),
    memory_free_gb: (freeMem / 1024 / 1024 / 1024).toFixed(2),
    memory_usage_pct: ((usedMem / totalMem) * 100).toFixed(1),
  };
}

async function getPostgresStats(connectionString: string): Promise<PostgresStats> {
  try {
    const client = new Client({ connectionString });
    await client.connect();

    const { rows: dbStats } = await client.query(`
      SELECT 
        numbackends as connections,
        xact_commit + xact_rollback as total_transactions,
        blks_read as disk_blocks_read,
        blks_hit as cache_blocks_hit,
        ROUND(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 2) as cache_hit_ratio
      FROM pg_stat_database 
      WHERE datname = current_database()
    `);

    const { rows: sizeStats } = await client.query(`
      SELECT pg_size_pretty(pg_database_size(current_database())) as db_size
    `);

    await client.end();

    return {
      ...dbStats[0],
      db_size: sizeStats[0].db_size,
    };
  } catch (err) {
    return { error: err instanceof Error ? err.message : String(err) };
  }
}

// ============================================
// HEAVY LOAD TESTS
// ============================================

async function heavyLoadTest_SeparateDB(dbName: string, duration: number = 10000): Promise<LoadTestResult> {
  const results: LoadTestResult = {
    name: dbName,
    type: "separate_db",
    queries_executed: 0,
    errors: 0,
    avg_latency: 0,
    p95_latency: 0,
    p99_latency: 0,
  };

  const latencies: number[] = [];
  const startTime = Date.now();
  const client = new Client({
    connectionString: `postgres://postgres:4321@localhost:5432/${dbName}`,
  });

  try {
    await client.connect();

    while (Date.now() - startTime < duration) {
      const queryStart = performance.now();

      try {
        const queryType = Math.random();

        if (queryType < 0.6) {
          const randomId = Math.floor(Math.random() * 10000) + 1;
          await client.query(`SELECT * FROM users_data WHERE "dbId" = $1`, [randomId]);
        } else if (queryType < 0.9) {
          await client.query(`SELECT * FROM users_data WHERE "dbId" > $1 LIMIT 10`, [Math.floor(Math.random() * 9000)]);
        } else {
          await client.query(`UPDATE users_data SET "name" = $1 WHERE "dbId" = $2`, [
            `test_${Date.now()}`,
            Math.floor(Math.random() * 10000) + 1,
          ]);
        }

        const latency = performance.now() - queryStart;
        latencies.push(latency);
        results.queries_executed++;
      } catch (err) {
        results.errors++;
      }
    }

    await client.end();

    if (latencies.length > 0) {
      latencies.sort((a, b) => a - b);
      results.avg_latency = latencies.reduce((a, b) => a + b, 0) / latencies.length;
      results.p95_latency = latencies[Math.floor(latencies.length * 0.95)];
      results.p99_latency = latencies[Math.floor(latencies.length * 0.99)];
    }

    return results;
  } catch (err) {
    try {
      await client.end();
    } catch {}
    return { ...results, error: err instanceof Error ? err.message : String(err) };
  }
}

async function heavyLoadTest_Schema(schemaName: string, duration: number = 10000): Promise<LoadTestResult> {
  const results: LoadTestResult = {
    name: schemaName,
    type: "schema",
    queries_executed: 0,
    errors: 0,
    avg_latency: 0,
    p95_latency: 0,
    p99_latency: 0,
  };

  const latencies: number[] = [];
  const startTime = Date.now();
  const client = new Client({
    connectionString: `postgres://postgres:4321@localhost:5432/tenants`,
  });

  try {
    await client.connect();

    while (Date.now() - startTime < duration) {
      const queryStart = performance.now();

      try {
        const queryType = Math.random();

        if (queryType < 0.6) {
          const randomId = Math.floor(Math.random() * 10000) + 1;
          await client.query(`SELECT * FROM "${schemaName}".users_data WHERE "dbId" = $1`, [randomId]);
        } else if (queryType < 0.9) {
          await client.query(`SELECT * FROM "${schemaName}".users_data WHERE "dbId" > $1 LIMIT 10`, [
            Math.floor(Math.random() * 9000),
          ]);
        } else {
          await client.query(`UPDATE "${schemaName}".users_data SET "name" = $1 WHERE "dbId" = $2`, [
            `test_${Date.now()}`,
            Math.floor(Math.random() * 10000) + 1,
          ]);
        }

        const latency = performance.now() - queryStart;
        latencies.push(latency);
        results.queries_executed++;
      } catch (err) {
        results.errors++;
      }
    }

    await client.end();

    if (latencies.length > 0) {
      latencies.sort((a, b) => a - b);
      results.avg_latency = latencies.reduce((a, b) => a + b, 0) / latencies.length;
      results.p95_latency = latencies[Math.floor(latencies.length * 0.95)];
      results.p99_latency = latencies[Math.floor(latencies.length * 0.99)];
    }

    return results;
  } catch (err) {
    try {
      await client.end();
    } catch {}
    return { ...results, error: err instanceof Error ? err.message : String(err) };
  }
}

// ============================================
// CONNECTION POOL TEST
// ============================================

async function connectionPoolTest_SeparateDB(
  dbNames: string[],
  concurrency: number = 50,
): Promise<ConnectionPoolResult> {
  console.log(`\n🔗 Connection Pool Test (Separate DBs, ${concurrency} concurrent)...`);

  const systemBefore = getSystemStats();
  const startTime = Date.now();

  await pMap(
    dbNames,
    async (db) => {
      const client = new Client({
        connectionString: `postgres://postgres:4321@localhost:5432/${db}`,
      });
      await client.connect();
      await client.query(`SELECT COUNT(*) FROM users_data`);
      await client.end();
      return { db, success: true };
    },
    { concurrency },
  );

  const duration = Date.now() - startTime;
  const systemAfter = getSystemStats();

  return {
    type: "separate_db",
    total_connections: dbNames.length,
    duration_ms: duration,
    queries_per_sec: (dbNames.length / (duration / 1000)).toFixed(2),
    system_before: systemBefore,
    system_after: systemAfter,
  };
}

async function connectionPoolTest_Schema(
  schemaNames: string[],
  concurrency: number = 50,
): Promise<ConnectionPoolResult> {
  console.log(`\n🔗 Connection Pool Test (Schemas, ${concurrency} concurrent)...`);

  const systemBefore = getSystemStats();
  const startTime = Date.now();

  await pMap(
    schemaNames,
    async (schema) => {
      const client = new Client({
        connectionString: `postgres://postgres:4321@localhost:5432/tenants`,
      });
      await client.connect();
      await client.query(`SELECT COUNT(*) FROM "${schema}".users_data`);
      await client.end();
      return { schema, success: true };
    },
    { concurrency },
  );

  const duration = Date.now() - startTime;
  const systemAfter = getSystemStats();

  return {
    type: "schema",
    total_connections: schemaNames.length,
    duration_ms: duration,
    queries_per_sec: (schemaNames.length / (duration / 1000)).toFixed(2),
    system_before: systemBefore,
    system_after: systemAfter,
  };
}

// ============================================
// MAIN
// ============================================

async function main(): Promise<void> {
  console.log("=".repeat(60));
  console.log("🔥 HEAVY LOAD PERFORMANCE TEST");
  console.log("   Simulating production workload: 300 QPS, mixed queries");
  console.log("=".repeat(60));

  // Проверяем что существует
  console.log("\n🔍 Validating databases and schemas...");
  const existingDatabases = await getExistingDatabases();
  const existingSchemas = await getExistingSchemas();

  const validDatabases = tenants.filter((db) => existingDatabases.has(db));
  const validSchemas = tenants.filter((schema) => existingSchemas.has(schema));

  const missingDatabases = tenants.filter((db) => !existingDatabases.has(db));
  const missingSchemas = tenants.filter((schema) => !existingSchemas.has(schema));

  console.log(`✅ Valid databases: ${validDatabases.length}/${tenants.length}`);
  console.log(`✅ Valid schemas: ${validSchemas.length}/${tenants.length}`);

  if (missingDatabases.length > 0) {
    console.log(`⚠️  Missing databases (${missingDatabases.length}): ${missingDatabases.slice(0, 5).join(", ")}...`);
  }

  if (missingSchemas.length > 0) {
    console.log(`⚠️  Missing schemas (${missingSchemas.length}): ${missingSchemas.slice(0, 5).join(", ")}...`);
  }

  if (validDatabases.length === 0 || validSchemas.length === 0) {
    console.error("\n❌ Not enough valid databases or schemas to test!");
    process.exit(1);
  }

  const testDatabases = validDatabases.slice(0, 100);
  const testSchemas = validSchemas.slice(0, 100);

  console.log(`\nTesting ${testDatabases.length} databases and ${testSchemas.length} schemas...`);

  // ===== 1. System baseline =====
  console.log("\n📊 System Baseline:");
  const baseline = getSystemStats();
  console.log(JSON.stringify(baseline, null, 2));

  // ===== 2. Heavy Load Test =====
  console.log("\n\n🔥 Heavy Load Test (10 seconds per tenant, concurrency=20)...");
  console.log("Testing Separate DBs...");

  const loadTestDB = await pMap(testDatabases, (db) => heavyLoadTest_SeparateDB(db, 10000), {
    concurrency: 20,
  });

  console.log("Testing Schemas...");

  const loadTestSchema = await pMap(testSchemas, (schema) => heavyLoadTest_Schema(schema, 10000), {
    concurrency: 20,
  });

  // Анализ результатов
  const dbSuccess = loadTestDB.filter((r) => !r.error && r.queries_executed > 0);
  const schemaSuccess = loadTestSchema.filter((r) => !r.error && r.queries_executed > 0);

  console.log("\n📈 Heavy Load Results:");
  console.log("\nSeparate DBs:");
  console.log(`  Successful tests:  ${dbSuccess.length}/${loadTestDB.length}`);
  console.log(`  Total queries:     ${dbSuccess.reduce((sum, r) => sum + r.queries_executed, 0)}`);
  console.log(
    `  Avg latency:       ${(dbSuccess.reduce((sum, r) => sum + r.avg_latency, 0) / dbSuccess.length).toFixed(2)}ms`,
  );
  console.log(
    `  P95 latency:       ${(dbSuccess.reduce((sum, r) => sum + r.p95_latency, 0) / dbSuccess.length).toFixed(2)}ms`,
  );
  console.log(
    `  P99 latency:       ${(dbSuccess.reduce((sum, r) => sum + r.p99_latency, 0) / dbSuccess.length).toFixed(2)}ms`,
  );
  console.log(`  Errors:            ${dbSuccess.reduce((sum, r) => sum + r.errors, 0)}`);

  console.log("\nSchemas:");
  console.log(`  Successful tests:  ${schemaSuccess.length}/${loadTestSchema.length}`);
  console.log(`  Total queries:     ${schemaSuccess.reduce((sum, r) => sum + r.queries_executed, 0)}`);
  console.log(
    `  Avg latency:       ${(schemaSuccess.reduce((sum, r) => sum + r.avg_latency, 0) / schemaSuccess.length).toFixed(2)}ms`,
  );
  console.log(
    `  P95 latency:       ${(schemaSuccess.reduce((sum, r) => sum + r.p95_latency, 0) / schemaSuccess.length).toFixed(2)}ms`,
  );
  console.log(
    `  P99 latency:       ${(schemaSuccess.reduce((sum, r) => sum + r.p99_latency, 0) / schemaSuccess.length).toFixed(2)}ms`,
  );
  console.log(`  Errors:            ${schemaSuccess.reduce((sum, r) => sum + r.errors, 0)}`);

  // ===== 3. Connection Pool Test =====
  const poolTestDB = await connectionPoolTest_SeparateDB(validDatabases.slice(0, 200), 50);
  const poolTestSchema = await connectionPoolTest_Schema(validSchemas.slice(0, 200), 50);

  console.log("\n🔗 Connection Pool Test Results:");
  console.log("\nSeparate DBs:");
  console.log(`  Duration:          ${poolTestDB.duration_ms}ms`);
  console.log(`  Queries/sec:       ${poolTestDB.queries_per_sec}`);
  console.log(
    `  Memory after:      ${poolTestDB.system_after.memory_used_gb}GB (${poolTestDB.system_after.memory_usage_pct}%)`,
  );

  console.log("\nSchemas:");
  console.log(`  Duration:          ${poolTestSchema.duration_ms}ms`);
  console.log(`  Queries/sec:       ${poolTestSchema.queries_per_sec}`);
  console.log(
    `  Memory after:      ${poolTestSchema.system_after.memory_used_gb}GB (${poolTestSchema.system_after.memory_usage_pct}%)`,
  );

  // ===== 4. Postgres Stats =====
  console.log("\n💾 PostgreSQL Statistics:");

  const dbStats = await getPostgresStats(`postgres://postgres:4321@localhost:5432/${validDatabases[0]}`);
  const schemaStats = await getPostgresStats(`postgres://postgres:4321@localhost:5432/tenants`);

  console.log("\nSeparate DB sample:");
  console.log(JSON.stringify(dbStats, null, 2));

  console.log("\nSchemas DB:");
  console.log(JSON.stringify(schemaStats, null, 2));

  // ===== Save Results =====
  const allResults = {
    validation: {
      total_tenants: tenants.length,
      valid_databases: validDatabases.length,
      valid_schemas: validSchemas.length,
      missing_databases: missingDatabases,
      missing_schemas: missingSchemas,
    },
    system_baseline: baseline,
    heavy_load: {
      separate_db: loadTestDB,
      schema: loadTestSchema,
    },
    connection_pool: {
      separate_db: poolTestDB,
      schema: poolTestSchema,
    },
    postgres_stats: {
      separate_db: dbStats,
      schema: schemaStats,
    },
  };

  fs.writeFileSync("./heavy-load-results.json", JSON.stringify(allResults, null, 2));
  console.log("\n✅ Results saved to heavy-load-results.json");

  // ===== Final Verdict =====
  console.log("\n" + "=".repeat(60));
  console.log("🏆 VERDICT");
  console.log("=".repeat(60));

  const dbAvgLatency = dbSuccess.reduce((sum, r) => sum + r.avg_latency, 0) / dbSuccess.length;
  const schemaAvgLatency = schemaSuccess.reduce((sum, r) => sum + r.avg_latency, 0) / schemaSuccess.length;
  const latencyImprovement = (((dbAvgLatency - schemaAvgLatency) / dbAvgLatency) * 100).toFixed(1);

  const dbTotalQueries = dbSuccess.reduce((sum, r) => sum + r.queries_executed, 0);
  const schemaTotalQueries = schemaSuccess.reduce((sum, r) => sum + r.queries_executed, 0);
  const throughputImprovement = (((schemaTotalQueries - dbTotalQueries) / dbTotalQueries) * 100).toFixed(1);

  const memoryDiff = (
    parseFloat(poolTestDB.system_after.memory_usage_pct) - parseFloat(poolTestSchema.system_after.memory_usage_pct)
  ).toFixed(1);

  console.log(
    `\nLatency:     Schemas ${latencyImprovement}% ${parseFloat(latencyImprovement) > 0 ? "better" : "worse"}`,
  );
  console.log(
    `Throughput:  Schemas ${throughputImprovement}% ${parseFloat(throughputImprovement) > 0 ? "better" : "worse"}`,
  );
  console.log(`Memory:      Schemas use ${memoryDiff}% ${parseFloat(memoryDiff) > 0 ? "less" : "more"}`);

  if (parseFloat(latencyImprovement) > 20 && parseFloat(throughputImprovement) > 20) {
    console.log("\n✅ RECOMMENDATION: Migrate to schemas - significant improvements!");
  } else if (parseFloat(latencyImprovement) > 10) {
    console.log("\n⚠️  RECOMMENDATION: Schemas show improvement, but test with real production load");
  } else {
    console.log("\n❌ RECOMMENDATION: Schemas don't show significant improvement for your workload");
  }
}

main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});
