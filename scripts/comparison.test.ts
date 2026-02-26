import fs from "node:fs";
import os from "node:os";
import pMap from "p-map";
import { Client } from "pg";
import { buildConnectionString, CONFIG } from "../src/config";

const tenants: string[] = JSON.parse(fs.readFileSync(CONFIG.dbListPath, "utf8"));

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

interface HardwareInfo {
  platform: string;
  arch: string;
  hostname: string;
  node_version: string;
  cpu_cores: number;
  cpu_model: string;
  memory_total_gb: number;
  memory_free_gb: number;
  load_avg_1m: number;
  load_avg_5m: number;
  load_avg_15m: number;
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

interface ResourceSnapshot {
  timestamp: number;
  cpu_usage_pct: number;
  memory_used_gb: number;
  memory_usage_pct: number;
  load_average: number[];
  postgres_connections?: number;
  postgres_active_queries?: number;
}

interface ResourceMonitoring {
  snapshots: ResourceSnapshot[];
  duration_sec: number;
  avg_cpu: number;
  max_cpu: number;
  avg_memory_pct: number;
  max_memory_pct: number;
  avg_load_1m: number;
  max_load_1m: number;
  max_connections: number;
  max_active_queries: number;
}

interface PostgresIOStats {
  shared_buffers_hit_pct: number;
  disk_blocks_read: number;
  disk_blocks_hit: number;
  temp_files: number;
  temp_bytes: number;
}

class ResourceMonitor {
  private snapshots: ResourceSnapshot[] = [];
  private intervalId?: NodeJS.Timeout;
  private connectionString: string;

  constructor(connectionString: string) {
    this.connectionString = connectionString;
  }

  async captureSnapshot(): Promise<ResourceSnapshot> {
    const cpus = os.cpus();
    const totalMem = os.totalmem();
    const freeMem = os.freemem();
    const usedMem = totalMem - freeMem;

    const cpuUsage =
      cpus.reduce((acc, cpu) => {
        const total = Object.values(cpu.times).reduce((a, b) => a + b, 0);
        const idle = cpu.times.idle;
        return acc + ((total - idle) / total) * 100;
      }, 0) / cpus.length;

    const snapshot: ResourceSnapshot = {
      timestamp: Date.now(),
      cpu_usage_pct: parseFloat(cpuUsage.toFixed(2)),
      memory_used_gb: parseFloat((usedMem / 1024 / 1024 / 1024).toFixed(2)),
      memory_usage_pct: parseFloat(((usedMem / totalMem) * 100).toFixed(2)),
      load_average: os.loadavg(),
    };

    try {
      const client = new Client({ connectionString: this.connectionString });
      await client.connect();

      const { rows } = await client.query(`
        SELECT 
          (SELECT count(*) FROM pg_stat_activity) as total_connections,
          (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') as active_queries
      `);

      snapshot.postgres_connections = parseInt(rows[0].total_connections, 10);
      snapshot.postgres_active_queries = parseInt(rows[0].active_queries, 10);

      await client.end();
    } catch {}

    return snapshot;
  }

  start(intervalMs: number = 1000): void {
    this.intervalId = setInterval(async () => {
      const snapshot = await this.captureSnapshot();
      this.snapshots.push(snapshot);
    }, intervalMs);
  }

  stop(): ResourceMonitoring {
    if (this.intervalId) clearInterval(this.intervalId);

    const duration =
      this.snapshots.length > 0
        ? (this.snapshots[this.snapshots.length - 1].timestamp - this.snapshots[0].timestamp) / 1000
        : 0;

    const avgCpu =
      this.snapshots.reduce((sum, s) => sum + s.cpu_usage_pct, 0) / this.snapshots.length;

    const maxCpu = Math.max(...this.snapshots.map((s) => s.cpu_usage_pct));

    const avgMemPct =
      this.snapshots.reduce((sum, s) => sum + s.memory_usage_pct, 0) / this.snapshots.length;

    const maxMemPct = Math.max(...this.snapshots.map((s) => s.memory_usage_pct));

    const load1 = this.snapshots.map((s) => s.load_average[0] ?? 0);
    const avgLoad1 = load1.length ? load1.reduce((a, b) => a + b, 0) / load1.length : 0;
    const maxLoad1 = load1.length ? Math.max(...load1) : 0;

    const maxConn = Math.max(...this.snapshots.map((s) => s.postgres_connections ?? 0), 0);
    const maxActive = Math.max(...this.snapshots.map((s) => s.postgres_active_queries ?? 0), 0);

    return {
      snapshots: this.snapshots,
      duration_sec: duration,
      avg_cpu: parseFloat(avgCpu.toFixed(2)),
      max_cpu: parseFloat(maxCpu.toFixed(2)),
      avg_memory_pct: parseFloat(avgMemPct.toFixed(2)),
      max_memory_pct: parseFloat(maxMemPct.toFixed(2)),
      avg_load_1m: parseFloat(avgLoad1.toFixed(2)),
      max_load_1m: parseFloat(maxLoad1.toFixed(2)),
      max_connections: maxConn,
      max_active_queries: maxActive,
    };
  }
}

async function getPostgresIOStats(connectionString: string): Promise<PostgresIOStats | null> {
  try {
    const client = new Client({ connectionString });
    await client.connect();

    const { rows } = await client.query(`
      SELECT 
        ROUND(100.0 * sum(blks_hit) / NULLIF(sum(blks_hit) + sum(blks_read), 0), 2) as hit_pct,
        sum(blks_read) as disk_reads,
        sum(blks_hit) as cache_hits,
        sum(temp_files) as temp_files,
        sum(temp_bytes) as temp_bytes
      FROM pg_stat_database
      WHERE datname = current_database()
    `);

    await client.end();

    return {
      shared_buffers_hit_pct: parseFloat(rows[0].hit_pct || "0"),
      disk_blocks_read: parseInt(rows[0].disk_reads || "0", 10),
      disk_blocks_hit: parseInt(rows[0].cache_hits || "0", 10),
      temp_files: parseInt(rows[0].temp_files || "0", 10),
      temp_bytes: parseInt(rows[0].temp_bytes || "0", 10),
    };
  } catch {
    return null;
  }
}

/** Sum blks_read/blks_hit etc. across multiple databases. Connect to any DB (e.g. postgres) to read pg_stat_database. */
async function getPostgresIOStatsForDatabases(
  dbNames: string[],
  connectionString: string = buildConnectionString(CONFIG.source, "postgres"),
): Promise<PostgresIOStats | null> {
  if (dbNames.length === 0) return null;
  try {
    const client = new Client({ connectionString });
    await client.connect();

    const { rows } = await client.query(
      `
      SELECT 
        ROUND(100.0 * sum(blks_hit) / NULLIF(sum(blks_hit) + sum(blks_read), 0), 2) as hit_pct,
        sum(blks_read)::bigint as disk_reads,
        sum(blks_hit)::bigint as cache_hits,
        sum(temp_files)::bigint as temp_files,
        sum(temp_bytes)::bigint as temp_bytes
      FROM pg_stat_database
      WHERE datname = ANY($1::text[])
      `,
      [dbNames],
    );

    await client.end();

    return {
      shared_buffers_hit_pct: parseFloat(rows[0]?.hit_pct || "0"),
      disk_blocks_read: parseInt(rows[0]?.disk_reads || "0", 10),
      disk_blocks_hit: parseInt(rows[0]?.cache_hits || "0", 10),
      temp_files: parseInt(rows[0]?.temp_files || "0", 10),
      temp_bytes: parseInt(rows[0]?.temp_bytes || "0", 10),
    };
  } catch {
    return null;
  }
}

// ============================================
// VALIDATION
// ============================================

async function getExistingSchemas(): Promise<Set<string>> {
  try {
    const client = new Client({
      connectionString: buildConnectionString(CONFIG.target),
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
    console.error("Failed to get existing schemas:", err);
    return new Set();
  }
}

async function getExistingDatabases(): Promise<Set<string>> {
  try {
    const client = new Client({
      connectionString: buildConnectionString(CONFIG.source, "postgres"),
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

// ============================================
// HARDWARE INFO
// ============================================

function getHardwareInfo(): HardwareInfo {
  const load = os.loadavg();
  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const cpus = os.cpus();
  const model = cpus[0]?.model ?? "unknown";
  return {
    platform: os.platform(),
    arch: os.arch(),
    hostname: os.hostname(),
    node_version: process.version,
    cpu_cores: cpus.length,
    cpu_model: model,
    memory_total_gb: parseFloat((totalMem / 1024 / 1024 / 1024).toFixed(2)),
    memory_free_gb: parseFloat((freeMem / 1024 / 1024 / 1024).toFixed(2)),
    load_avg_1m: load[0],
    load_avg_5m: load[1],
    load_avg_15m: load[2],
  };
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

async function _getPostgresStats(connectionString: string): Promise<PostgresStats> {
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
// COMPACT LOG FORMATTERS
// ============================================

const LINE = "─";
const W = 58;

function section(title: string): void {
  console.log(`\n${LINE.repeat(2)} ${title} ${LINE.repeat(Math.max(0, W - title.length - 4))}`);
}

function line(key: string, value: string | number): void {
  console.log(`  ${key.padEnd(22)} ${value}`);
}

function printHardwareInfo(hw: HardwareInfo): void {
  section("HARDWARE");
  line("Platform", `${hw.platform} / ${hw.arch}`);
  line("Host", hw.hostname);
  line("Node", hw.node_version);
  line("CPU", `${hw.cpu_cores} cores, ${hw.cpu_model}`);
  line("Memory", `${hw.memory_total_gb} GB total, ${hw.memory_free_gb} GB free`);
  line(
    "Load average",
    `${hw.load_avg_1m.toFixed(2)} / ${hw.load_avg_5m.toFixed(2)} / ${hw.load_avg_15m.toFixed(2)} (1/5/15 min)`,
  );
}

function printBaselineCompact(stats: SystemStats): void {
  section("BASELINE (before tests)");
  line("CPU", `${stats.cpu_count} cores, usage: ${stats.cpu_usage.slice(0, 4).join("% ")}% ...`);
  line(
    "Memory",
    `${stats.memory_used_gb}/${stats.memory_total_gb} GB (${stats.memory_usage_pct}%)`,
  );
}

function printResourceBlock(
  label: string,
  r: ResourceMonitoring,
  pgIO: { before?: PostgresIOStats | null; after?: PostgresIOStats | null },
): void {
  console.log(`\n  [ ${label} ]`);
  line("CPU (avg/max)", `${r.avg_cpu}% / ${r.max_cpu}%`);
  line("Memory (avg/max)", `${r.avg_memory_pct}% / ${r.max_memory_pct}%`);
  line("Load 1m (avg/max)", `${r.avg_load_1m} / ${r.max_load_1m}`);
  line("PG connections (max)", r.max_connections);
  line("PG active queries (max)", r.max_active_queries);
  if (pgIO.after) {
    const diskDelta = pgIO.before
      ? pgIO.after.disk_blocks_read - pgIO.before.disk_blocks_read
      : pgIO.after.disk_blocks_read;
    line("Cache hit", `${pgIO.after.shared_buffers_hit_pct}%`);
    line("Disk reads (delta)", diskDelta);
  }
}

function printQueryPerf(
  label: string,
  totalQueries: number,
  avgLatency: number,
  successCount: number,
): void {
  console.log(`\n  [ ${label} ]`);
  line("Total queries", totalQueries);
  line("Successful workers", successCount);
  line("Avg latency", `${avgLatency.toFixed(2)} ms`);
}

// ============================================
// HEAVY LOAD TESTS
// ============================================

async function heavyLoadTest_SeparateDB(
  dbName: string,
  duration: number = 10000,
): Promise<LoadTestResult> {
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
    connectionString: buildConnectionString(CONFIG.source, dbName),
  });
  const table = CONFIG.benchTable;
  const idCol = CONFIG.benchIdColumn;
  const nameCol = CONFIG.benchNameColumn;

  try {
    await client.connect();

    while (Date.now() - startTime < duration) {
      const queryStart = performance.now();

      try {
        const queryType = Math.random();

        if (queryType < 0.6) {
          const randomId = Math.floor(Math.random() * 10000) + 1;
          await client.query(`SELECT * FROM "${table}" WHERE "${idCol}" = $1`, [randomId]);
        } else if (queryType < 0.9) {
          await client.query(`SELECT * FROM "${table}" WHERE "${idCol}" > $1 LIMIT 10`, [
            Math.floor(Math.random() * 9000),
          ]);
        } else {
          await client.query(`UPDATE "${table}" SET "${nameCol}" = $1 WHERE "${idCol}" = $2`, [
            `test_${Date.now()}`,
            Math.floor(Math.random() * 10000) + 1,
          ]);
        }

        const latency = performance.now() - queryStart;
        latencies.push(latency);
        results.queries_executed++;
      } catch (_err) {
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

async function heavyLoadTest_Schema(
  schemaName: string,
  duration: number = 10000,
): Promise<LoadTestResult> {
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
    connectionString: buildConnectionString(CONFIG.target),
  });
  const table = CONFIG.benchTable;
  const idCol = CONFIG.benchIdColumn;
  const nameCol = CONFIG.benchNameColumn;

  try {
    await client.connect();

    while (Date.now() - startTime < duration) {
      const queryStart = performance.now();

      try {
        const queryType = Math.random();

        if (queryType < 0.6) {
          const randomId = Math.floor(Math.random() * 10000) + 1;
          await client.query(`SELECT * FROM "${schemaName}"."${table}" WHERE "${idCol}" = $1`, [
            randomId,
          ]);
        } else if (queryType < 0.9) {
          await client.query(
            `SELECT * FROM "${schemaName}"."${table}" WHERE "${idCol}" > $1 LIMIT 10`,
            [Math.floor(Math.random() * 9000)],
          );
        } else {
          await client.query(
            `UPDATE "${schemaName}"."${table}" SET "${nameCol}" = $1 WHERE "${idCol}" = $2`,
            [`test_${Date.now()}`, Math.floor(Math.random() * 10000) + 1],
          );
        }

        const latency = performance.now() - queryStart;
        latencies.push(latency);
        results.queries_executed++;
      } catch (_err) {
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
// MAIN
// ============================================

async function main(): Promise<void> {
  const width = 60;
  console.log(`\n${"═".repeat(width)}`);
  console.log("  HEAVY LOAD: Separate DBs vs Schemas + hardware monitoring");
  console.log("═".repeat(width));

  const existingDatabases = await getExistingDatabases();
  const existingSchemas = await getExistingSchemas();
  const validDatabases = tenants.filter((db) => existingDatabases.has(db));
  const validSchemas = tenants.filter((schema) => existingSchemas.has(schema));

  if (validDatabases.length === 0 || validSchemas.length === 0) {
    console.error("\nNot enough databases or schemas to run the test.");
    process.exit(1);
  }

  const testDatabases = validDatabases.slice(0, 900);
  const testSchemas = validSchemas.slice(0, 900);
  console.log(
    `\nValid: ${validDatabases.length}/${tenants.length} DBs, ${validSchemas.length}/${tenants.length} schemas. Running: ${testDatabases.length} DBs, ${testSchemas.length} schemas.`,
  );

  const hardware = getHardwareInfo();
  printHardwareInfo(hardware);
  const baseline = getSystemStats();
  printBaselineCompact(baseline);

  section("TEST: SEPARATE DATABASES");
  console.log("  Running load...");
  const pgIO_DB_before = await getPostgresIOStatsForDatabases(testDatabases);
  const dbTest = await runWithMonitoring(
    buildConnectionString(CONFIG.source, testDatabases[0]),
    () => pMap(testDatabases, (db) => heavyLoadTest_SeparateDB(db, 10000), { concurrency: 20 }),
  );
  const pgIO_DB_after = await getPostgresIOStatsForDatabases(testDatabases);
  const loadTestDB = dbTest.result;
  const resourcesDB = dbTest.resources;
  const pgIO_DB = { before: pgIO_DB_before, after: pgIO_DB_after };

  section("TEST: SCHEMAS");
  console.log("  Running load...");
  const schemaTest = await runWithMonitoring(buildConnectionString(CONFIG.target), () =>
    pMap(testSchemas, (schema) => heavyLoadTest_Schema(schema, 10000), { concurrency: 20 }),
  );

  const loadTestSchema = schemaTest.result;
  const resourcesSchema = schemaTest.resources;
  const pgIO_Schema = schemaTest.postgres_io;

  // ============================================================
  // ====================== ANALYTICS ===========================
  // ============================================================

  const dbSuccess = loadTestDB.filter((r: LoadTestResult) => !r.error && r.queries_executed > 0);
  const schemaSuccess = loadTestSchema.filter(
    (r: LoadTestResult) => !r.error && r.queries_executed > 0,
  );
  const dbTotalQueries = dbSuccess.reduce((sum, r) => sum + r.queries_executed, 0);
  const schemaTotalQueries = schemaSuccess.reduce((sum, r) => sum + r.queries_executed, 0);
  const dbAvgLatency = dbSuccess.length
    ? dbSuccess.reduce((sum, r) => sum + r.avg_latency, 0) / dbSuccess.length
    : 0;
  const schemaAvgLatency = schemaSuccess.length
    ? schemaSuccess.reduce((sum, r) => sum + r.avg_latency, 0) / schemaSuccess.length
    : 0;

  section("RESULTS: QUERY PERFORMANCE");
  printQueryPerf("Separate DBs", dbTotalQueries, dbAvgLatency, dbSuccess.length);
  printQueryPerf("Schemas", schemaTotalQueries, schemaAvgLatency, schemaSuccess.length);

  section("RESULTS: HARDWARE + POSTGRES LOAD");
  printResourceBlock("Separate DBs", resourcesDB, pgIO_DB);
  printResourceBlock("Schemas", resourcesSchema, pgIO_Schema);

  section("VERDICT (Schemas vs Separate DBs)");
  const latencyImprovement = dbAvgLatency
    ? (((dbAvgLatency - schemaAvgLatency) / dbAvgLatency) * 100).toFixed(1)
    : "0";
  const throughputImprovement = dbTotalQueries
    ? (((schemaTotalQueries - dbTotalQueries) / dbTotalQueries) * 100).toFixed(1)
    : "0";
  const cpuImprovement = resourcesDB.avg_cpu
    ? (((resourcesDB.avg_cpu - resourcesSchema.avg_cpu) / resourcesDB.avg_cpu) * 100).toFixed(1)
    : "0";
  const memoryImprovement = resourcesDB.avg_memory_pct
    ? (
        ((resourcesDB.avg_memory_pct - resourcesSchema.avg_memory_pct) /
          resourcesDB.avg_memory_pct) *
        100
      ).toFixed(1)
    : "0";
  line(
    "Latency",
    `Schemas ${latencyImprovement}% ${parseFloat(latencyImprovement) > 0 ? "better" : "worse"}`,
  );
  line(
    "Throughput",
    `Schemas ${throughputImprovement}% ${parseFloat(throughputImprovement) > 0 ? "better" : "worse"}`,
  );
  line("CPU", `Schemas ${cpuImprovement}% ${parseFloat(cpuImprovement) > 0 ? "lower" : "higher"}`);
  line(
    "Memory",
    `Schemas ${memoryImprovement}% ${parseFloat(memoryImprovement) > 0 ? "lower" : "higher"}`,
  );

  const allResults = {
    hardware,
    baseline,
    separate_databases: dbTest,
    schemas: schemaTest,
  };
  fs.writeFileSync("./heavy-load-results.json", JSON.stringify(allResults, null, 2));
  console.log("\nResults saved to heavy-load-results.json\n");
}

async function runWithMonitoring(connectionString: string, loadFn: () => Promise<any>) {
  const monitor = new ResourceMonitor(connectionString);
  monitor.start(1000);

  const pgIOBefore = await getPostgresIOStats(connectionString);

  const result = await loadFn();

  const resources = monitor.stop();
  const pgIOAfter = await getPostgresIOStats(connectionString);

  return {
    result,
    resources,
    postgres_io: {
      before: pgIOBefore,
      after: pgIOAfter,
    },
  };
}

main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});
