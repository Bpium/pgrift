import "dotenv/config";
import type { ClientConfig } from "pg";

export function parseConnectionString(url: string): ClientConfig {
  const u = new URL(url);
  return {
    host: u.hostname,
    port: u.port ? parseInt(u.port, 10) : 5432,
    user: u.username || undefined,
    password: u.password ? decodeURIComponent(u.password) : undefined,
    database: u.pathname.slice(1) || undefined,
  };
}

function resolveConfig(
  urlEnv: string | undefined,
  hostEnv: string | undefined,
  portEnv: string | undefined,
  userEnv: string | undefined,
  passEnv: string | undefined,
  dbEnv?: string | undefined,
  requireDatabase: boolean = false,
): ClientConfig {
  if (urlEnv) {
    const parsed = parseConnectionString(urlEnv);
    const finalConfig = dbEnv ? { ...parsed, database: dbEnv } : parsed;

    if (requireDatabase && !finalConfig.database) {
      throw new Error(`Database name is required(IN TARGET_URL) in connection string: ${urlEnv}`);
    }

    return finalConfig;
  }

  const cfg: ClientConfig = {
    host: hostEnv ?? "localhost",
    port: parseInt(portEnv ?? "5432", 10),
    user: userEnv ?? "postgres",
    password: passEnv ?? "",
    ...(dbEnv ? { database: dbEnv } : {}),
  };

  if (requireDatabase && !cfg.database) {
    throw new Error(`Database name must be provided via environment variables`);
  }

  return cfg;
}

/** Build a connection URL from config and optional database name. */
export function buildConnectionString(cfg: ClientConfig, database?: string): string {
  const db = database ?? cfg.database ?? "postgres";
  const user = cfg.user ?? "postgres";
  const pwRaw = typeof cfg.password === "string" ? cfg.password : "";
  const pw = pwRaw ? `:${encodeURIComponent(pwRaw)}` : "";
  const port = cfg.port ?? 5432;
  return `postgresql://${user}${pw}@${cfg.host}:${port}/${db}`;
}

export const CONFIG = {
  source: resolveConfig(
    process.env.SOURCE_URL,
    process.env.SOURCE_HOST,
    process.env.SOURCE_PORT,
    process.env.SOURCE_USER,
    process.env.SOURCE_PASSWORD,
  ),
  target: resolveConfig(
    process.env.TARGET_URL,
    process.env.TARGET_HOST,
    process.env.TARGET_PORT,
    process.env.TARGET_USER,
    process.env.TARGET_PASSWORD,
    process.env.TARGET_DATABASE,
    true,
  ),
  ssl: process.env.SSL === "true",
  schemaRenameStrategy: (process.env.SCHEMA_RENAME_STRATEGY ?? "rewrite") as "rewrite" | "rename",
  sourceReadonly: process.env.SOURCE_READONLY === "true",
  dryRun: process.env.DRY_RUN === "true",
  maxRetries: parseInt(process.env.MAX_RETRIES ?? "3", 10),
  dbListFile: process.env.DB_LIST_FILE ?? null,
  sourceDiscoveryDatabase: process.env.SOURCE_DISCOVERY_DATABASE ?? null,
  dumpDir: process.env.DUMP_DIR ?? "/tmp/pg_migration_dumps",
  stateFile: process.env.STATE_FILE ?? "./migration-state.json",
  concurrency: parseInt(process.env.CONCURRENCY ?? "10", 10),
  excludeDatabases: ["postgres", "template0", "template1", process.env.TARGET_DATABASE ?? "tenants"],
  filterPrefix: process.env.FILTER_PREFIX ?? null,
  execTimeoutMs: process.env.EXEC_TIMEOUT_MS ? parseInt(process.env.EXEC_TIMEOUT_MS, 10) : 0,
  skipChecksumAboveRows: process.env.SKIP_CHECKSUM_ABOVE_ROWS
    ? parseInt(process.env.SKIP_CHECKSUM_ABOVE_ROWS, 10)
    : undefined,

  /** Path to JSON array of DB/schema names (benchmark scripts). Default: ./scripts/db-list.json */
  dbListPath: process.env.DB_LIST_PATH ?? "./scripts/db-list.json",
  /** Table name used by benchmark scripts (e.g. comparison, manyBases, manySchemas). Default: users_data */
  benchTable: process.env.BENCH_TABLE ?? "users_data",
  /** Column used as synthetic ID in benchmarks. Default: dbId */
  benchIdColumn: process.env.BENCH_ID_COLUMN ?? "dbId",
  /** Column used as name field in benchmarks. Default: name */
  benchNameColumn: process.env.BENCH_NAME_COLUMN ?? "name",

  /** createRandomDb script: number of DBs to create. Default: 1000 */
  createDbCount: parseInt(process.env.CREATE_DB_COUNT ?? "1000", 10),
  /** createRandomDb script: DB name prefix. Default: bench_db_ */
  createDbPrefix: process.env.CREATE_DB_PREFIX ?? "bench_db_",
  /** createRandomDb script: path to project that runs postinstall-dev for each DB. Optional. */
  createDbProjectPath: process.env.CREATE_DB_PROJECT_PATH ?? null,

  /**
   * Bpium admin API — optional, only active when BPIUM_API_BASE is set.
   * After each successful migration the script will PATCH the record's $schema and $database fields.
   * $schema is set to the tenant's dbName (= schema name in the target DB).
   * $database is set to the target database name (from TARGET_URL).
   *
   * Required env vars:
   *   BPIUM_API_BASE      — e.g. https://admin.bpium.ru
   *   BPIUM_CATALOG_ID    — catalog segment in the URL, e.g. $companies
   *   BPIUM_LOGIN         — Basic auth login
   *   BPIUM_PASSWORD      — Basic auth password
   *
   * Optional:
   *   BPIUM_TIMEZONE_OFFSET — default 180
   */
  bpium: process.env.BPIUM_API_BASE
    ? {
        apiBase: process.env.BPIUM_API_BASE,
        catalogId: process.env.BPIUM_CATALOG_ID ?? "$companies",
        login: process.env.BPIUM_LOGIN ?? "",
        password: process.env.BPIUM_PASSWORD ?? "",
        timezoneOffset: parseInt(process.env.BPIUM_TIMEZONE_OFFSET ?? "180", 10),
      }
    : null,
};
