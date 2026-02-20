import "dotenv/config";
import { ClientConfig } from "pg";

function parseConnectionString(url: string): ClientConfig {
  const u = new URL(url);
  return {
    host: u.hostname,
    port: u.port ? parseInt(u.port) : 5432,
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
): ClientConfig {
  if (urlEnv) {
    const parsed = parseConnectionString(urlEnv);
    // database из URL можно перебить явным env, если нужно
    return dbEnv ? { ...parsed, database: dbEnv } : parsed;
  }
  return {
    host: hostEnv ?? "localhost",
    port: parseInt(portEnv ?? "5432"),
    user: userEnv ?? "postgres",
    password: passEnv ?? "",
    ...(dbEnv ? { database: dbEnv } : {}),
  };
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
    process.env.TARGET_DATABASE ?? "tenants",
  ),
  dumpDir: process.env.DUMP_DIR ?? "/tmp/pg_migration_dumps",
  stateFile: process.env.STATE_FILE ?? "./migration-state.json",
  concurrency: parseInt(process.env.CONCURRENCY ?? "10"),
  excludeDatabases: ["postgres", "template0", "template1", process.env.TARGET_DATABASE ?? "tenants"],
  filterPrefix: process.env.FILTER_PREFIX ?? null,
  execTimeoutMs: parseInt(process.env.EXEC_TIMEOUT_MS ?? "600000"),
};
