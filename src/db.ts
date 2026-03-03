import fs from "node:fs";
import { Client } from "pg";
import { CONFIG, parseConnectionString } from "./config";
import type { TenantEntry } from "./types";
import { log } from "./utils";

export async function withClient<T>(
  config: object,
  fn: (client: Client) => Promise<T>,
): Promise<T> {
  const sslConfig = CONFIG.ssl ? { ssl: { rejectUnauthorized: false } } : {};
  const client = new Client({ ...sslConfig, ...config });
  await client.connect();
  try {
    return await fn(client);
  } finally {
    await client.end();
  }
}

export async function ensureTargetDatabase(): Promise<void> {
  await withClient({ ...CONFIG.target, database: "postgres" }, async (client) => {
    const { rows } = await client.query("SELECT 1 FROM pg_database WHERE datname = $1", [
      CONFIG.target.database,
    ]);
    if (rows.length === 0) {
      throw new Error(
        `Target database "${CONFIG.target.database}" does not exist. Please create it manually before running migration.`,
      );
    }
    log("info", `target database "${CONFIG.target.database}" exists, proceeding...`);
  });
}

export function getTenantsFromFile(filePath: string): TenantEntry[] {
  const content = fs.readFileSync(filePath, "utf-8");
  const entries: unknown = JSON.parse(content);

  if (!Array.isArray(entries)) {
    throw new Error(`DB_LIST_FILE must contain a JSON array of connection strings`);
  }

  return entries.map((entry, i) => {
    if (typeof entry !== "string") {
      throw new Error(`DB_LIST_FILE entry at index ${i} must be a connection string, got ${typeof entry}`);
    }
    const source = parseConnectionString(entry);
    if (!source.database) {
      throw new Error(`Connection string at index ${i} must include a database name: ${entry}`);
    }
    return { db: source.database, source };
  });
}

export async function getTenants(): Promise<string[]> {
  return withClient({ ...CONFIG.source, database: "postgres" }, async (client) => {
    const placeholders = CONFIG.excludeDatabases.map((_, i) => `$${i + 1}`).join(", ");
    const { rows } = await client.query<{ datname: string }>(
      `SELECT datname FROM pg_database
       WHERE datname NOT IN (${placeholders})
         AND datname NOT LIKE 'pg_%'
       ORDER BY datname`,
      CONFIG.excludeDatabases,
    );
    let tenants = rows.map((r) => r.datname);
    if (CONFIG.filterPrefix) {
      tenants = tenants.filter((db) => db.startsWith(CONFIG.filterPrefix!));
    }
    return tenants;
  });
}
