import { Client } from "pg";
import { CONFIG } from "./config";
import { log } from "./utils";

export async function withClient<T>(
  config: object,
  fn: (client: Client) => Promise<T>,
): Promise<T> {
  const client = new Client(config);
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
      await client.query(`CREATE DATABASE "${CONFIG.target.database}"`);
      log("info", `created database: ${CONFIG.target.database}`);
    }
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
