import { buildConnectionString, CONFIG } from "./config";
import { log } from "./utils";

function buildBpiumUrl(recordId: number): string {
  const { apiBase, catalogId, timezoneOffset } = CONFIG.bpium!;
  return `${apiBase}/api/v1/catalogs/${catalogId}/records/${recordId}?timezoneOffset=${timezoneOffset}&skipPrevId=true`;
}

function buildAuthHeader(): string {
  const { login, password } = CONFIG.bpium!;
  return `Basic ${Buffer.from(`${login}:${password}`).toString("base64")}`;
}

async function bpiumPatch(recordId: number, values: Record<string, unknown>): Promise<void> {
  const res = await fetch(buildBpiumUrl(recordId), {
    method: "PATCH",
    headers: {
      Authorization: buildAuthHeader(),
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ values }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Bpium API ${res.status}: ${text.slice(0, 200)}`);
  }
}

/**
 * Disables the API version for a record before migration starts.
 * Sets $version to [] so the domain becomes temporarily unavailable.
 */
export async function disableBpiumVersion(recordId: number, db: string): Promise<void> {
  if (!CONFIG.bpium) return;

  await bpiumPatch(recordId, { $version: [] });
  log("info", `  [${db}] bpium api version disabled (record #${recordId})`);
}

/**
 * Restores $version from env without touching $schema/$database.
 * Called when migration fails so the tenant isn't left with an empty version.
 */
export async function restoreBpiumVersion(recordId: number, db: string): Promise<void> {
  if (!CONFIG.bpium) return;
  const { apiVersion } = CONFIG.bpium;
  await bpiumPatch(recordId, { $version: apiVersion ? [apiVersion] : [] });
  log("info", `  [${db}] bpium version restored after failure (record #${recordId})`);
}

/**
 * After migration: updates $schema, $database and restores $version.
 */
export async function updateBpiumSchema(recordId: number, db: string): Promise<void> {
  if (!CONFIG.bpium) return;

  if (String(recordId) === "1" || db === "core") throw new Error("Cannot manipulate with service database!");

  const { apiVersion } = CONFIG.bpium;

  const connectionString = buildConnectionString(CONFIG.target);
  const connectionStringWithSsl = connectionString.endsWith("?ssl=true")
    ? connectionString
    : connectionString + "?ssl=true";


  const values: Record<string, unknown> = {
    $schema: db,
    $database: connectionStringWithSsl,
    $version: apiVersion ? [apiVersion] : [],
  };

  await bpiumPatch(recordId, values);
  log("info", `  [${db}] bpium schema updated (record #${recordId})`);
}
