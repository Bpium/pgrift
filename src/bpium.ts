import { buildConnectionString, CONFIG } from "./config";
import { log } from "./utils";

/**
 * Sends a PATCH request to the Bpium admin API to update the schema field
 * for a given record.
 *
 * Only runs if BPIUM_API_BASE, BPIUM_LOGIN, BPIUM_PASSWORD, BPIUM_CATALOG_ID,
 * and BPIUM_SCHEMA_NAME are all set in the environment.
 *
 * @param recordId - The Bpium record ID (from db-list.json `id` field)
 * @param db       - Database name (used only for logging)
 */
export async function updateBpiumSchema(recordId: number, db: string): Promise<void> {
  if (!CONFIG.bpium) return;

  const { apiBase, catalogId, login, password, timezoneOffset } = CONFIG.bpium;

  if (String(recordId) === "1" || db === "core") throw new Error("Cannot manipulate with service database!");

  const url =
    `${apiBase}/api/v1/catalogs/${catalogId}/records/${recordId}` + `?timezoneOffset=${timezoneOffset}&skipPrevId=true`;

  const auth = Buffer.from(`${login}:${password}`).toString("base64");

  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Basic ${auth}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      values: {
        $schema: [db],
        $database: [buildConnectionString(CONFIG.target)],
      },
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Bpium API ${res.status}: ${text.slice(0, 200)}`);
  }

  log("info", `bpium schema updated for ${db} (record #${recordId})`);
}
