import type { Client, ClientConfig } from "pg";
import { CONFIG } from "./config";
import { withClient } from "./db";
import { log } from "./utils";

export async function tableChecksum(
  client: Client,
  schema: string,
  table: string,
): Promise<string> {
  const { rows: cols } = await client.query<{
    column_name: string;
    data_type: string;
  }>(
    `SELECT column_name, data_type
     FROM information_schema.columns
     WHERE table_schema = $1 AND table_name = $2
     ORDER BY ordinal_position`,
    [schema, table],
  );

  if (cols.length === 0) return "empty";

  const orderBy = cols
    .map((c) => {
      const colExpr =
        c.data_type === "json" || c.data_type === "jsonb"
          ? `"${c.column_name}"::text`
          : `"${c.column_name}"`;
      return `${colExpr} NULLS FIRST`;
    })
    .join(", ");

  const { rows } = await client.query<{ checksum: string }>(
    `SELECT md5(string_agg(row_md5, ',' ORDER BY rn)) AS checksum
     FROM (
       SELECT ROW_NUMBER() OVER (ORDER BY ${orderBy}) AS rn,
              md5(ROW(${cols.map((c) => `"${c.column_name}"`).join(", ")})::text) AS row_md5
       FROM ${schema === "public" ? "public" : `"${schema}"`}."${table}"
     ) sub`,
  );

  return rows[0]?.checksum ?? "null";
}

export async function verifyMigration(dbName: string, sourceOverride?: ClientConfig): Promise<{ ok: boolean; reasons: string[] }> {
  const reasons: string[] = [];
  const src = sourceOverride ?? CONFIG.source;

  await withClient({ ...src, database: dbName }, async (srcClient) => {
    await withClient(CONFIG.target, async (tgtClient) => {
      const { rows: srcTables } = await srcClient.query<{
        table_name: string;
      }>(
        `SELECT table_name FROM information_schema.tables
         WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
         ORDER BY table_name`,
      );
      const { rows: tgtTables } = await tgtClient.query<{
        table_name: string;
      }>(
        `SELECT table_name FROM information_schema.tables
         WHERE table_schema = $1 AND table_type = 'BASE TABLE'
         ORDER BY table_name`,
        [dbName],
      );

      const srcTableNames = srcTables.map((r) => r.table_name);
      const tgtTableNames = new Set(tgtTables.map((r) => r.table_name));

      const missingTables = srcTableNames.filter((t) => !tgtTableNames.has(t));
      if (missingTables.length > 0) {
        reasons.push(`missing tables: ${missingTables.join(", ")}`);
      }

      for (const table of srcTableNames) {
        if (!tgtTableNames.has(table)) continue;

        const { rows: srcCnt } = await srcClient.query<{ cnt: string }>(
          `SELECT COUNT(*)::text AS cnt FROM public."${table}"`,
        );
        const { rows: tgtCnt } = await tgtClient.query<{ cnt: string }>(
          `SELECT COUNT(*)::text AS cnt FROM "${dbName}"."${table}"`,
        );

        const srcCount = srcCnt[0].cnt;
        const tgtCount = tgtCnt[0].cnt;

        if (srcCount !== tgtCount) {
          reasons.push(`${table}: row count mismatch (src: ${srcCount}, tgt: ${tgtCount})`);
          continue;
        }

        const rowCount = parseInt(srcCount, 10);
        const skipAbove = CONFIG.skipChecksumAboveRows;

        if (skipAbove !== undefined && rowCount > skipAbove) {
          log("warn", `  ${table}: checksum skipped (${rowCount} rows > threshold ${skipAbove})`);
          continue;
        }

        const [srcChecksum, tgtChecksum] = await Promise.all([
          tableChecksum(srcClient, "public", table),
          tableChecksum(tgtClient, dbName, table),
        ]);

        if (srcChecksum !== tgtChecksum) {
          reasons.push(`${table}: checksum mismatch (src: ${srcChecksum}, tgt: ${tgtChecksum})`);
        }
      }
    });
  });

  return { ok: reasons.length === 0, reasons };
}
