"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.tableChecksum = tableChecksum;
exports.verifyMigration = verifyMigration;
const config_1 = require("./config");
const db_1 = require("./db");
const utils_1 = require("./utils");
async function tableChecksum(client, schema, table) {
    const { rows: cols } = await client.query(`SELECT column_name, data_type
     FROM information_schema.columns
     WHERE table_schema = $1 AND table_name = $2
     ORDER BY ordinal_position`, [schema, table]);
    if (cols.length === 0)
        return "empty";
    const orderBy = cols
        .map((c) => {
        const colExpr = c.data_type === "json" || c.data_type === "jsonb"
            ? `"${c.column_name}"::text`
            : `"${c.column_name}"`;
        return `${colExpr} NULLS FIRST`;
    })
        .join(", ");
    const { rows } = await client.query(`SELECT md5(string_agg(row_md5, ',' ORDER BY rn)) AS checksum
     FROM (
       SELECT ROW_NUMBER() OVER (ORDER BY ${orderBy}) AS rn,
              md5(ROW(${cols.map((c) => `"${c.column_name}"`).join(", ")})::text) AS row_md5
       FROM ${schema === "public" ? "public" : `"${schema}"`}."${table}"
     ) sub`);
    return rows[0]?.checksum ?? "null";
}
async function verifyMigration(dbName) {
    const reasons = [];
    await (0, db_1.withClient)({ ...config_1.CONFIG.source, database: dbName }, async (srcClient) => {
        await (0, db_1.withClient)(config_1.CONFIG.target, async (tgtClient) => {
            const { rows: srcTables } = await srcClient.query(`SELECT table_name FROM information_schema.tables
         WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
         ORDER BY table_name`);
            const { rows: tgtTables } = await tgtClient.query(`SELECT table_name FROM information_schema.tables
         WHERE table_schema = $1 AND table_type = 'BASE TABLE'
         ORDER BY table_name`, [dbName]);
            const srcTableNames = srcTables.map((r) => r.table_name);
            const tgtTableNames = new Set(tgtTables.map((r) => r.table_name));
            const missingTables = srcTableNames.filter((t) => !tgtTableNames.has(t));
            if (missingTables.length > 0) {
                reasons.push(`missing tables: ${missingTables.join(", ")}`);
            }
            for (const table of srcTableNames) {
                if (!tgtTableNames.has(table))
                    continue;
                const { rows: srcCnt } = await srcClient.query(`SELECT COUNT(*)::text AS cnt FROM public."${table}"`);
                const { rows: tgtCnt } = await tgtClient.query(`SELECT COUNT(*)::text AS cnt FROM "${dbName}"."${table}"`);
                const srcCount = srcCnt[0].cnt;
                const tgtCount = tgtCnt[0].cnt;
                if (srcCount !== tgtCount) {
                    reasons.push(`${table}: row count mismatch (src: ${srcCount}, tgt: ${tgtCount})`);
                    continue;
                }
                const rowCount = parseInt(srcCount, 10);
                const skipAbove = config_1.CONFIG.skipChecksumAboveRows;
                if (skipAbove !== undefined && rowCount > skipAbove) {
                    (0, utils_1.log)("warn", `  ${table}: checksum skipped (${rowCount} rows > threshold ${skipAbove})`);
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
