import { execSync, ExecSyncOptions } from "child_process";
import { Client } from "pg";
import fs from "fs";
import path from "path";
import { CONFIG } from "./config";
import { State, FailedEntry } from "./types";

// ─── helpers ────────────────────────────────────────────────────────────────

function log(level: "info" | "warn" | "error" | "done" | "fail", msg: string): void {
  const prefix: Record<typeof level, string> = {
    info: "[info]",
    warn: "[warn]",
    error: "[error]",
    done: "[done]",
    fail: "[fail]",
  };
  const out = level === "error" || level === "fail" ? process.stderr : process.stdout;
  out.write(`${prefix[level]} ${msg}\n`);
}

/** Атомарная запись файла: пишем во временный, потом rename. */
function atomicWrite(filePath: string, data: string): void {
  const tmp = `${filePath}.tmp`;
  fs.writeFileSync(tmp, data, "utf-8");
  fs.renameSync(tmp, filePath);
}

function loadState(): State {
  if (fs.existsSync(CONFIG.stateFile)) {
    try {
      return JSON.parse(fs.readFileSync(CONFIG.stateFile, "utf-8")) as State;
    } catch {
      log("warn", `state file is corrupted, starting fresh`);
    }
  }
  return {
    completed: [],
    failed: [],
    startedAt: new Date().toISOString(),
    lastUpdated: new Date().toISOString(),
  };
}

function saveState(state: State): void {
  state.lastUpdated = new Date().toISOString();
  atomicWrite(CONFIG.stateFile, JSON.stringify(state, null, 2));
}

function exec(cmd: string, password: string): void {
  const opts: ExecSyncOptions = {
    env: { ...process.env, PGPASSWORD: password },
    stdio: "pipe",
    timeout: CONFIG.execTimeoutMs ?? 10 * 60 * 1000, // 10 min default
  };
  execSync(cmd, opts);
}

// ─── database helpers ────────────────────────────────────────────────────────

async function withClient<T>(config: object, fn: (client: Client) => Promise<T>): Promise<T> {
  const client = new Client(config);
  await client.connect();
  try {
    return await fn(client);
  } finally {
    await client.end();
  }
}

async function ensureTargetDatabase(): Promise<void> {
  await withClient({ ...CONFIG.target, database: "postgres" }, async (client) => {
    const { rows } = await client.query("SELECT 1 FROM pg_database WHERE datname = $1", [CONFIG.target.database]);
    if (rows.length === 0) {
      await client.query(`CREATE DATABASE "${CONFIG.target.database}"`);
      log("info", `created database: ${CONFIG.target.database}`);
    }
  });
}

async function getTenants(): Promise<string[]> {
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

async function createSchema(schemaName: string): Promise<void> {
  await withClient(CONFIG.target, async (client) => {
    await client.query(`CREATE SCHEMA IF NOT EXISTS "${schemaName}"`);
  });
}

// ─── disk space guard ────────────────────────────────────────────────────────

function getFreeBytesOnDir(dir: string): number {
  // df -k returns 1K-blocks; column 4 is available
  const out = execSync(`df -k "${dir}"`, { encoding: "utf-8" });
  const line = out.trim().split("\n")[1];
  const available = parseInt(line.trim().split(/\s+/)[3], 10);
  return available * 1024;
}

function assertDiskSpace(dir: string, minBytes = 512 * 1024 * 1024 /* 512 MB */): void {
  const free = getFreeBytesOnDir(dir);
  if (free < minBytes) {
    throw new Error(
      `Not enough disk space in ${dir}: ${Math.round(free / 1024 / 1024)} MB free, ` +
        `need at least ${Math.round(minBytes / 1024 / 1024)} MB`,
    );
  }
}

// ─── migration ───────────────────────────────────────────────────────────────

async function migrateExtensions(dbName: string): Promise<void> {
  // Получаем список extensions из source БД и устанавливаем их в target
  const extensions = await withClient({ ...CONFIG.source, database: dbName }, async (client) => {
    const { rows } = await client.query<{ extname: string; extversion: string }>(
      `SELECT extname, extversion FROM pg_extension
         WHERE extname != 'plpgsql'  -- plpgsql уже есть в любой БД
         ORDER BY extname`,
    );
    return rows;
  });

  if (extensions.length === 0) return;

  await withClient(CONFIG.target, async (client) => {
    for (const { extname } of extensions) {
      // Если extension не поддерживает установку в произвольную схему — бросаем ошибку.
      // Глобальная установка нам не нужна: тенант должен быть полностью изолирован в своей схеме.
      await client.query(`CREATE EXTENSION IF NOT EXISTS "${extname}" WITH SCHEMA "${dbName}"`);
      log("info", `  extension ${extname} installed into schema "${dbName}"`);
    }
  });
}

async function migrateTenant(dbName: string): Promise<void> {
  const dumpFile = path.join(CONFIG.dumpDir, `${dbName}.dump`);
  const src = CONFIG.source;
  const tgt = CONFIG.target;
  const srcPw = String(src.password ?? "");
  const tgtPw = String(tgt.password ?? "");

  assertDiskSpace(CONFIG.dumpDir);

  try {
    // Extensions ставим до восстановления — они могут быть нужны для типов/функций в дампе
    await migrateExtensions(dbName);

    // Dump в custom binary format — надёжно, быстрее, не требует regex-замен
    exec(
      `pg_dump -Fc ` +
        `-h ${src.host} -p ${src.port} -U ${src.user} ` +
        `-n public ` + // только public schema
        `--no-owner --no-acl ` + // права не переносим, они будут свои
        `-d "${dbName}" ` +
        `-f "${dumpFile}"`,
      srcPw,
    );

    await createSchema(dbName);

    // Восстанавливаем с явным маппингом схемы: public → dbName
    exec(
      `pg_restore -Fc ` +
        `-h ${tgt.host} -p ${tgt.port} -U ${tgt.user} ` +
        `-d "${tgt.database}" ` +
        `--no-owner --no-acl ` +
        `-n public ` + // читаем из public…
        `--schema="${dbName}" ` + // …кладём в схему dbName (pg_restore 16+)
        // для старых версий pg_restore используй --target-schema вместо --schema
        `"${dumpFile}"`,
      tgtPw,
    );
  } finally {
    // Удаляем dump в любом случае — не оставляем чувствительные данные на диске
    if (fs.existsSync(dumpFile)) {
      fs.unlinkSync(dumpFile);
    }
  }
}

// ─── verification ────────────────────────────────────────────────────────────

/**
 * Строим контрольную сумму таблицы через агрегацию md5 по всем строкам.
 *
 * Подход: приводим каждую строку к тексту через ROW()::text, берём md5,
 * затем md5 от конкатенации всех md5 в порядке сортировки по всем колонкам.
 * Это детерминированно при одинаковом порядке строк и данных.
 *
 * Для очень больших таблиц (>10M строк) это может быть медленно —
 * в таких случаях можно ограничиться только COUNT, выставив skipChecksumAbove в CONFIG.
 */
async function tableChecksum(client: Client, schema: string, table: string): Promise<string> {
  // Получаем колонки в детерминированном порядке для ORDER BY
  const { rows: cols } = await client.query<{ column_name: string }>(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = $1 AND table_name = $2
     ORDER BY ordinal_position`,
    [schema, table],
  );

  if (cols.length === 0) return "empty";

  const orderBy = cols.map((c) => `"${c.column_name}" NULLS FIRST`).join(", ");

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

async function verifyMigration(dbName: string): Promise<{ ok: boolean; reasons: string[] }> {
  const reasons: string[] = [];

  await withClient({ ...CONFIG.source, database: dbName }, async (srcClient) => {
    await withClient(CONFIG.target, async (tgtClient) => {
      // 1. Список таблиц
      const { rows: srcTables } = await srcClient.query<{ table_name: string }>(
        `SELECT table_name FROM information_schema.tables
         WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
         ORDER BY table_name`,
      );
      const { rows: tgtTables } = await tgtClient.query<{ table_name: string }>(
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

        // 2. Количество строк
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
          // Контрольную сумму считать бессмысленно если количество строк уже расходится
          continue;
        }

        // 3. Контрольная сумма данных
        //    Пропускаем для очень больших таблиц если задан порог в конфиге
        const rowCount = parseInt(srcCount, 10);
        const skipAbove = (CONFIG as any).skipChecksumAboveRows as number | undefined;

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

// ─── batch runner ────────────────────────────────────────────────────────────

async function runBatch(tenants: string[], state: State): Promise<void> {
  const results = await Promise.allSettled(
    tenants.map(async (db) => {
      await migrateTenant(db);

      const { ok, reasons } = await verifyMigration(db);
      if (!ok) {
        throw new Error(`verification failed: ${reasons.join(" | ")}`);
      }

      return db;
    }),
  );

  for (let i = 0; i < results.length; i++) {
    const db = tenants[i];
    const result = results[i];

    if (result.status === "fulfilled") {
      state.completed.push(db);
      log("done", `${db} (${state.completed.length} total)`);
    } else {
      const message = result.reason instanceof Error ? result.reason.message : String(result.reason);

      const existing = state.failed.find((f: FailedEntry) => f.db === db);
      if (existing) {
        existing.attempts++;
        existing.error = message;
      } else {
        state.failed.push({ db, error: message, attempts: 1 });
      }
      log("fail", `${db}: ${message.slice(0, 200)}`);
    }
  }
}

// ─── main ────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  fs.mkdirSync(CONFIG.dumpDir, { recursive: true });

  await ensureTargetDatabase();

  const state = loadState();
  const allTenants = await getTenants();
  const completed = new Set(state.completed);
  const remaining = allTenants.filter((db) => !completed.has(db));

  log(
    "info",
    `total: ${allTenants.length} | done: ${state.completed.length} | ` +
      `remaining: ${remaining.length} | failed: ${state.failed.length}`,
  );

  if (remaining.length === 0) {
    log("info", "nothing to migrate");
    return;
  }

  const batches: string[][] = [];
  for (let i = 0; i < remaining.length; i += CONFIG.concurrency) {
    batches.push(remaining.slice(i, i + CONFIG.concurrency));
  }

  const startTime = Date.now();
  const initialDone = state.completed.length;

  for (let i = 0; i < batches.length; i++) {
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    const migratedSoFar = state.completed.length - initialDone;
    const pct = Math.round((state.completed.length / allTenants.length) * 100);
    const remainingCount = remaining.length - migratedSoFar;
    const eta = migratedSoFar > 0 ? Math.round((elapsed / migratedSoFar) * remainingCount) : "?";

    log("info", `batch ${i + 1}/${batches.length} | ${pct}% | ${elapsed}s elapsed | ETA ~${eta}s`);

    await runBatch(batches[i], state);
    saveState(state);
  }

  const totalTime = Math.round((Date.now() - startTime) / 1000);

  log("info", `completed: ${state.completed.length} | failed: ${state.failed.length} | time: ${totalTime}s`);

  if (state.failed.length > 0) {
    log("warn", "failed tenants:");
    state.failed.forEach((f: FailedEntry) =>
      log("fail", `  ${f.db} (${f.attempts} attempts): ${f.error.slice(0, 200)}`),
    );
  }

  atomicWrite(
    "./migration-report.json",
    JSON.stringify({ ...state, totalDatabases: allTenants.length, totalTimeSeconds: totalTime }, null, 2),
  );
}

main().catch((err: unknown) => {
  log("error", `fatal: ${err instanceof Error ? (err.stack ?? err.message) : String(err)}`);
  process.exit(1);
});
