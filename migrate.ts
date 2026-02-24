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

// ─── migration (новый подход через промежуточную БД) ────────────────────────

async function migrateTenant(dbName: string): Promise<void> {
  const dumpFile = path.join(CONFIG.dumpDir, `${dbName}.dump`);
  const finalDumpFile = path.join(CONFIG.dumpDir, `${dbName}.final.dump`);
  const tempDbName = `_temp_migration_${dbName}`;

  const src = CONFIG.source;
  const tgt = CONFIG.target;

  const srcPw = String(src.password ?? "");
  const tgtPw = String(tgt.password ?? "");

  assertDiskSpace(CONFIG.dumpDir);

  try {
    // ─────────────────────────────
    // 1️⃣ Дамп исходной БД
    // ─────────────────────────────
    log("info", `  [${dbName}] dumping source database...`);
    exec(
      [
        "pg_dump",
        `-h "${src.host}"`,
        `-p ${src.port}`,
        `-U "${src.user}"`,
        `-d "${dbName}"`,
        `--no-owner`,
        `--no-acl`,
        `-f "${dumpFile}"`,
      ].join(" "),
      srcPw,
    );

    // ─────────────────────────────
    // 2️⃣ Создаём временную БД на target сервере
    // ─────────────────────────────
    log("info", `  [${dbName}] creating temp database...`);
    await withClient({ ...tgt, database: "postgres" }, async (client) => {
      await client.query(`DROP DATABASE IF EXISTS "${tempDbName}"`);
      await client.query(`CREATE DATABASE "${tempDbName}"`);
    });

    // ─────────────────────────────
    // 3️⃣ Восстанавливаем во временную БД
    // ─────────────────────────────
    log("info", `  [${dbName}] restoring to temp database...`);
    exec(
      [
        "psql",
        `-h "${tgt.host}"`,
        `-p ${tgt.port}`,
        `-U "${tgt.user}"`,
        `-d "${tempDbName}"`,
        `-v ON_ERROR_STOP=0`, // Игнорируем ошибки при восстановлении
        `-f "${dumpFile}"`,
      ].join(" "),
      tgtPw,
    );

    // ─────────────────────────────
    // 4️⃣ Переименовываем схему public в имя тенанта
    // ─────────────────────────────
    log("info", `  [${dbName}] renaming schema...`);
    await withClient({ ...tgt, database: tempDbName }, async (client) => {
      await client.query(`ALTER SCHEMA public RENAME TO "${dbName}"`);
      // Создаём новую пустую схему public чтобы pg_dump работал корректно
      await client.query(`CREATE SCHEMA public`);
    });

    // ─────────────────────────────
    // 5️⃣ Дампим переименованную схему
    // ─────────────────────────────
    log("info", `  [${dbName}] dumping renamed schema...`);
    exec(
      [
        "pg_dump",
        `-h "${tgt.host}"`,
        `-p ${tgt.port}`,
        `-U "${tgt.user}"`,
        `-d "${tempDbName}"`,
        `-n "${dbName}"`, // Дампим именно переименованную схему
        `--no-owner`,
        `--no-acl`,
        `-f "${finalDumpFile}"`,
      ].join(" "),
      tgtPw,
    );

    // ─────────────────────────────
    // 6️⃣ Удаляем временную БД
    // ─────────────────────────────
    log("info", `  [${dbName}] dropping temp database...`);
    await withClient({ ...tgt, database: "postgres" }, async (client) => {
      // Принудительно закрываем все соединения к временной БД
      await client.query(`
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = '${tempDbName}' AND pid <> pg_backend_pid()
      `);
      await client.query(`DROP DATABASE IF EXISTS "${tempDbName}"`);
    });

    // ─────────────────────────────
    // 7️⃣ Восстанавливаем в целевую БД
    // ─────────────────────────────
    log("info", `  [${dbName}] restoring to target database...`);

    // Удаляем схему если существует
    await withClient(tgt, async (client) => {
      await client.query(`DROP SCHEMA IF EXISTS "${dbName}" CASCADE`);
    });

    // Восстанавливаем
    exec(
      [
        "psql",
        `-h "${tgt.host}"`,
        `-p ${tgt.port}`,
        `-U "${tgt.user}"`,
        `-d "${tgt.database}"`,
        `-v ON_ERROR_STOP=1`,
        `-f "${finalDumpFile}"`,
      ].join(" "),
      tgtPw,
    );

    log("info", `  [${dbName}] migration completed`);
  } finally {
    // Очистка временных файлов
    if (fs.existsSync(dumpFile)) fs.unlinkSync(dumpFile);
    if (fs.existsSync(finalDumpFile)) fs.unlinkSync(finalDumpFile);

    // Очистка временной БД на случай если что-то пошло не так
    try {
      await withClient({ ...tgt, database: "postgres" }, async (client) => {
        await client.query(`
          SELECT pg_terminate_backend(pid)
          FROM pg_stat_activity
          WHERE datname = '${tempDbName}' AND pid <> pg_backend_pid()
        `);
        await client.query(`DROP DATABASE IF EXISTS "${tempDbName}"`);
      });
    } catch {
      // Игнорируем ошибки при очистке
    }
  }
}

// ─── verification ────────────────────────────────────────────────────────────

async function tableChecksum(client: Client, schema: string, table: string): Promise<string> {
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
