import fs from "node:fs";
import * as readline from "node:readline";
import { CONFIG } from "./config";
import { ensureTargetDatabase, getTenants, getTenantsFromFile } from "./db";
import { migrateTenant } from "./migrate-tenant";
import { loadState, saveState } from "./state";
import type { FailedEntry, State, TenantEntry } from "./types";
import { atomicWrite, log } from "./utils";
import { verifyMigration } from "./verify-migration";

async function runBatch(tenants: TenantEntry[], state: State): Promise<void> {
  const results = await Promise.allSettled(
    tenants.map(async ({ db, source }) => {
      await migrateTenant(db, source);

      const { ok, reasons } = await verifyMigration(db, source);
      if (!ok) {
        throw new Error(`verification failed: ${reasons.join(" | ")}`);
      }

      return db;
    }),
  );

  for (let i = 0; i < results.length; i++) {
    const db = tenants[i].db;
    const result = results[i];

    if (result.status === "fulfilled") {
      state.completed.push(db);
      log("done", `${db} (${state.completed.length} total)`);
    } else {
      const message =
        result.reason instanceof Error ? result.reason.message : String(result.reason);

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

function askConfirm(prompt: string): Promise<boolean> {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise((resolve) => {
    rl.question(prompt, (answer) => {
      rl.close();
      resolve(answer.trim().toUpperCase() === "Y");
    });
  });
}

export async function runMigration(): Promise<void> {
  const confirmed = await askConfirm("Run migration? (Y/N): ");
  if (!confirmed) {
    log("info", "Aborted.");
    return;
  }

  fs.mkdirSync(CONFIG.dumpDir, { recursive: true });

  await ensureTargetDatabase();

  const state = loadState();

  let allTenants: TenantEntry[];
  if (CONFIG.dbListFile) {
    log("info", `loading tenant list from file: ${CONFIG.dbListFile}`);
    allTenants = getTenantsFromFile(CONFIG.dbListFile);
  } else {
    const dbNames = await getTenants();
    allTenants = dbNames.map((db) => ({ db }));
  }

  const completed = new Set(state.completed);
  const remaining = allTenants.filter(({ db }) => !completed.has(db));

  log(
    "info",
    `total: ${allTenants.length} | done: ${state.completed.length} | ` +
      `remaining: ${remaining.length} | failed: ${state.failed.length}`,
  );

  if (remaining.length === 0) {
    log("info", "nothing to migrate");
    return;
  }

  const batches: TenantEntry[][] = [];
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

  log(
    "info",
    `completed: ${state.completed.length} | failed: ${state.failed.length} | time: ${totalTime}s`,
  );

  if (state.failed.length > 0) {
    log("warn", "failed tenants:");
    for (const f of state.failed)
      log("fail", `  ${f.db} (${f.attempts} attempts): ${f.error.slice(0, 200)}`);
  }

  atomicWrite(
    "./migration-report.json",
    JSON.stringify(
      {
        ...state,
        totalDatabases: allTenants.length,
        totalTimeSeconds: totalTime,
      },
      null,
      2,
    ),
  );
}
