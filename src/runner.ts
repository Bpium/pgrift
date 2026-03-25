import fs from "node:fs";
import * as readline from "node:readline";
import { disableBpiumVersion, updateBpiumSchema } from "./bpium";
import { CONFIG } from "./config";
import { ensureTargetDatabase, getTenants, getTenantsFromFile } from "./db";
import { migrateTenant } from "./migrate-tenant";
import { loadState, saveState } from "./state";
import type { FailedEntry, State, TenantEntry } from "./types";
import { atomicWrite, checkBinaries, log } from "./utils";
import { verifyMigration } from "./verify-migration";

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------

let shuttingDown = false;

function registerShutdownHandlers(): void {
  const handler = (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    log("warn", `${signal} received — finishing current batch, then exiting. State will be saved.`);
  };
  process.on("SIGINT", () => handler("SIGINT"));
  process.on("SIGTERM", () => handler("SIGTERM"));
}

// ---------------------------------------------------------------------------
// Batch runner
// ---------------------------------------------------------------------------

async function runBatch(tenants: TenantEntry[], state: State): Promise<void> {
  const results = await Promise.allSettled(
    tenants.map(async ({ db, source, bpiumId }) => {
      if (bpiumId !== undefined) {
        await disableBpiumVersion(bpiumId, db);
      }

      await migrateTenant(db, source);

      const { ok, reasons } = await verifyMigration(db, source);
      if (!ok) {
        throw new Error(`verification failed: ${reasons.join(" | ")}`);
      }

      if (bpiumId !== undefined) {
        await updateBpiumSchema(bpiumId, db);
      }

      return db;
    }),
  );

  for (let i = 0; i < results.length; i++) {
    const db = tenants[i].db;
    const result = results[i];

    if (result.status === "fulfilled") {
      state.completed.push(db);
      // Remove from failed list if it previously failed and is now done
      state.failed = state.failed.filter((f: FailedEntry) => f.db !== db);
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
      log("fail", `${db}: ${message.slice(0, 300)}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function askConfirm(prompt: string): Promise<boolean> {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise((resolve) => {
    rl.question(prompt, (answer) => {
      rl.close();
      resolve(answer.trim().toUpperCase() === "Y");
    });
  });
}

function writeReport(state: State, allTenants: TenantEntry[], totalTime: number): void {
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

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

export async function runMigration(): Promise<void> {
  // 1. Preflight checks
  checkBinaries();

  // 2. Dry-run banner
  if (CONFIG.dryRun) {
    log("warn", "DRY RUN mode — no data will be migrated");
  }

  // 3. User confirmation (skip in dry-run? still ask so user sees the plan)
  const confirmed = await askConfirm(`Run migration${CONFIG.dryRun ? " (DRY RUN)" : ""}? (Y/N): `);
  if (!confirmed) {
    log("info", "Aborted.");
    return;
  }

  registerShutdownHandlers();

  fs.mkdirSync(CONFIG.dumpDir, { recursive: true });

  await ensureTargetDatabase();

  const state = loadState();

  // 4. Discover tenants
  let allTenants: TenantEntry[];
  if (CONFIG.dbListFile) {
    log("info", `loading tenant list from file: ${CONFIG.dbListFile}`);
    allTenants = getTenantsFromFile(CONFIG.dbListFile);
  } else {
    const dbNames = await getTenants();
    allTenants = dbNames.map((db) => ({ db }));
  }

  // 5. Filter remaining: exclude completed and tenants that hit maxRetries
  const completedSet = new Set(state.completed);
  const exhaustedSet = new Set(
    state.failed.filter((f) => f.attempts >= CONFIG.maxRetries).map((f) => f.db),
  );

  const remaining = allTenants.filter(
    ({ db }) => !completedSet.has(db) && !exhaustedSet.has(db),
  );

  log(
    "info",
    `total: ${allTenants.length} | done: ${state.completed.length} | ` +
      `remaining: ${remaining.length} | failed: ${state.failed.length}` +
      (exhaustedSet.size > 0 ? ` | exhausted (≥${CONFIG.maxRetries} attempts): ${exhaustedSet.size}` : ""),
  );

  if (exhaustedSet.size > 0) {
    log("warn", `skipping ${exhaustedSet.size} tenant(s) that exceeded MAX_RETRIES=${CONFIG.maxRetries}:`);
    for (const db of exhaustedSet) {
      const entry = state.failed.find((f) => f.db === db);
      log("fail", `  ${db} (${entry?.attempts} attempts): ${entry?.error?.slice(0, 200)}`);
    }
  }

  // 6. Dry-run: just list what would be migrated
  if (CONFIG.dryRun) {
    log("info", `would migrate ${remaining.length} tenant(s):`);
    for (const { db } of remaining) log("info", `  - ${db}`);
    log("warn", "dry run complete — no changes made");
    return;
  }

  if (remaining.length === 0) {
    log("info", "nothing to migrate");
    writeReport(state, allTenants, 0);
    return;
  }

  // 7. Split into batches and run
  const batches: TenantEntry[][] = [];
  for (let i = 0; i < remaining.length; i += CONFIG.concurrency) {
    batches.push(remaining.slice(i, i + CONFIG.concurrency));
  }

  const startTime = Date.now();
  const initialDone = state.completed.length;

  for (let i = 0; i < batches.length; i++) {
    // Check shutdown flag between batches
    if (shuttingDown) {
      log("warn", "shutdown requested — stopping after current batch");
      break;
    }

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
    for (const f of state.failed) {
      log("fail", `  ${f.db} (${f.attempts} attempts): ${f.error.slice(0, 200)}`);
    }
  }

  writeReport(state, allTenants, totalTime);

  if (shuttingDown) {
    log("warn", "migration interrupted — run again to resume from where it stopped");
    process.exit(0);
  }
}
