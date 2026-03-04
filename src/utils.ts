import { type ExecSyncOptions, execSync } from "node:child_process";
import fs from "node:fs";
import chalk from "chalk";
import { CONFIG } from "./config";

export type LogLevel = "info" | "warn" | "error" | "done" | "fail";

function timestamp(): string {
  return new Date().toLocaleTimeString("en-GB"); // HH:MM:SS
}

export function log(level: LogLevel, msg: string): void {
  const ts = chalk.gray(`[${timestamp()}]`);

  const prefix: Record<LogLevel, string> = {
    info: chalk.cyan("[info]"),
    warn: chalk.yellow("[warn]"),
    error: chalk.red("[error]"),
    done: chalk.green("[done]"),
    fail: chalk.red.bold("[fail]"),
  };

  const out = level === "error" || level === "fail" ? process.stderr : process.stdout;
  out.write(`${ts} ${prefix[level]} ${msg}\n`);
}

/** Atomic file write: write to temp file then rename. */
export function atomicWrite(filePath: string, data: string): void {
  const tmp = `${filePath}.tmp`;
  fs.writeFileSync(tmp, data, "utf-8");
  fs.renameSync(tmp, filePath);
}

export function exec(cmd: string, password: string): void {
  const opts: ExecSyncOptions = {
    env: {
      ...process.env,
      PGPASSWORD: password,
      ...(CONFIG.ssl ? { PGSSLMODE: "require" } : {}),
      // Disable statement/lock timeouts for long-running dump/restore operations.
      // This prevents Odyssey and managed PG from killing slow restores.
      PGOPTIONS: "-c statement_timeout=0 -c lock_timeout=0 -c idle_in_transaction_session_timeout=0",
    },
    stdio: "pipe",
    // 0 = no timeout (default). PostgreSQL-level timeouts are handled via PGOPTIONS above.
    ...(CONFIG.execTimeoutMs > 0 ? { timeout: CONFIG.execTimeoutMs } : {}),
  };

  try {
    execSync(cmd, opts);
  } catch (err: unknown) {
    // Extract the actual PostgreSQL / pg_dump error from stderr
    const stderr =
      err instanceof Error && "stderr" in err
        ? String((err as NodeJS.ErrnoException & { stderr: Buffer }).stderr).trim()
        : "";
    const fallback = err instanceof Error ? err.message : String(err);
    throw new Error(stderr || fallback);
  }
}

/**
 * Checks that required PostgreSQL client binaries are available in PATH.
 * Called once at startup before any migration work begins.
 */
export function checkBinaries(): void {
  for (const bin of ["pg_dump", "psql"]) {
    try {
      execSync(`${bin} --version`, { stdio: "pipe" });
    } catch {
      throw new Error(
        `"${bin}" not found in PATH. Please install PostgreSQL client tools (v14+) and make sure they are accessible.`,
      );
    }
  }
}
