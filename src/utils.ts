import { type ExecSyncOptions, execSync } from "node:child_process";
import fs from "node:fs";
import { CONFIG } from "./config";

export type LogLevel = "info" | "warn" | "error" | "done" | "fail";

export function log(level: LogLevel, msg: string): void {
  const prefix: Record<LogLevel, string> = {
    info: "[info]",
    warn: "[warn]",
    error: "[error]",
    done: "[done]",
    fail: "[fail]",
  };
  const out = level === "error" || level === "fail" ? process.stderr : process.stdout;
  out.write(`${prefix[level]} ${msg}\n`);
}

/** Atomic file write: write to temp file then rename. */
export function atomicWrite(filePath: string, data: string): void {
  const tmp = `${filePath}.tmp`;
  fs.writeFileSync(tmp, data, "utf-8");
  fs.renameSync(tmp, filePath);
}

export function exec(cmd: string, password: string): void {
  const opts: ExecSyncOptions = {
    env: { ...process.env, PGPASSWORD: password },
    stdio: "pipe",
    timeout: CONFIG.execTimeoutMs ?? 10 * 60 * 1000,
  };
  execSync(cmd, opts);
}
