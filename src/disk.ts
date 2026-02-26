import { execSync } from "node:child_process";

export function getFreeBytesOnDir(dir: string): number {
  const out = execSync(`df -k "${dir}"`, { encoding: "utf-8" });
  const line = out.trim().split("\n")[1];
  const available = parseInt(line.trim().split(/\s+/)[3], 10);
  return available * 1024;
}

export function assertDiskSpace(dir: string, minBytes = 512 * 1024 * 1024): void {
  const free = getFreeBytesOnDir(dir);
  if (free < minBytes) {
    throw new Error(
      `Not enough disk space in ${dir}: ${Math.round(
        free / 1024 / 1024,
      )} MB free, need at least ${Math.round(minBytes / 1024 / 1024)} MB`,
    );
  }
}
