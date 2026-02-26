import fs from "node:fs";
import { CONFIG } from "./config";
import type { State } from "./types";
import { atomicWrite, log } from "./utils";

export function loadState(): State {
  if (fs.existsSync(CONFIG.stateFile)) {
    try {
      return JSON.parse(fs.readFileSync(CONFIG.stateFile, "utf-8")) as State;
    } catch {
      log("warn", "state file is corrupted, starting fresh");
    }
  }
  return {
    completed: [],
    failed: [],
    startedAt: new Date().toISOString(),
    lastUpdated: new Date().toISOString(),
  };
}

export function saveState(state: State): void {
  state.lastUpdated = new Date().toISOString();
  atomicWrite(CONFIG.stateFile, JSON.stringify(state, null, 2));
}
