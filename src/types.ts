import type { ClientConfig } from "pg";

export interface TenantEntry {
  db: string;
  source?: ClientConfig; // if undefined, falls back to CONFIG.source
}

export interface FailedEntry {
  db: string;
  error: string;
  attempts: number;
}

export interface State {
  completed: string[];
  failed: FailedEntry[];
  startedAt: string;
  lastUpdated: string;
}
