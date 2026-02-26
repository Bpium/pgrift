#!/usr/bin/env node
import { runMigration } from "./src/runner";
import { log } from "./src/utils";

runMigration().catch((err: unknown) => {
  log("error", `fatal: ${err instanceof Error ? (err.stack ?? err.message) : String(err)}`);
  process.exit(1);
});
