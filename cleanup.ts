#!/usr/bin/env ts-node

/**
 * Cleanup script - removes all custom schemas from target database,
 * state file, dump directory contents, and migration report.
 * Use only for testing!
 *
 * Usage: npm run cleanup  (or ts-node cleanup.ts)
 */

import fs from "node:fs";
import path from "node:path";
import { Client } from "pg";
import { CONFIG } from "./src/config";

function cleanLocalFiles(): void {
  // State file
  if (fs.existsSync(CONFIG.stateFile)) {
    fs.unlinkSync(CONFIG.stateFile);
    console.log("Deleted state file:", CONFIG.stateFile);
  }

  // Migration report
  const reportPath = "./migration-report.json";
  if (fs.existsSync(reportPath)) {
    fs.unlinkSync(reportPath);
    console.log("Deleted:", reportPath);
  }

  // Dump directory: remove all files (e.g. *.final.dump)
  if (fs.existsSync(CONFIG.dumpDir)) {
    const entries = fs.readdirSync(CONFIG.dumpDir, { withFileTypes: true });
    let removed = 0;
    for (const ent of entries) {
      const full = path.join(CONFIG.dumpDir, ent.name);
      if (ent.isFile()) {
        fs.unlinkSync(full);
        removed++;
      }
    }
    if (removed > 0) {
      console.log(`Deleted ${removed} file(s) in dump dir: ${CONFIG.dumpDir}`);
    }
  }
}

async function cleanupSchemas(): Promise<void> {
  cleanLocalFiles();

  const client = new Client(CONFIG.target);

  try {
    await client.connect();
    console.log("Connected to target DB:", CONFIG.target.database);

    // Get all custom schemas (exclude system schemas)
    const { rows } = await client.query(`
      SELECT schema_name 
      FROM information_schema.schemata 
      WHERE schema_name NOT IN (
        'information_schema', 
        'pg_catalog', 
        'pg_toast', 
        'public'
      )
      ORDER BY schema_name
    `);

    const schemas = rows.map((r: any) => r.schema_name);

    if (schemas.length === 0) {
      console.log("No custom schemas found. Cleanup complete.");
      return;
    }

    console.log(`Found ${schemas.length} schemas to delete:`);
    for (const schema of schemas) console.log(`  ${schema}`);

    // Confirmation
    const readline = require("node:readline").createInterface({
      input: process.stdin,
      output: process.stdout,
    });

    const response = await new Promise<string>((resolve) => {
      readline.question("Type 'DELETE ALL' to continue: ", (answer: string) => {
        readline.close();
        resolve(answer);
      });
    });

    if (response !== "DELETE ALL") {
      console.log("Aborted");
      return;
    }

    // Delete schemas
    let deleted = 0;
    for (const schema of schemas) {
      try {
        // Terminate active connections to schema
        await client.query(
          `
          SELECT pg_terminate_backend(pid)
          FROM pg_stat_activity 
          WHERE datname = $1 
            AND pid <> pg_backend_pid()
            AND query LIKE $2
          `,
          [CONFIG.target.database, `%${schema}%`],
        );

        // Drop schema
        await client.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
        console.log(`Deleted schema: ${schema}`);
        deleted++;
      } catch (error) {
        console.error(`Failed to delete ${schema}:`, error);
      }
    }

    console.log(`Cleanup complete. Deleted ${deleted}/${schemas.length} schemas`);
  } finally {
    await client.end();
  }
}

cleanupSchemas().catch(console.error);
