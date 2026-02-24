#!/usr/bin/env ts-node
/**
 * Cleanup script - removes all custom schemas from target database
 * Use only for testing!
 *
 * Usage: ts-node cleanup-schemas.ts
 */

import { Client } from "pg";
import { CONFIG } from "./config";

async function cleanupSchemas(): Promise<void> {
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
      console.log("No custom schemas found");
      return;
    }

    console.log(`Found ${schemas.length} schemas to delete:`);
    schemas.forEach((schema: string) => console.log(`  ${schema}`));

    // Confirmation
    const readline = require("readline").createInterface({
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
