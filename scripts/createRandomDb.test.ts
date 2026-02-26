import { execSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import { Client } from "pg";
import { buildConnectionString, CONFIG } from "../src/config";

async function main(): Promise<void> {
  const { createDbCount, createDbPrefix, createDbProjectPath, dbListPath } = CONFIG;

  const client = new Client({
    connectionString: buildConnectionString(CONFIG.source, "postgres"),
  });

  await client.connect();

  const dbNames: string[] = [];

  for (let i = 1; i <= createDbCount; i++) {
    const dbName = `${createDbPrefix}${i}`;
    dbNames.push(dbName);

    try {
      await client.query(`CREATE DATABASE "${dbName}"`);
      console.log(`[${i}/${createDbCount}] Created: ${dbName}`);
    } catch (e: any) {
      if (e.code !== "42P04") {
        await client.end();
        throw e;
      }
    }

    if (createDbProjectPath) {
      const dbUrl = buildConnectionString(CONFIG.source, dbName);
      try {
        execSync(`npm run postinstall-dev`, {
          cwd: createDbProjectPath,
          env: { ...process.env, DB_CONNECTION_STRING: dbUrl },
          stdio: "inherit",
        });
        console.log(`[${i}/${createDbCount}] postinstall-dev OK`);
      } catch (e: any) {
        console.error(`[${i}/${createDbCount}] postinstall-dev FAILED:`, e.stderr?.toString());
      }
    }
  }

  await client.end();

  writeFileSync(dbListPath, JSON.stringify(dbNames, null, 2));
  console.log(`Done. ${dbListPath} written.`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
