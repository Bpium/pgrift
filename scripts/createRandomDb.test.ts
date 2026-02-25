import { execSync } from "child_process";
import { Client } from "pg";
import { writeFileSync } from "fs";

const BASE_CONN: string = "postgres://postgres:4321@localhost:5432/postgres";

const DB_COUNT: number = 1000;
const DB_PREFIX: string = "bench_db_";
const PROJECT_PATH: string = "C:/Users/Satoru/Desktop/main/server";

function getConnectionString(dbName: string): string {
  return `postgresql://postgres:4321@localhost:5432/${dbName}`;
}

async function main(): Promise<void> {
  const client = new Client({
    connectionString: BASE_CONN,
  });

  await client.connect();

  const dbNames: string[] = [];

  for (let i = 1; i <= DB_COUNT; i++) {
    const dbName = `${DB_PREFIX}${i}`;
    dbNames.push(dbName);

    try {
      await client.query(`CREATE DATABASE "${dbName}"`);
      console.log(`[${i}/${DB_COUNT}] Created: ${dbName}`);
    } catch (e: any) {
      // 42P04 = database already exists
      if (e.code !== "42P04") {
        await client.end();
        throw e;
      }
    }

    const dbUrl = getConnectionString(dbName);

    postgres: try {
      execSync(`npm run postinstall-dev`, {
        cwd: PROJECT_PATH,
        env: { ...process.env, DB_CONNECTION_STRING: dbUrl },
        stdio: "inherit", // вместо "pipe"
      });
      console.log(`[${i}/${DB_COUNT}] postinstall-dev OK`);
    } catch (e: any) {
      console.error(`[${i}/${DB_COUNT}] postinstall-dev FAILED:`, e.stderr?.toString());
    }
  }

  await client.end();

  writeFileSync("./scripts/db-list.json", JSON.stringify(dbNames, null, 2));
  console.log("Done. db-list.json written.");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
