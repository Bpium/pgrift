"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.withClient = withClient;
exports.ensureTargetDatabase = ensureTargetDatabase;
exports.getTenants = getTenants;
const pg_1 = require("pg");
const config_1 = require("./config");
const utils_1 = require("./utils");
async function withClient(config, fn) {
    const client = new pg_1.Client(config);
    await client.connect();
    try {
        return await fn(client);
    }
    finally {
        await client.end();
    }
}
async function ensureTargetDatabase() {
    await withClient({ ...config_1.CONFIG.target, database: "postgres" }, async (client) => {
        const { rows } = await client.query("SELECT 1 FROM pg_database WHERE datname = $1", [
            config_1.CONFIG.target.database,
        ]);
        if (rows.length === 0) {
            await client.query(`CREATE DATABASE "${config_1.CONFIG.target.database}"`);
            (0, utils_1.log)("info", `created database: ${config_1.CONFIG.target.database}`);
        }
    });
}
async function getTenants() {
    return withClient({ ...config_1.CONFIG.source, database: "postgres" }, async (client) => {
        const placeholders = config_1.CONFIG.excludeDatabases.map((_, i) => `$${i + 1}`).join(", ");
        const { rows } = await client.query(`SELECT datname FROM pg_database
       WHERE datname NOT IN (${placeholders})
         AND datname NOT LIKE 'pg_%'
       ORDER BY datname`, config_1.CONFIG.excludeDatabases);
        let tenants = rows.map((r) => r.datname);
        if (config_1.CONFIG.filterPrefix) {
            tenants = tenants.filter((db) => db.startsWith(config_1.CONFIG.filterPrefix));
        }
        return tenants;
    });
}
