# pg-tenant-migrator

**pg-tenant-migrator** is a utility to consolidate multiple PostgreSQL databases (each representing a tenant) into a single target database, placing each tenant’s tables in a dedicated schema.

---

## Features

- **Automatic Discovery:** Finds all tenant databases on the source server (optionally filtered by name prefix).
- **Dump and Restore:** For each tenant, exports the database with `pg_dump`, creates a matching schema in the target database, and restores the dump into that schema using `pg_restore`.
- **Batch Processing:** Migrates multiple tenants in parallel, configurable by the concurrency setting.
- **Progress Tracking & Resume:** Persists status to `migration-state.json` and can resume seamlessly after interruptions.
- **Migration Report:** Outputs a detailed `migration-report.json` with migration statistics.

---

## Requirements

- **Node.js** v18 or higher
- `pg_dump` and `pg_restore` must be available in your system `PATH`
- Source databases must be network-accessible from the machine running this script

---

## Configuration

Edit the `CONFIG` object at the top of `migrate.ts`:


| Key                | Description                                                                    |
| ------------------ | ------------------------------------------------------------------------------ |
| `source`           | Connection details for the source PostgreSQL server                            |
| `target`           | Connection details and target database name                                    |
| `dumpDir`          | Temporary directory for dump files (removed after each tenant migration)       |
| `stateFile`        | Path to the JSON file tracking migration state                                 |
| `concurrency`      | Number of tenants to process in parallel                                       |
| `excludeDatabases` | List of databases to skip (system DBs, the target itself, etc.)                |
| `filterPrefix`     | Only migrate databases whose name begins with this string (set `null` for all) |


---

## Usage

```bash
npm install
npx ts-node migrate.ts
```

If the process is interrupted, simply re-run the command—previously completed tenants will be skipped automatically.

---

## Caveats

- **Schema Mapping Limitation:** The `pg_restore --schema` flag filters dump objects by schema name, but does not remap them. If your source databases contain schemas besides `public`, you may need manual intervention. Always test migration on a single tenant first.
- **Extensions:** PostgreSQL extensions installed in a source database are *not* restored automatically.
- **Data Validation:** This tool only checks that the number of tables matches between source and target (via `information_schema.tables`). No further validation (e.g., row counts, checksums). Treat this as a sanity check only.

