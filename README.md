# pgrift

Moves many PostgreSQL databases (one per tenant) into a single DB: each tenant becomes a schema. Dumps the `public` schema, rewrites references to the target schema name, then restores via `psql` into the target. State is saved so you can resume after a crash.

**Requires:** Node 18+, `pg_dump` and `psql` in PATH.

## Setup

```bash
cp .env.example .env
# edit .env: SOURCE_URL, TARGET_URL, and other options
npm install
```

## Configuration

All config is read from env (see `.env.example`).

### Connection

| Variable | Description |
|----------|-------------|
| `SOURCE_URL` | Source PostgreSQL connection string (no database — tenant DBs are discovered from it) |
| `TARGET_URL` | Target PostgreSQL connection string including database name |
| `TARGET_DATABASE` | Target database name (default: `tenants`). **Must already exist** |
| `SSL` | Set to `true` to enable SSL for all connections and pg_dump/psql (required for Yandex Cloud and other managed PG) |

### Tenant discovery — choose one of two options

**Option 1 — auto-discover from source (default):**

All databases on the source server are migrated, excluding system ones. Use `FILTER_PREFIX` to narrow down.

| Variable | Description |
|----------|-------------|
| `FILTER_PREFIX` | Only migrate DBs whose name starts with this prefix (e.g. `bench_db_`) |

**Option 2 — explicit list via JSON file:**

Set `DB_LIST_FILE` to a path of a JSON file with an array of connection strings. Each entry is a full PostgreSQL connection string including the database name. When set, `SOURCE_URL` and `FILTER_PREFIX` are ignored for tenant discovery.

```json
[
  "postgresql://user:pass@host1:5432/tenant_a",
  "postgresql://user:pass@host2:5432/tenant_b",
  "postgresql://user:pass@host1:5432/tenant_c"
]
```

```env
DB_LIST_FILE=./db-list.json
```

This is useful when:
- Tenants live on different servers
- You need to migrate a specific subset without a shared prefix
- You are on a managed platform (e.g. Yandex Cloud) that disallows `CREATE DATABASE`

### Schema rename strategy

pgrift supports two strategies for renaming the `public` schema during migration:

| Strategy | `SCHEMA_RENAME_STRATEGY` | Source DB modified? | Requires schema ownership? |
|----------|--------------------------|---------------------|---------------------------|
| **Dump rewrite** (default) | `rewrite` | No | No |
| **In-place rename** | `rename` | Yes (rolled back) | Yes |

**`rewrite`** (default) — Dumps the `public` schema as-is, then rewrites all schema references (`public.*` → `"<tenant>".*`) directly in the dump file before restoring. The source database is never modified. Works on managed platforms (Yandex Cloud, etc.) where the DB user is not the owner of the `public` schema.

**`rename`** — Renames the `public` schema in the source database via `ALTER SCHEMA public RENAME TO "<tenant>"`, dumps, then rolls back the rename. Requires the DB user to be the **owner** of the `public` schema. Use this on self-managed PostgreSQL where you have full control.

```env
# Default — works everywhere, including managed PG:
SCHEMA_RENAME_STRATEGY=rewrite

# Self-managed PG where user owns public schema:
SCHEMA_RENAME_STRATEGY=rename
```

### Other options

| Variable | Default | Description |
|----------|---------|-------------|
| `SCHEMA_RENAME_STRATEGY` | `rewrite` | Schema rename strategy: `rewrite` (dump file rewrite, no source modification) or `rename` (in-place ALTER SCHEMA, requires ownership) |
| `SOURCE_READONLY` | `false` | After a successful dump, set the source DB to read-only (`ALTER DATABASE … SET default_transaction_read_only = true`). Useful during cutover to prevent new writes landing in the old location. Wrapped in try/catch — never blocks migration if the user lacks `ALTER DATABASE` privilege |
| `SOURCE_DISCOVERY_DATABASE` | `postgres` | Database to connect to on the source server when discovering tenants via `pg_database`. On managed platforms (e.g. Yandex Cloud) the system `postgres` database is often inaccessible — set this to any existing DB the user has access to |
| `DUMP_DIR` | `/tmp/pg_migration_dumps` | Temp directory for dump files |
| `STATE_FILE` | `./migration-state.json` | Resume state file |
| `CONCURRENCY` | `10` | Number of tenants to process in parallel |
| `MAX_RETRIES` | `3` | Max attempts per tenant before it is skipped for the rest of the run. Exhausted tenants are clearly reported and can be retried manually |
| `DRY_RUN` | `false` | Print which tenants would be migrated without touching any data |
| `EXEC_TIMEOUT_MS` | `0` | Hard process timeout for pg_dump / psql (ms). `0` = no timeout (default). PostgreSQL-level timeouts are already disabled via `PGOPTIONS`. Set this only if you want a safety kill for completely hung processes |
| `SKIP_CHECKSUM_ABOVE_ROWS` | — | Skip MD5 checksum for tables with more rows than this |

## Commands

| Command | What it does |
|---------|--------------|
| `pgrift` / `npx pgrift` / `npm run dev` | Run migration |
| `npm run verify` | Compare source DBs vs target schemas: table list, row counts, checksums |
| `npm run cleanup` | Remove all tenant schemas from target, state file, dump dir contents, and `migration-report.json` |
| `npm run lint` | Run Biome linter |
| `npm run lint:fix` | Lint and apply safe fixes |
| `npm run format` | Format code with Biome |

After an interrupt (`Ctrl+C`), pgrift finishes the current batch, saves state, and exits cleanly. Run again — completed tenants are skipped automatically.

## What the migration does per tenant

**With `SCHEMA_RENAME_STRATEGY=rewrite` (default):**

1. Terminate connections to the source DB.
2. `pg_dump -n public` — dump the public schema as-is.
3. Rewrite the dump file: replace all `public` schema references with `"<tenant>"` (safely skips COPY data blocks).
4. In target: drop schema `"<tenant>"` if exists, create extensions, apply dump with `psql -f`.
5. Verify: compare table list, row counts, and MD5 checksums.

**With `SCHEMA_RENAME_STRATEGY=rename`:**

1. Terminate connections to the source DB.
2. In source: `ALTER SCHEMA public RENAME TO "<tenant>"`, create new `public`, set DB `search_path`.
3. `pg_dump -n "<tenant>"` to a file (fixes `gin_trgm_ops` schema reference in dump if present).
4. Rollback source: restore `public`, reset `search_path`.
5. In target: drop schema `"<tenant>"` if exists, create extensions, apply dump with `psql -f`.
6. Verify: compare table list, row counts, and MD5 checksums.

## Verification

Built-in verification runs automatically after each tenant. It checks:
- All tables present in target schema
- Row counts match
- MD5 checksums match (skipped for tables above `SKIP_CHECKSUM_ABOVE_ROWS`)

Standalone: `npm run verify [db1 db2 ...]`. With no args, uses all tenant DBs from source.

## Yandex Cloud / managed PostgreSQL

Managed platforms typically disallow `CREATE DATABASE` via SQL. pgrift handles this — the target database must be **created manually** via the cloud console, then referenced in `TARGET_URL`. The migration verifies it exists and fails with a clear error if not.

Managed platforms also restrict access to the system `postgres` database. pgrift uses it by default to discover tenant DBs (`SELECT datname FROM pg_database`). If your user can't connect to `postgres`, set `SOURCE_DISCOVERY_DATABASE` to any existing DB they do have access to:

```env
SSL=true
SOURCE_DISCOVERY_DATABASE=some_existing_db
```

The discovery database is only used to read the list of databases — no data is modified in it.

## Other scripts

- `npm run comparison` — load test: separate DBs vs single-DB schemas.
- `npm run many-schemas` / `many-bases` — benchmark helpers.
- `npm run create-db` — create random DBs for testing.

## Caveats

- Source must have only `public` schema. Test on one tenant first.
- Extensions are re-created in target from source list; some may fail if already present.
- Cleanup wipes all non-system schemas in target and local state — dev/test only.
- The target database must exist before running migration.

## Publishing to npm

1. `npm login`
2. `npm version patch` (or `minor` / `major`)
3. `npm run build && npm publish`
4. Scoped package: `npm publish --access public`
