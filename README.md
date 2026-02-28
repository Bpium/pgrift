# pgrift

Moves many PostgreSQL databases (one per tenant) into a single DB: each tenant becomes a schema. Uses in-place rename on source (`public` → tenant name), `pg_dump`, then `psql` into the target. State is saved so you can resume after a crash.

**Requires:** Node 18+, `pg_dump` and `psql` in PATH.

## Setup

```bash
cp .env.example .env
# edit .env: SOURCE_URL, TARGET_URL (and optionally DUMP_DIR, STATE_FILE, CONCURRENCY, FILTER_PREFIX)
npm install
```

Config is read from env (see `.env.example`). Main options: `SOURCE_URL`, `TARGET_URL`, `TARGET_DATABASE` (default `tenants`), `DUMP_DIR`, `STATE_FILE`, `CONCURRENCY`, `FILTER_PREFIX` (e.g. `bench_db_` to only migrate those DBs).

## Commands

| Command | What it does |
|--------|----------------|
| `pgrift` / `npx pgrift` / `npm run dev` | Run migration |
| `npm run verify` | Compare source DBs vs target schemas: table list, row counts, optional checksums |
| `npm run cleanup` | Remove all tenant schemas from target, state file, dump dir contents, and `migration-report.json`. Asks for `DELETE ALL` before dropping schemas |
| `npm run lint` | Run Biome linter (`just lint`) |
| `npm run lint:fix` | Lint and apply safe fixes |
| `npm run format` | Format code with Biome |

After an interrupt, run `npm run dev` again; completed tenants are skipped.

## What the migration does per tenant

1. Terminate connections to the source DB.
2. In source: `ALTER SCHEMA public RENAME TO "<tenant>"`, create new `public`, set DB `search_path`.
3. `pg_dump -n "<tenant>"` to a file (fixes `gin_trgm_ops` schema in dump if present).
4. Rollback source: restore `public`, reset `search_path`.
5. In target: create schema `"<tenant>"`, apply dump with `psql -f`.

Extensions from the source DB are created in target `public` if possible; no custom format or `pg_restore`.

## Verification

Built-in (after each tenant in the migration): same tables, row counts, and MD5 checksums per table (optional skip for large tables via `SKIP_CHECKSUM_ABOVE_ROWS`).

Standalone: `npm run verify` [db1 db2 ...]. With no args, uses all tenant DBs from source.

## Other scripts

- `npm run comparison` — load test: separate DBs vs single-DB schemas (writes `heavy-load-results.json`).
- `npm run many-schemas` / `many-bases` — benchmark helpers.
- `npm run create-db` — create random DBs for testing.

## Caveats

- Source must have only `public` (or you’ll need to handle other schemas yourself). Test on one tenant first.
- Extensions are re-created in target from the list in source; some may fail if already present or incompatible.
- Cleanup wipes all non-system schemas in the target and local migration state; use for dev/test only.

## Publishing to npm

1. Create an account at [npmjs.com](https://www.npmjs.com) and run `npm login`.
2. Bump version if needed: `npm version patch` (or `minor` / `major`).
3. Build and publish: `npm run build && npm publish`.
4. For scoped packages (e.g. `@username/pgrift`): use `npm publish --access public`.
