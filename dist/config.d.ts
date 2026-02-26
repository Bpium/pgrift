import "dotenv/config";
import type { ClientConfig } from "pg";
/** Build a connection URL from config and optional database name. */
export declare function buildConnectionString(cfg: ClientConfig, database?: string): string;
export declare const CONFIG: {
    source: ClientConfig;
    target: ClientConfig;
    dumpDir: string;
    stateFile: string;
    concurrency: number;
    excludeDatabases: string[];
    filterPrefix: string | null;
    execTimeoutMs: number;
    skipChecksumAboveRows: number | undefined;
    /** Path to JSON array of DB/schema names (benchmark scripts). Default: ./scripts/db-list.json */
    dbListPath: string;
    /** Table name used by benchmark scripts (e.g. comparison, manyBases, manySchemas). Default: users_data */
    benchTable: string;
    /** Column used as synthetic ID in benchmarks. Default: dbId */
    benchIdColumn: string;
    /** Column used as name field in benchmarks. Default: name */
    benchNameColumn: string;
    /** createRandomDb script: number of DBs to create. Default: 1000 */
    createDbCount: number;
    /** createRandomDb script: DB name prefix. Default: bench_db_ */
    createDbPrefix: string;
    /** createRandomDb script: path to project that runs postinstall-dev for each DB. Optional. */
    createDbProjectPath: string | null;
};
