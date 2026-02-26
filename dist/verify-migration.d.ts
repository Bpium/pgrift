import type { Client } from "pg";
export declare function tableChecksum(client: Client, schema: string, table: string): Promise<string>;
export declare function verifyMigration(dbName: string): Promise<{
    ok: boolean;
    reasons: string[];
}>;
