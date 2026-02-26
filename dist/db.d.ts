import { Client } from "pg";
export declare function withClient<T>(config: object, fn: (client: Client) => Promise<T>): Promise<T>;
export declare function ensureTargetDatabase(): Promise<void>;
export declare function getTenants(): Promise<string[]>;
