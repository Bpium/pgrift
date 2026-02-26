export type LogLevel = "info" | "warn" | "error" | "done" | "fail";
export declare function log(level: LogLevel, msg: string): void;
/** Atomic file write: write to temp file then rename. */
export declare function atomicWrite(filePath: string, data: string): void;
export declare function exec(cmd: string, password: string): void;
