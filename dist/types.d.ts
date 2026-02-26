export interface FailedEntry {
    db: string;
    error: string;
    attempts: number;
}
export interface State {
    completed: string[];
    failed: FailedEntry[];
    startedAt: string;
    lastUpdated: string;
}
