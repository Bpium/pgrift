import json
from pathlib import Path
from urllib.parse import urlparse
from typing import Optional, List, Tuple, Dict

script_dir = Path(__file__).resolve().parent

save_dir = "data"
Path(save_dir).mkdir(parents=True, exist_ok=True)

with open(script_dir / "data.json", "r", encoding="utf-8") as file:
    source_data = json.load(file)


def extract_db_name(connection_string: str) -> Optional[str]:
    """Extract database name from a PostgreSQL connection string."""
    try:
        return urlparse(connection_string).path.lstrip("/") or None
    except Exception:
        return None


def build_entries(items: List[Dict]) -> Tuple[List[str], List[Dict]]:
    """
    Returns two lists from a source array:
      - connections: legacy flat list of connection strings
      - entries:     new format [{ id, dbName }] 
                     (only items that have both $database and id)
    """
    connections: List[str] = []
    entries: List[Dict] = []

    for item in items:
        conn = item.get("$database")
        if not conn:
            continue

        connections.append(conn)

        record_id = item.get("id")
        if record_id is not None:
            db_name = extract_db_name(conn)
            if db_name:
                entries.append({"id": record_id, "dbName": db_name})

    return connections, entries


tiny_connections, tiny_entries = build_entries(source_data.get("tiny", []))
big_connections, big_entries = build_entries(source_data.get("biggest", []))

# ── Legacy output (flat connection strings) ───────────────────────────────────
with open(Path(save_dir) / "tiny.json", "w", encoding="utf-8") as f:
    json.dump(tiny_connections, f, indent=4, ensure_ascii=False)

with open(Path(save_dir) / "big.json", "w", encoding="utf-8") as f:
    json.dump(big_connections, f, indent=4, ensure_ascii=False)

# ── New output ({ id, dbName } objects) ──────────────────────────────────────
with open(Path(save_dir) / "tiny-entries.json", "w", encoding="utf-8") as f:
    json.dump(tiny_entries, f, indent=4, ensure_ascii=False)

with open(Path(save_dir) / "big-entries.json", "w", encoding="utf-8") as f:
    json.dump(big_entries, f, indent=4, ensure_ascii=False)

print("Extraction complete!")
print(f"Tiny  — connections: {len(tiny_connections)}, entries with id: {len(tiny_entries)}")
print(f"Big   — connections: {len(big_connections)}, entries with id: {len(big_entries)}")