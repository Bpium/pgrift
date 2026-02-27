import json
from pathlib import Path

script_dir = Path(__file__).resolve().parent

save_dir = "data"
Path(save_dir).mkdir(parents=True, exist_ok=True)

with open(script_dir / "data.json", "r", encoding="utf-8") as file:
    source_data = json.load(file)

tiny_connections = [
    item['$database'] 
    for item in source_data.get('tiny', []) 
    if '$database' in item
]

big_connections = [
    item['$database'] 
    for item in source_data.get('biggest', []) 
    if '$database' in item
]

with open(Path(save_dir) / 'tiny.json', 'w', encoding='utf-8') as file:
    json.dump(tiny_connections, file, indent=4, ensure_ascii=False)
    
with open(Path(save_dir) / 'big.json', 'w', encoding='utf-8') as file:
    json.dump(big_connections, file, indent=4, ensure_ascii=False)

print(f"Extraction complete!")
print(f"Tiny databases found: {len(tiny_connections)}")
print(f"Biggest databases found: {len(big_connections)}")