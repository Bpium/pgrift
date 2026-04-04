import json
from pathlib import Path

# Путь к исходному файлу
input_file = Path("dbs.json")  # замените на свой файл
output_dir = Path("data")
output_dir.mkdir(exist_ok=True)

# Сколько частей хотим
num_parts = 3

# Чтение исходного JSON
with open(input_file, "r", encoding="utf-8") as f:
    data = json.load(f)

total = len(data)
part_size = total // num_parts
remainder = total % num_parts  # на случай, если не делится нацело

start = 0
for i in range(num_parts):
    # Добавляем остаток к первой части, если есть
    end = start + part_size + (1 if i < remainder else 0)
    part = data[start:end]
    
    output_file = output_dir / f"part_{i+1}.json"
    with open(output_file, "w", encoding="utf-8") as f_out:
        json.dump(part, f_out, indent=4, ensure_ascii=False)
    
    print(f"Создан файл {output_file} с {len(part)} объектами")
    start = end