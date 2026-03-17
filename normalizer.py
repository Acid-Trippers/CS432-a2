import re
import uuid
import json
import os
from difflib import get_close_matches

class AutonomousNormalizer:
    def __init__(self, similarity_threshold=0.85):
        self.master_keys = []
        self.threshold = similarity_threshold
        self.tables = {}
        self.schema = {}
        self.relationships = []

    def _normalize_key(self, key):
        snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', key).lower().replace(" ", "_")
        matches = get_close_matches(snake_case, self.master_keys, n=1, cutoff=self.threshold)
        if matches:
            return matches[0]
        self.master_keys.append(snake_case)
        return snake_case

    def _infer_type(self, value):
        if value is None: return "TEXT"
        if isinstance(value, bool): return "BOOLEAN"
        if isinstance(value, int): return "INTEGER"
        if isinstance(value, float): return "FLOAT"
        if isinstance(value, str):
            if re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*', value):
                return "TIMESTAMP"
            return "VARCHAR(255)"
        return "TEXT"

    def process_record(self, data, table_name="users", parent_info=None):
        table_name = self._normalize_key(table_name)
        
        if table_name not in self.tables:
            self.tables[table_name] = []
            self.schema[table_name] = {}

        record_id = str(uuid.uuid4())
        row = {"id": record_id}

        if parent_info:
            p_name, p_id = parent_info
            fk_col = f"{p_name}_id"
            row[fk_col] = p_id
            rel_entry = {"from": table_name, "to": p_name, "key": fk_col}
            if rel_entry not in self.relationships:
                self.relationships.append(rel_entry)

        for key, value in data.items():
            norm_key = self._normalize_key(key)

            if isinstance(value, dict):
                # Shred nested objects (like 'metadata' or 'sensor_data') into new tables
                self.process_record(value, norm_key, (table_name, record_id))
            
            elif isinstance(value, list):
                # Shred arrays into repeating group tables
                list_table = f"{table_name}_{norm_key}"
                for item in value:
                    if isinstance(item, dict):
                        self.process_record(item, list_table, (table_name, record_id))
                    else:
                        if list_table not in self.tables: self.tables[list_table] = []
                        self.tables[list_table].append({
                            "id": str(uuid.uuid4()),
                            f"{table_name}_id": record_id,
                            "item_value": item
                        })
            else:
                row[norm_key] = value
                if norm_key not in self.schema[table_name]:
                    self.schema[table_name][norm_key] = self._infer_type(value)

        self.tables[table_name].append(row)

    def export(self, filename="normalized_data.json"):
        output = {
            "metadata": {
                "schema": self.schema,
                "relationships": self.relationships
            },
            "tables": self.tables
        }
        with open(filename, "w") as f:
            json.dump(output, f, indent=4)
        print(f"\n[SUCCESS] Normalization complete.")
        print(f"[*] Generated Tables: {', '.join(self.tables.keys())}")
        print(f"[*] Data saved to {filename}")

def run():
    input_file = "received_data.json"
    if not os.path.exists(input_file):
        print(f"[!] Error: {input_file} not found.")
        return

    engine = AutonomousNormalizer()
    with open(input_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try:
                record = json.loads(line)
                engine.process_record(record)
            except json.JSONDecodeError as e:
                print(f"[!] Skipping invalid JSON line: {e}")

    engine.export()

if __name__ == "__main__":
    run()