import json
import os
from config import METADATA_MANAGER_FILE, FIELD_METADATA_FILE

class SQLDecomposer:
    def __init__(self):
        self.field_stats_lookup = {}
        self.metadata_structure = []

    def _load_data(self):
        if not os.path.exists(FIELD_METADATA_FILE) or not os.path.exists(METADATA_MANAGER_FILE):
            print(f"[!] Error: One or both metadata files are missing.")
            return False
            
        with open(FIELD_METADATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            self.field_stats_lookup = {item['fieldName']: item for item in data}
            
        with open(METADATA_MANAGER_FILE, 'r', encoding='utf-8') as f:
            manager_data = json.load(f)
            self.metadata_structure = manager_data.get('fields', [])
        return True

    def run_decomposition(self):
        if not self._load_data():
            return

        updated_results = []

        for field in self.metadata_structure:
            fname = field['field_name']
            depth = field['nesting_depth']
            decision_entry = self.field_stats_lookup.get(fname)
            
            if not decision_entry:
                continue

            # Check for the Depth Override Case first
            is_sql_candidate = decision_entry.get('decision') == 'SQL'
            
            if is_sql_candidate and depth <= 2:
                # Case 1: The field is an object/nested structure -> This becomes a TABLE
                if field.get('is_nested', False):
                    decision_entry["decomposition_strategy"] = "separate_table"
                    decision_entry["table_config"] = {
                        "table_name": f"rel_{fname.replace('.', '_')}",
                        "nesting_level": depth,
                        "parent_table": f"rel_{field['parent_path'].replace('.', '_')}" if field.get('parent_path') else "main_records"
                    }
                
                # Case 2: The field is a leaf node -> This becomes a COLUMN in its parent's table
                else:
                    decision_entry["decomposition_strategy"] = "direct_column"
                    parent_path = field.get('parent_path')
                    decision_entry["table_config"] = {
                        "target_table": f"rel_{parent_path.replace('.', '_')}" if parent_path else "main_records"
                    }
            
            # Case 3: Data is depth > 2 OR it was already MONGO/UNKNOWN
            else:
                # Log the reason for the override if it was originally SQL
                if is_sql_candidate and depth > 2:
                    decision_entry["reason"] = f"OVERRIDE: Field is SQL-stable but Depth {depth} > 2. Moved to native_storage to prevent deep joins."
                
                decision_entry["decomposition_strategy"] = "native_storage"
                decision_entry["table_config"] = None

            updated_results.append(decision_entry)

        with open(FIELD_METADATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(updated_results, f, indent=2)

        print(f"\n[SUCCESS] Decomposition logic applied.")
        print(f"[*] Depth limit (<=2) enforced. Results saved to: {FIELD_METADATA_FILE}")

if __name__ == "__main__":
    SQLDecomposer().run_decomposition()