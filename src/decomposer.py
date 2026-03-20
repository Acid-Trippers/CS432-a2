"""
Field Decomposition Module
Purpose: Maps nested JSON structures to Relational SQL patterns.

Logic:
  - Applies a Depth <= 2 constraint for normalization.
  - Pattern 1 (1-to-1): Nested 'OBJECT' becomes a separate table.
  - Pattern 2 (1-to-N): 'ARRAY_OF_OBJECTS' becomes a separate table for rows.
  - Pattern 3 (Junction): 'ARRAY_OF_PRIMITIVES' becomes a bridge table.
  - Depth > 2: Data is collapsed into 'native_storage' (JSON/String).

Example:
  {person: {address: {house: ""}}} 
  -> rel_person (Table) 
  -> rel_person_address (Table) 
  -> house (Column in rel_person_address)
"""

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

            stype = decision_entry.get('structuralType', 'SCALAR')
            is_sql_candidate = decision_entry.get('decision') == 'SQL'
            
            # Identify immediate parent for relational linking
            parent_path = field.get('parent_path')
            if parent_path:
                parent_table = f"rel_{parent_path.replace('.', '_')}"
            else:
                parent_table = "main_records"

            # ENFORCE DEPTH & SQL STRATEGY
            if is_sql_candidate and depth <= 2:
                
                # PATTERN 1: Nested Dictionary (1-to-1)
                if stype == "OBJECT":
                    decision_entry["decomposition_strategy"] = "separate_table_1_to_1"
                    decision_entry["table_config"] = {
                        "table_name": f"rel_{fname.replace('.', '_')}",
                        "relationship": "ONE_TO_ONE",
                        "parent_table": parent_table
                    }

                # PATTERN 2: Array of Dictionaries (1-to-N)
                elif stype == "ARRAY_OF_OBJECTS":
                    decision_entry["decomposition_strategy"] = "separate_table_1_to_N"
                    decision_entry["table_config"] = {
                        "table_name": f"rel_{fname.replace('.', '_')}",
                        "relationship": "ONE_TO_MANY",
                        "parent_table": parent_table
                    }

                # PATTERN 3: Array of Primitives (Junction Table)
                elif stype == "ARRAY_OF_PRIMITIVES":
                    decision_entry["decomposition_strategy"] = "junction_table"
                    decision_entry["table_config"] = {
                        "table_name": f"jt_{fname.replace('.', '_')}",
                        "relationship": "MANY_TO_MANY_EMULATED",
                        "parent_table": parent_table
                    }

                # DEFAULT: Scalar Leaf Node (Direct Column)
                else:
                    decision_entry["decomposition_strategy"] = "direct_column"
                    decision_entry["table_config"] = {
                        "target_table": parent_table
                    }

            # OVERRIDE: Depth > 2 or Mongo Decision
            else:
                if is_sql_candidate and depth > 2:
                    decision_entry["reason"] = f"OVERRIDE: SQL candidate demoted. Depth {depth} > 2 limit."
                
                decision_entry["decomposition_strategy"] = "native_storage"
                decision_entry["table_config"] = None

            updated_results.append(decision_entry)

        with open(FIELD_METADATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(updated_results, f, indent=2)

        print(f"\n" + "="*50)
        print(f"SUCCESS: Decomposition completed.")
        print(f"Relational patterns applied for Depth <= 2.")
        print(f"Processed {len(updated_results)} fields.")
        print("="*50)

if __name__ == "__main__":
    SQLDecomposer().run_decomposition()