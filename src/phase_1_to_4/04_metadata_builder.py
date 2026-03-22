"""
Consolidates user-defined schemas with statistically analyzed fields into a unified metadata playbook.

- Transforms nested initial schemas into flat, dot-notation path mappings.
- Extracts expected intended data types directly from the user's base template.
- Merges user-defined constraints with the statistically generated field properties.
- Flags discovered but unmapped buffer variables, instructing the classifier to use heuristics.
- Outputs the finalized schema property mapping to `metadata.json`.
"""

import json
import os
from src.config import INITIAL_SCHEMA_FILE, ANALYZED_SCHEMA_FILE, METADATA_FILE

def merge_metadata(is_update=False, n_old=0, n_new=0):
    if not os.path.exists(INITIAL_SCHEMA_FILE):
        print(f"[!] {INITIAL_SCHEMA_FILE} not found.")
        return
    if not os.path.exists(ANALYZED_SCHEMA_FILE):
        print(f"[!] {ANALYZED_SCHEMA_FILE} not found.")
        return

    with open(INITIAL_SCHEMA_FILE, 'r') as f:
        initial_schema = json.load(f)
    
    with open(ANALYZED_SCHEMA_FILE, 'r') as f:
        analyzed_schema = json.load(f)

    # Flatten initial_schema to match analyzed_schema's field-based list
    # Analyzed schema uses dot notation for paths (e.g. "socials.twitter")
    user_constraints = {}
    
    def flatten_schema(schema_node, path=""):
        if isinstance(schema_node, dict):
            if path:
                user_constraints[path] = {"user_type": "object"}
            for k, v in schema_node.items():
                new_path = f"{path}.{k}" if path else k
                flatten_schema(v, new_path)
        elif isinstance(schema_node, list) and schema_node:
            # Array itself
            if path:
                user_constraints[path] = {"user_type": "array"}
            # Array template
            new_path = f"{path}[]"
            flatten_schema(schema_node[0], new_path)
        else:
            # Leaf node (primitive type)
            if path:
                user_constraints[path] = {
                    "user_type": schema_node
                }

    flatten_schema(initial_schema)

    # Merge into analyzed_schema fields
    # 3. Handle Update (Evolution) vs. Initialise (Fresh)
    if is_update and os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'r') as f:
            existing_metadata = json.load(f)
        
        existing_fields = {f['field_name']: f for f in existing_metadata.get('fields', [])}
        
        # Calculate weights based on your ratio suggestion
        total_n = n_old + n_new
        w_old = n_old / total_n if total_n > 0 else 0
        w_new = n_new / total_n if total_n > 0 else 1

        for new_field in analyzed_schema.get('fields', []):
            name = new_field['field_name']
            
            if name in existing_fields:
                # Proportional Update for Frequency and Stability
                old = existing_fields[name]
                old['frequency'] = (old['frequency'] * w_old) + (new_field['frequency'] * w_new)
                old['type_stability'] = (old['type_stability'] * w_old) + (new_field['type_stability'] * w_new)
                
                # Discovery Logic: Graduate if frequency crosses 0.1
                if old['frequency'] >= 0.1:
                    old['is_discovered_buffer'] = False
                else:
                    # Keep in buffer if it's not in the initial_schema
                    old['is_discovered_buffer'] = name not in user_constraints
            else:
                # Brand New Field discovered in this batch
                new_field['user_constraints'] = user_constraints.get(name)
                # Probation check: Is it common enough yet?
                new_field['is_discovered_buffer'] = (new_field['frequency'] < 0.1) and (name not in user_constraints)
                existing_metadata['fields'].append(new_field)
        
        analyzed_schema = existing_metadata
    else:
        # Initialise Mode: Standard mapping with probation flags
        for field in analyzed_schema.get('fields', []):
            name = field['field_name']
            field['user_constraints'] = user_constraints.get(name)
            # Flag for buffer if rare and not in initial schema
            field['is_discovered_buffer'] = (field['frequency'] < 0.1) and (name not in user_constraints)

    with open(METADATA_FILE, 'w') as f:
        json.dump(analyzed_schema, f, indent=4)
        
    print(f"\n[SUCCESS] Pipeline Phase 3c Complete.")
    print(f"[+] Consolidated Metadata: {METADATA_FILE}")

    return analyzed_schema

if __name__ == "__main__":
    merge_metadata()
