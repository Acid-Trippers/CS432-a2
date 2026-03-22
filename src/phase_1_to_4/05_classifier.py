"""
Evaluates merged schema metadata to algorithmically route fields into the optimal database pipeline.

- Grades fields based on historical appearance frequency and type stability to identify SQL candidates.
- Quarantines highly sparse outlier fields into an UNKNOWN buffer for manual review.
- Prunes relational complexity by forcing deeply nested objects (Depth > 2) into the MongoDB pipeline.
- Overrides basic statistical routing decisions with structural limitations and rules.
- Embeds the final routing choices (SQL/MONGO/UNKNOWN) backward into `metadata.json`.
"""

import json
import os
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from src.config import DATA_DIR, METADATA_FILE

CLASSIFIER_CONFIG = {
    "rare_field_threshold": 0.01,
    "type_stability_threshold": 1.0,
    "density_threshold": 0.50,
    "max_internal_nesting": 2
}

@dataclass
class FieldStats:
    fieldName: str
    frequency: float
    dominantType: str
    typeStability: float
    cardinality: float
    isNested: bool
    isArray: bool
    is_discovered_buffer: bool = False
    nestingDepth: int = 0
    parentPath: Optional[str] = None

class SchemaClassifier:
    def __init__(self, user_schema: Dict = None):
        self.user_schema = user_schema or {}

    def classify_statistically(self, field: FieldStats) -> Dict[str, Any]:
        """
        Phase 1: Determine decision based on statistics and discovery flags.
        """
        # Fix: Use attribute access on the dataclass
        if field.is_discovered_buffer is True:
            return {
                "decision": "BUFFER",
                "confidence": 1.0,
                "reason": "Probation: Frequency < 0.1 (Discovery Logic)"
            }

        # Existing frequency logic (Rare Data)
        if field.frequency < CLASSIFIER_CONFIG["rare_field_threshold"]:
            return {
                "decision": "UNKNOWN",
                "confidence": round(1.0 - field.frequency, 3),
                "reason": f"Rare field (seen in only {field.frequency:.1%})"
            }

        # Restoring the SQL vs MONGO logic
        is_stable = field.typeStability >= CLASSIFIER_CONFIG["type_stability_threshold"]
        is_dense = field.frequency >= CLASSIFIER_CONFIG["density_threshold"]

        if is_stable and is_dense:
            return {
                "decision": "SQL",
                "confidence": round((field.typeStability + field.frequency) / 2, 3),
                "reason": "Stable and Dense (Statistical Merit)"
            }
        else:
            return {
                "decision": "MONGO",
                "confidence": 0.8,
                "reason": f"Unstable or Sparse (Freq: {field.frequency:.1%}, Stability: {field.typeStability:.1%})"
            }
        
def runPipeline(verbose=True):
    if not os.path.exists(METADATA_FILE):
        print(f"[X] ERROR: {METADATA_FILE} not found.")
        return

    with open(METADATA_FILE, 'r', encoding='utf-8') as f:
        analyzed_data = json.load(f)

    classifier = SchemaClassifier()
    field_dict = {}

    # --- PASS 1: INDIVIDUAL STATISTICAL EVALUATION ---
    for field in analyzed_data['fields']:
        stats = FieldStats(
            fieldName=field['field_name'],
            frequency=field['frequency'],
            dominantType=field['dominant_type'],
            typeStability=field['type_stability'],
            cardinality=field['cardinality'],
            isNested=field['is_nested'],
            isArray=field['is_array'],
            is_discovered_buffer=field.get('is_discovered_buffer', False),
            nestingDepth=field.get('nesting_depth', 0),
            parentPath=field.get('parent_path', None)
        )
        
        # Get decision based purely on numbers (ignoring structure for now)
        result = classifier.classify_statistically(stats)
        
        field['decision'] = result['decision']
        field['confidence'] = result['confidence']
        field['reason'] = result['reason']
        
        # Store in dict for Phase 2 lookups
        field_dict[field['field_name']] = field

    # --- PASS 2: STRUCTURAL DELINEATION (DEEP NESTING) ---
    # Logic: If a field contains more than 2 levels of nesting *inside it*, 
    # the entire field and ALL of its children are routed to MONGO to prevent fragmenting 
    # deep documents across SQL tables.
    
    # First, calculate the maximum depth of any descendant for each field
    max_descendant_depth = {}
    for field in analyzed_data['fields']:
        path = field['field_name']
        depth = field.get('nesting_depth', 0)
        
        # Every field is its own descendant of depth `depth`
        max_descendant_depth[path] = depth
        
        # Update your parents' max depths
        parts = path.split('.')
        # Handle array brackets in paths (if any) while resolving parents
        # This simple loop registers the max depth backwards up the chain
        for i in range(1, len(parts)):
            parent_path = ".".join(parts[:i])
            parent_path_no_array = parent_path.replace('[]', '')
            
            # Simple parent substring matching
            for p in field_dict:
                if path.startswith(p):
                    max_descendant_depth[p] = max(max_descendant_depth.get(p, 0), depth)

    # Now apply the Mongo exile rule top-down
    for field in analyzed_data['fields']:
        path = field['field_name']
        depth = field.get('nesting_depth', 0)
        max_depth = max_descendant_depth.get(path, depth)
        
        internal_nesting_levels = max_depth - depth
        
        # 1. Does this field have > 2 levels of nesting INSIDE it?
        if internal_nesting_levels > CLASSIFIER_CONFIG["max_internal_nesting"]:
            field['decision'] = "MONGO"
            field['reason'] = f"Exiled: Contains {internal_nesting_levels} levels of deep nesting"
            field['confidence'] = 1.0
            continue
            
        # 2. Is this field a child of something that was already exiled to Mongo?
        # Check parents to see if they were forced into Mongo
        inherited_exile = False
        for p_path, p_meta in field_dict.items():
            if path != p_path and path.startswith(p_path):
                # If the parent was exiled to Mongo due to deep nesting
                p_internal_levels = max_descendant_depth.get(p_path, p_meta.get('nesting_depth', 0)) - p_meta.get('nesting_depth', 0)
                if p_internal_levels > CLASSIFIER_CONFIG["max_internal_nesting"]:
                    field['decision'] = "MONGO"
                    field['reason'] = f"Inherited Exile from deep parent object ({p_path})"
                    field['confidence'] = 1.0
                    inherited_exile = True
                    break
        
        if inherited_exile:
            continue

# --- FINAL SUMMARY & EVOLUTION REPORT ---
    old_decisions = {}
    if os.path.exists(METADATA_FILE):
        try:
            with open(METADATA_FILE, 'r', encoding='utf-8') as f:
                old_meta = json.load(f)
                old_decisions = {f['field_name']: f.get('decision') for f in old_meta.get('fields', [])}
        except: pass

    # MODE A: Full Table (Initialise)
    if verbose:
        print("\n" + "="*80)
        print("HYBRID CLASSIFICATION RESULTS (FULL TABLE)")
        print("="*80)
        print(f"{'Field':<40} {'Decision':<10} {'Reason'}")
        print("-"*80)
        
        counts = {"SQL": 0, "MONGO": 0, "UNKNOWN": 0, "BUFFER": 0}
        for field in analyzed_data['fields']:
            dec = field['decision']
            counts[dec] = counts.get(dec, 0) + 1
            print(f"{field['field_name']:<40} {dec:<10} {field.get('reason', '')}")
        
        print("-"*80)
        print(f"SQL: {counts['SQL']} | Mongo: {counts['MONGO']} | Buffer: {counts['BUFFER']}")
        print("="*80)

    # MODE B: Evolution Only (Fetch)
    else:
        print("\n" + "="*50)
        print("      SCHEMA EVOLUTION REPORT")
        print("="*50)
        changes_found = False
        for field in analyzed_data['fields']:
            name = field['field_name']
            new_dec = field['decision']
            old_dec = old_decisions.get(name)
            
            if old_dec and new_dec != old_dec:
                print(f"[!] GRADUATION: '{name}' {old_dec} -> {new_dec}")
                changes_found = True
            elif not old_dec:
                print(f"[+] NEW FIELD:  '{name}' -> {new_dec}")
                changes_found = True

        if not changes_found:
            print("[~] No schema changes. Evolution stable.")
        print("="*50)

    sql_total = sum(1 for f in analyzed_data['fields'] if f['decision'] == 'SQL')
    mongo_total = sum(1 for f in analyzed_data['fields'] if f['decision'] == 'MONGO')
    buffer_total = sum(1 for f in analyzed_data['fields'] if f['decision'] == 'UNKNOWN')

    print(f"\n[GLOBAL STATE] SQL Columns: {sql_total} | Mongo Fields: {mongo_total} | Buffered: {buffer_total}")
    print("="*50)

    # Final Save
    with open(METADATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(analyzed_data, f, indent=4)

def run_classification(verbose=True):
    runPipeline(verbose=verbose)

if __name__ == "__main__":
    run_classification(verbose=True)