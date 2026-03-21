"""
Field Classification Module (Refined Version)
Logic: 
  1. Statistical Merit: SQL if Stable (>=95%) AND Dense (>=70%).
  2. The Unknown Gate: UNKNOWN if Frequency < 10%.
  3. Structural Pruning: If Depth > 2, the field and ALL children go to MONGO.
"""

import json
import os
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from src.config import DATA_DIR, METADATA_FILE

@dataclass
class FieldStats:
    fieldName: str
    frequency: float
    dominantType: str
    typeStability: float
    cardinality: float
    isNested: bool
    isArray: bool
    nestingDepth: int = 0
    parentPath: Optional[str] = None

class SchemaClassifier:
    def __init__(self, user_schema: Dict = None):
        self.user_schema = user_schema or {}

    def classify_statistically(self, field: FieldStats) -> Dict[str, Any]:
        """
        Phase 1: Determine decision based ONLY on statistics and frequency.
        """
        # 1. The Unknown Gate (Rare Data)
        if field.frequency < 0.01:
            return {
                "decision": "UNKNOWN",
                "confidence": round(1.0 - field.frequency, 3),
                "reason": f"Rare field (seen in only {field.frequency:.1%})"
            }

        # 2. Statistical Merit (Stable & Dense)
        # We define SQL-worthy as: Type stays the same AND it's usually present.
        is_stable = field.typeStability >= 1.0
        is_dense = field.frequency >= 0.50

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

def runPipeline():
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

    # --- PASS 2: STRUCTURAL PRUNING (DEPTH > 2) ---
    # Logic: If a field is deep, it and its children are forced to MONGO.
    for field in analyzed_data['fields']:
        depth = field.get('nesting_depth', 0)
        path = field['field_name']

        # Check if the field itself is too deep
        if depth > 2:
            field['decision'] = "MONGO"
            field['reason'] = f"Exiled: Nesting Depth {depth} > 2"
            field['confidence'] = 1.0
            continue

        # Check if any part of the parent path was already exiled due to depth
        # Example: if 'a.b.c' is Depth 3, then 'a.b.c.d' must also be Mongo.
        parts = path.split('.')
        for i in range(1, len(parts)):
            parent_path = ".".join(parts[:i])
            parent_meta = field_dict.get(parent_path)
            
            if parent_meta and parent_meta.get('nesting_depth', 0) > 2:
                field['decision'] = "MONGO"
                field['reason'] = f"Inherited Exile from deep parent ({parent_path})"
                field['confidence'] = 1.0
                break

    # --- FINAL SUMMARY & OUTPUT ---
    sql_count = 0
    mongo_count = 0
    unknown_count = 0

    print("\n" + "="*80)
    print("HYBRID CLASSIFICATION RESULTS")
    print("="*80)
    print(f"{'Field':<40} {'Decision':<10} {'Reason'}")
    print("-"*80)
    
    for field in analyzed_data['fields']:
        dec = field['decision']
        if dec == 'SQL': sql_count += 1
        elif dec == 'MONGO': mongo_count += 1
        else: unknown_count += 1
        
        print(f"{field['field_name']:<40} {dec:<10} {field.get('reason', '')}")
    
    print("-"*80)
    print(f"SQL: {sql_count} | Mongo: {mongo_count} | Unknown: {unknown_count}")
    print("="*80)

    with open(METADATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(analyzed_data, f, indent=4)
    print(f"\n[+] Metadata finalized at {METADATA_FILE}")

def run_classification():
    runPipeline()

if __name__ == "__main__":
    run_classification()