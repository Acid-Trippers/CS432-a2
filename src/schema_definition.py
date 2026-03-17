VALID_TYPES = ["string", "int", "float", "bool", "json", "array_int", "array_string", "array_float", "array_bool"]

import json
import os
import sys

def get_guided_input():
    schema = {}
    print("\n--- Guided Schema Entry ---")
    print("Enter fields in the format: name,type,unique(y/n),not_null(y/n)")
    print("Valid types: string, int, float, bool, json, array_int, array_string, array_float, array_bool")
    print("Example: user_id,int,y,y")
    print("Type 'done' when finished.\n")

    while True:
        entry = input(f"Field {len(schema) + 1}: ").strip().lower()
        if entry == 'done':
            if not schema:
                print("[!] You must define at least one field.")
                continue
            break
        
        try:
            parts = [p.strip() for p in entry.split(",")]
            if len(parts) != 4:
                print("[!] Invalid format. Expected: name,type,unique,not_null")
                continue

            name, dtype, uniq, nn = parts
            
            if dtype not in VALID_TYPES:
                print(f"[!] Invalid type. Choose from: {', '.join(VALID_TYPES)}")
                continue

            if uniq not in ['y', 'n'] or nn not in ['y', 'n']:
                print("[!] unique and not_null must be 'y' or 'n'")
                continue

            schema[name] = {
                "type": dtype,
                "unique": uniq == 'y',
                "not_null": nn == 'y'
            }
        except Exception as e:
            print(f"[!] Error: {e}. Try again.")

    return schema

import json

def get_pasted_json():
    print("\n--- JSON Paste Mode ---")
    print("Paste your JSON schema below. When finished, press Enter, then Ctrl+D (Linux/Mac) or Ctrl+Z (Windows) and Enter.")
    print("--------------------------------------------------")
    
    try:
        # This reads the entire block of text from the clipboard/terminal
        raw_data = sys.stdin.read() 
        data = json.loads(raw_data)
        return data
    except json.JSONDecodeError as e:
        print(f"\n[!] Invalid JSON format: {e}")
        return None

def main():
    print("=== Database Pipeline Setup ===")
    print("1. Guided Entry (name,type,unique,not_null)")
    print("2. Paste Raw JSON")
    
    choice = input("\nSelect an option [1/2]: ").strip()

    if choice == '2':
        import sys
        schema = get_pasted_json()
        if not schema:
            print("[!] Paste failed or was empty. Falling back to Guided Entry...")
            schema = get_guided_input() # The comma-separated version we wrote earlier
    else:
        schema = get_guided_input()

    with open("initial_schema.json", "w") as f:
        json.dump(schema, f, indent=4)
    
    print("\n[+] initial_schema.json has been saved.")

if __name__ == "__main__":
    main()