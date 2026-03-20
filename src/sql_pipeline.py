"""
SQL Pipeline Orchestrator
Purpose: Integrate SQL engine into the main data pipeline.

Responsibilities:
1. Schema creation from metadata.json
2. Data loading and bulk insertion
3. Pipeline summary and statistics
"""

import os
import sys
import argparse
import json
from typing import Tuple

from config import SQL_DATA_FILE, DATA_DIR, DATABASE_URL, METADATA_FILE
from sql_engine import SQLEngine


def archive_processed_data(source_file: str, archive_file: str):
    """Archive processed data from source to archive file, then clear source"""
    if not os.path.exists(source_file):
        return
    
    try:
        # Read data from sql_data.json
        with open(source_file, 'r') as f:
            data = json.load(f)
        
        # Read existing archive or start fresh
        archive_data = []
        if os.path.exists(archive_file):
            with open(archive_file, 'r') as f:
                archive_data = json.load(f)
        
        # Append new data to archive
        if isinstance(data, list):
            archive_data.extend(data)
        else:
            archive_data.append(data)
        
        # Write to archive
        with open(archive_file, 'w') as f:
            json.dump(archive_data, f, indent=2)
        
        # Clear source file
        with open(source_file, 'w') as f:
            json.dump([], f)
        
        print(f"[+] Archived {len(archive_data) if isinstance(archive_data, list) else 1} records to {archive_file}")
    except Exception as e:
        print(f"[!] Error archiving data: {e}")


def run_sql_pipeline(engine: SQLEngine) -> Tuple[int, int]:
    """
    Run the complete SQL pipeline:
    1. Schema creation
    2. Data loading from sql_data.json
    
    Returns:
        (successful_inserts, failed_inserts)
    """
    print("\n" + "=" * 80)
    print("SQL PIPELINE ORCHESTRATOR")
    print("=" * 80)
    
    # Step 1: Initialize schema
    print("\n[STEP 1] Initializing SQL Schema...")
    if not engine.initialize():
        print("[!] Failed to initialize SQL Engine")
        return 0, 1
    
    stats = engine.get_database_stats()
    print(f"[+] Database initialized with {len(stats)} tables")
    
    # Step 2: Check for data file
    print("\n[STEP 2] Loading SQL Data...")
    if not os.path.exists(SQL_DATA_FILE):
        print(f"[!] No SQL data file found at {SQL_DATA_FILE}")
        print("[*] Run the ingestion and classification pipeline first.")
        return 0, 0
    
    file_size = os.path.getsize(SQL_DATA_FILE) / (1024 * 1024)
    print(f"[*] Loading {file_size:.2f} MB...")
    
    # Step 3: Bulk insert
    print("\n[STEP 3] Bulk Inserting Records...")
    success_count, fail_count = engine.bulk_insert_from_file(SQL_DATA_FILE)
    
    # Step 4: Archive processed data
    if success_count > 0:
        print("\n[STEP 4] Archiving Processed Data...")
        archive_file = os.path.join(DATA_DIR, "data_till_now_sql.json")
        archive_processed_data(SQL_DATA_FILE, archive_file)
    
    # Step 5: Summary
    print("\n" + "=" * 80)
    print("SQL PIPELINE SUMMARY")
    print("=" * 80)
    
    final_stats = engine.get_database_stats()
    total_records = sum(final_stats.values())
    
    print(f"\nDatabase: {DATABASE_URL}")
    print(f"\nLoad Results:")
    print(f"  Successful Inserts: {success_count}")
    print(f"  Failed Inserts: {fail_count}")
    print(f"  Total Processed: {success_count + fail_count}")
    
    print(f"\nFinal Database State:")
    for table_name, count in sorted(final_stats.items()):
        print(f"  {table_name:<30} {count:>10} records")
    
    print(f"\nTotal Records in Database: {total_records}")
    print("=" * 80)
    
    return success_count, fail_count


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="SQL Pipeline - Load data into SQL database")
    parser.add_argument(
        'command',
        nargs='?',
        choices=['run', 'init', 'status'],
        default='run',
        help='Command to execute'
    )
    parser.add_argument('--database-url', help='Custom database URL', default=None)
    
    args = parser.parse_args()
    engine = SQLEngine(database_url=args.database_url)
    
    try:
        if args.command == 'init':
            # Initialize schema only
            print("\n" + "=" * 80)
            print("SQL SCHEMA INITIALIZATION")
            print("=" * 80)
            
            if not os.path.exists(METADATA_FILE):
                print(f"[!] Error: {METADATA_FILE} not found")
                print("[!] Run 'python main.py initialise' first to generate metadata.")
                sys.exit(1)
            
            if engine.initialize():
                stats = engine.get_database_stats()
                print(f"\n[+] Schema initialized with {len(stats)} tables")
                for table_name, count in stats.items():
                    print(f"    {table_name}: {count} records")
            else:
                print("[!] Schema initialization failed")
                sys.exit(1)
        
        elif args.command == 'status':
            # Show database status
            print("\n" + "=" * 80)
            print("SQL DATABASE STATUS")
            print("=" * 80)
            
            if engine.initialize():
                stats = engine.get_database_stats()
                print(f"\nDatabase: {DATABASE_URL}")
                print(f"\nTable Statistics:")
                total = sum(stats.values())
                for table_name, count in sorted(stats.items()):
                    print(f"  {table_name:<30} {count:>10} records")
                print(f"\nTotal Records: {total}")
            else:
                print("[!] Failed to connect to database")
                sys.exit(1)
        
        else:  # run (default)
            success, fail = run_sql_pipeline(engine)
            sys.exit(0 if success > 0 or fail == 0 else 1)
    
    finally:
        engine.close()


if __name__ == "__main__":
    main()
