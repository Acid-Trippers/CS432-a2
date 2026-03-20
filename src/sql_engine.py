"""
SQL Engine Module
Purpose: Handle data normalization, CRUD operations, and nested data decomposition.

Responsibilities:
1. Normalize nested JSON data into relational SQL tables
2. Handle array decomposition
3. Manage primary/foreign key relationships
4. Insert, read operations
5. Handle bulk inserts from SQL data
"""

import json
import os
from typing import List, Dict, Any, Optional, Tuple
import logging

from sqlalchemy import inspect
from sqlalchemy.orm import Session

from config import DATA_DIR, METADATA_FILE
from sql_schema_definer import SQLSchemaBuilder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataNormalizer:
    """Handles normalization of nested data for SQL storage"""
    
    def __init__(self):
        self.metadata = {}
    
    def load_metadata(self):
        """Load metadata.json"""
        if not os.path.exists(METADATA_FILE):
            logger.warning(f"Metadata file not found: {METADATA_FILE}")
            return
        
        with open(METADATA_FILE, 'r') as f:
            data = json.load(f)
            self.metadata = {
                field['field_name']: field 
                for field in data.get('fields', [])
            }
    
    def normalize_record(self, record: Dict) -> Tuple[Dict, Dict[str, List[Dict]]]:
        """
        Decompose a record into root and nested data.
        Only processes fields that exist in metadata.
        
        Returns:
            (root_data, nested_data_by_table)
        """
        root_data = {}
        nested_data = {}
        
        for field_name, value in record.items():
            # Skip fields not in metadata
            if field_name not in self.metadata:
                continue
                
            field_meta = self.metadata[field_name]
            is_nested = field_meta.get('is_nested', False)
            is_array = field_meta.get('is_array', False)
            
            if is_array:
                # Handle array fields
                if value is None:
                    continue
                    
                table_name = f"main_records_{field_name}".replace('.', '_')
                items = value if isinstance(value, list) else [value]
                
                if field_meta.get('array_content_type') == 'object':
                    # Array of objects - store as JSON
                    nested_data[table_name] = [
                        {'data': json.dumps(item) if not isinstance(item, str) else item, 'position': idx}
                        for idx, item in enumerate(items)
                    ]
                else:
                    # Array of primitives
                    nested_data[table_name] = [
                        {
                            'value': str(item),
                            'value_type': type(item).__name__.lower(),
                            'position': idx
                        }
                        for idx, item in enumerate(items)
                    ]
            
            elif is_nested:
                # Handle nested objects
                if value is None:
                    continue
                    
                table_name = f"main_records_{field_name}".replace('.', '_')
                if isinstance(value, dict):
                    nested_data[table_name] = [value]
            
            else:
                # Root-level field - include even if None (let DB handle NULL)
                root_data[field_name] = value
        
        return root_data, nested_data


class SQLEngine:
    """Core SQL operations and data management"""
    
    def __init__(self, database_url: str = None):
        self.schema_builder = SQLSchemaBuilder(database_url=database_url)
        self.normalizer = DataNormalizer()
        self.session: Optional[Session] = None
        self.models = {}
        self.table_relationships = {}
    
    def initialize(self) -> bool:
        """Initialize SQL engine and database schema"""
        try:
            logger.info("Initializing SQL Engine...")
            self.schema_builder.analyze_and_build()
            self.models = self.schema_builder.get_models()
            self.normalizer.load_metadata()
            self._build_relationships()
            self.session = self.schema_builder.get_session()
            logger.info("SQL Engine initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize SQL Engine: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _build_relationships(self):
        """Build parent-child relationship map"""
        for table_name in self.models.keys():
            if table_name.startswith('main_records_'):
                self.table_relationships[table_name] = 'main_records'
    
    def insert_record(self, record: Dict) -> Optional[int]:
        """Insert a record with all its nested data"""
        try:
            root_data, nested_data = self.normalizer.normalize_record(record)
            
            MainRecords = self.models.get('main_records')
            if not MainRecords:
                logger.error("MainRecords model not found")
                return None
            
            # Insert main record
            main_record = MainRecords(**root_data)
            self.session.add(main_record)
            self.session.flush()
            record_id = main_record.id
            
            # Insert nested records
            for table_name, nested_records in nested_data.items():
                NestedModel = self.models.get(table_name)
                if not NestedModel:
                    logger.warning(f"Model for {table_name} not found, skipping")
                    continue
                
                fk_column = 'main_records_id'
                for nested_record in nested_records:
                    nested_record[fk_column] = record_id
                    nested_obj = NestedModel(**nested_record)
                    self.session.add(nested_obj)
            
            self.session.commit()
            logger.info(f"Inserted record with ID {record_id}")
            return record_id
            
        except Exception as e:
            logger.error(f"Error inserting record: {e}")
            self.session.rollback()
            import traceback
            traceback.print_exc()
            return None
    
    def bulk_insert_from_file(self, json_file: str) -> Tuple[int, int]:
        """Bulk insert records from JSON file"""
        if not os.path.exists(json_file):
            logger.error(f"File not found: {json_file}")
            return 0, 0
        
        try:
            with open(json_file, 'r') as f:
                records = json.load(f)
            
            if not isinstance(records, list):
                logger.error("JSON file must contain a list of records")
                return 0, 1
            
            success_count = 0
            fail_count = 0
            
            logger.info(f"Starting bulk insert of {len(records)} records...")
            
            for idx, record in enumerate(records):
                record_id = self.insert_record(record)
                if record_id:
                    success_count += 1
                else:
                    fail_count += 1
                
                if (idx + 1) % 100 == 0:
                    logger.info(f"  Processed {idx + 1}/{len(records)} records...")
            
            logger.info(f"Bulk insert complete: {success_count} success, {fail_count} failed")
            return success_count, fail_count
            
        except Exception as e:
            logger.error(f"Bulk insert failed: {e}")
            return 0, 1
    
    def query_all(self, table_name: str = 'main_records', limit: int = 100) -> List[Dict]:
        """Query records from a table"""
        try:
            Model = self.models.get(table_name)
            if not Model:
                logger.error(f"Table {table_name} not found")
                return []
            
            records = self.session.query(Model).limit(limit).all()
            result = []
            for record in records:
                result.append({col.name: getattr(record, col.name) for col in inspect(Model).columns})
            return result
            
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return []
    
    def get_table_count(self, table_name: str = 'main_records') -> int:
        """Get row count for a table"""
        try:
            Model = self.models.get(table_name)
            if not Model:
                return 0
            return self.session.query(Model).count()
        except Exception as e:
            logger.error(f"Count query failed: {e}")
            return 0
    
    def get_database_stats(self) -> Dict[str, int]:
        """Get statistics for all tables"""
        return {table_name: self.get_table_count(table_name) for table_name in self.models.keys()}
    
    def close(self):
        """Close database connection"""
        if self.session:
            self.session.close()
            logger.info("Database connection closed")



def run_sql_engine_demo():
    """Demo/test the SQL engine"""
    print("\n" + "=" * 80)
    print("SQL ENGINE DEMO")
    print("=" * 80)
    
    engine = SQLEngine()
    if not engine.initialize():
        print("[!] Failed to initialize SQL Engine")
        return
    
    print("[+] SQL Engine initialized")
    
    # Show database stats
    stats = engine.get_database_stats()
    print(f"\n[INFO] Database stats:")
    for table_name, count in stats.items():
        print(f"  {table_name}: {count} records")
    
    engine.close()


if __name__ == "__main__":
    run_sql_engine_demo()
