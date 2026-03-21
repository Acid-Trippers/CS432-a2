"""
SQL Schema Definer Module
Purpose: Define SQL schema with proper PK/FK relationships from metadata.json.

Responsibilities:
1. Load metadata.json (final metadata from classifier)
2. Parse field structures to identify:
   - Root-level fields (become columns in main table)
   - Nested objects (become separate tables with FK relationships)
   - Arrays (array items stored in junction tables)
3. Create SQLAlchemy models
4. Enforce PK/FK constraints
5. Create actual database tables
"""

import json
import os
from typing import Dict, List, Tuple, Any, Optional

try:
    from sqlalchemy import (
        create_engine, Column, Integer, String, Float, Boolean, DateTime,
        ForeignKey, JSON, inspect
    )
    from sqlalchemy.orm import declarative_base, Session
except ImportError:
    print("[!] SQLAlchemy not installed. Install with: pip install sqlalchemy")
    raise

from src.config import DATA_DIR, METADATA_FILE

# Global SQLAlchemy setup
Base = declarative_base()


class SchemaAnalyzer:
    """Analyzes metadata to determine table structure"""
    
    def __init__(self):
        self.metadata = {}
        self.table_hierarchy = {}  # parent -> [children]
        
    def load_schemas(self):
        """Load metadata.json"""
        if not os.path.exists(METADATA_FILE):
            raise FileNotFoundError(f"Metadata file not found: {METADATA_FILE}")
        
        with open(METADATA_FILE, 'r') as f:
            data = json.load(f)
            # Store field metadata indexed by field name
            self.metadata = {
                field['field_name']: field 
                for field in data.get('fields', [])
            }
    
    def _map_python_type_to_sql(self, python_type: str) -> Any:
        """Map Python type to SQLAlchemy type"""
        type_str = str(python_type).lower()
        
        # Direct type mappings
        type_map = {
            "string": String(255),
            "int": Integer,
            "integer": Integer,
            "float": Float,
            "bool": Boolean,
            "boolean": Boolean,
            "datetime": DateTime,
            "date": DateTime,
        }
        
        # Check direct mappings first
        if type_str in type_map:
            return type_map[type_str]
        
        # If it looks like a URL or very long string, use Text
        if type_str.startswith("http://") or type_str.startswith("https://") or len(type_str) > 50:
            return String(500)
        
        # Default to String
        return String(255)
    
    def get_root_fields(self) -> Dict[str, Dict]:
        """Extract root-level fields (not nested objects or arrays)"""
        root_fields = {}
        for field_name, field_meta in self.metadata.items():
            # Skip if nested or array
            if field_meta.get('is_nested') or field_meta.get('is_array'):
                continue
            
            # Make all fields nullable by default - the data contains missing values
            # even for fields marked as required in metadata
            root_fields[field_name] = {
                'type': field_meta.get('dominant_type', 'string'),
                'nullable': True,  # Allow NULL for all fields since data has gaps
            }
        
        return root_fields
    
    def get_nested_objects(self) -> Dict[str, Dict]:
        """Extract nested object fields"""
        nested = {}
        for field_name, field_meta in self.metadata.items():
            if field_meta.get('is_nested') and not field_meta.get('is_array'):
                nested[field_name] = {
                    'type': 'object',
                    'depth': field_meta.get('nesting_depth', 1),
                }
        return nested
    
    def get_arrays(self) -> Dict[str, Dict]:
        """Extract array fields"""
        arrays = {}
        for field_name, field_meta in self.metadata.items():
            if field_meta.get('is_array'):
                arrays[field_name] = {
                    'type': field_meta.get('array_content_type', 'string'),
                    'content_type': 'object' if field_meta.get('array_content_type') == 'object' else 'primitive',
                }
        return arrays
    
    def build_table_hierarchy(self):
        """Build relationship graph of tables"""
        # All root fields go in main_records table
        self.table_hierarchy['main_records'] = []
        
        # Nested objects become child tables
        nested = self.get_nested_objects()
        for field_name in nested.keys():
            table_name = f"main_records_{field_name}".replace('.', '_')
            self.table_hierarchy['main_records'].append({
                'field_name': field_name,
                'type': 'nested_object',
                'table_name': table_name
            })
        
        # Arrays become junction tables
        arrays = self.get_arrays()
        for field_name in arrays.keys():
            table_name = f"main_records_{field_name}".replace('.', '_')
            self.table_hierarchy['main_records'].append({
                'field_name': field_name,
                'type': 'array',
                'table_name': table_name
            })


class SQLSchemaBuilder:
    """Builds SQLAlchemy models and creates database schema"""
    
    def __init__(self, database_url: str = None):
        if database_url is None:
            db_path = os.path.join(DATA_DIR, "engine.db")
            database_url = f"sqlite:///{db_path}"
        
        self.database_url = database_url
        self.engine = None
        self.analyzer = SchemaAnalyzer()
        self.models = {}  # table_name -> SQLAlchemy model
    
    def analyze_and_build(self):
        """Load metadata, analyze structure, and build models"""
        print("[*] Loading metadata...")
        self.analyzer.load_schemas()
        
        print("[*] Building table hierarchy...")
        self.analyzer.build_table_hierarchy()
        
        print("[*] Creating SQLAlchemy models...")
        self._create_models()
        
        print("[*] Creating database engine...")
        self.engine = create_engine(self.database_url, echo=False)
        
        print("[*] Creating database tables...")
        self._create_tables()
    
    def _create_models(self):
        """Dynamically create SQLAlchemy models"""
        # 1. Create main_records table
        self._create_main_table()
        
        # 2. Create nested object tables
        nested = self.analyzer.get_nested_objects()
        for field_name in nested.keys():
            self._create_nested_table(field_name)
        
        # 3. Create array tables
        arrays = self.analyzer.get_arrays()
        for field_name in arrays.keys():
            self._create_array_table(field_name)
    
    def _create_main_table(self):
        """Create main_records table from root fields"""
        root_fields = self.analyzer.get_root_fields()
        
        attrs = {
            '__tablename__': 'main_records',
            'id': Column(Integer, primary_key=True, autoincrement=True),
        }
        
        for field_name, info in root_fields.items():
            sql_type = self.analyzer._map_python_type_to_sql(info['type'])
            attrs[field_name] = Column(
                sql_type,
                nullable=info['nullable']
            )
        
        MainRecords = type('MainRecords', (Base,), attrs)
        self.models['main_records'] = MainRecords
    
    def _create_nested_table(self, field_name: str):
        """Create a table for a nested object"""
        table_name = f"main_records_{field_name}".replace('.', '_')
        
        attrs = {
            '__tablename__': table_name,
            'id': Column(Integer, primary_key=True, autoincrement=True),
            'main_records_id': Column(Integer, ForeignKey('main_records.id'), nullable=False),
        }
        
        NestedTable = type(f'{table_name.capitalize()}', (Base,), attrs)
        self.models[table_name] = NestedTable
    
    def _create_array_table(self, field_name: str):
        """Create a table for an array"""
        table_name = f"main_records_{field_name}".replace('.', '_')
        
        attrs = {
            '__tablename__': table_name,
            'id': Column(Integer, primary_key=True, autoincrement=True),
            'main_records_id': Column(Integer, ForeignKey('main_records.id'), nullable=False),
            'position': Column(Integer, nullable=True),  # Maintain array order
        }
        
        # Check if array contains objects or primitives
        field_meta = self.analyzer.metadata.get(field_name, {})
        if field_meta.get('array_content_type') == 'object':
            # Store as JSON for object arrays
            attrs['data'] = Column(JSON, nullable=False)
        else:
            # Store as string for primitive arrays
            attrs['value'] = Column(String(255), nullable=False)
            attrs['value_type'] = Column(String(50), nullable=True)
        
        ArrayTable = type(f'{table_name.capitalize()}', (Base,), attrs)
        self.models[table_name] = ArrayTable
    
    def _create_tables(self):
        """Create actual database tables"""
        Base.metadata.create_all(self.engine)
        print(f"[+] Database schema created at: {self.database_url}")
        
        # Print schema summary
        inspector = inspect(self.engine)
        print("\n[INFO] Created tables:")
        for table_name in inspector.get_table_names():
            columns = inspector.get_columns(table_name)
            print(f"  - {table_name}: {[col['name'] for col in columns]}")
    
    def get_session(self):
        """Get SQLAlchemy session for data operations"""
        from sqlalchemy.orm import sessionmaker
        SessionLocal = sessionmaker(bind=self.engine)
        return SessionLocal()
    
    def get_models(self):
        """Return dictionary of all models"""
        return self.models


def run_schema_definition():
    """Main entry point to define SQL schema"""
    print("\n" + "=" * 80)
    print("SQL SCHEMA DEFINER")
    print("=" * 80)
    
    try:
        builder = SQLSchemaBuilder()
        builder.analyze_and_build()
        
        print("\n" + "=" * 80)
        print("[SUCCESS] SQL Schema definition complete!")
        print("=" * 80)
        print(f"\nDatabase URL: {builder.database_url}")
        print(f"Models available: {list(builder.models.keys())}")
        
        return builder
        
    except FileNotFoundError as e:
        print(f"[!] Error: {e}")
        print("[!] Please ensure metadata.json exists (run 'python main.py initialise' first).")
        return None
    except Exception as e:
        print(f"[!] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    run_schema_definition()
