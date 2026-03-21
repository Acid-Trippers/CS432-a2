import json
from .config import QUERY_FILE, METADATA_FILE
from src.CRUD_runner import create_operation, read_operation, update_operation, delete_operation

def query_parser():
    """Parse the query.json and interpret what it means."""
    try:
        with open(QUERY_FILE, 'r') as f:
            query = json.load(f)
        
        if not query:
            print("There is no query to execute")
            return None
        # Extract basic fields from query
        operation = query.get("operation")  # READ, UPDATE, DELETE
        entity = query.get("entity")        # employees, departments, etc.
        filters = query.get("filters")      # WHERE clause conditions
        payload = query.get("payload")      # Data to update (if UPDATE operation)
        
        # Print what we extracted
        print(f"\n--- PARSED QUERY ---")
        print(f"Operation: {operation}")
        print(f"Entity: {entity}")
        print(f"Filters: {filters}")
        if payload:
            print(f"Payload: {payload}")
        
        return {
            "operation": operation,
            "entity": entity,
            "filters": filters,
            "payload": payload
        }
        
    except FileNotFoundError:
        print(f"Error: {QUERY_FILE} not found")
        return None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {QUERY_FILE}")
        return None

def get_field_locations():
    """Read metadata.json and create a map of field -> database location."""
    try:
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)
        
        # Build a dictionary: field_name -> decision
        field_map = {}
        for field in metadata.get("fields", []):
            field_name = field.get("field_name")
            decision = field.get("decision")  # SQL, MongoDB, Unknown, etc.
            field_map[field_name] = decision
        
        return field_map
    
    except FileNotFoundError:
        print(f"Error: {METADATA_FILE} not found")
        return {}
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {METADATA_FILE}")
        return {}

def analyze_query_databases(parsed_query):
    """Analyze which databases we need to query based on filter fields."""
    filters = parsed_query.get("filters", {})
    field_map = get_field_locations()
    
    # Categorize each filter field by its database
    field_locations = {}
    for field_name in filters.keys():
        location = field_map.get(field_name, "Unknown")
        field_locations[field_name] = location
    
    # Determine which databases to query
    databases_needed = set()
    for location in field_locations.values():
        databases_needed.add(location)
    
    print(f"\n--- FIELD LOCATIONS ---")
    for field, location in field_locations.items():
        print(f"{field}: {location}")
    
    print(f"\n--- DATABASES NEEDED ---")
    print(f"Databases: {', '.join(databases_needed)}")
    
    return {
        "field_locations": field_locations,
        "databases_needed": list(databases_needed)
    }
    
def query_checker():
    parsed_query = query_parser()
    
    if parsed_query:
        db_analysis = analyze_query_databases(parsed_query)
        print(f"\nAnalysis Result: {db_analysis}")
        
    match parsed_query["operation"]:
        case "CREATE":
            create_operation(parsed_query, db_analysis)
            print("Sucess")
        case "READ":
            read_operation(parsed_query, db_analysis)
            print("Sucess")
        case "UPDATE":
            update_operation(parsed_query, db_analysis)
            print("Sucess")
        case "DELETE":
            delete_operation(parsed_query, db_analysis)
            print("Sucess")

if __name__ == "__main__":
    query_checker()
        
        