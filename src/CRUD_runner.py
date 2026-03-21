def create_operation(parsed_query, db_analysis):
    return
    
def read_operation(parsed_query, db_analysis):
    return
    
def update_operation(parsed_query, db_analysis):
    return 
    
def delete_operation(parsed_query, db_analysis):
    """
    Execute DELETE operation across relevant databases.
    
    Args:
        parsed_query: Contains operation, entity, filters, payload
        db_analysis: Contains field_locations and databases_needed
    
    Returns:
        dict with status, deleted_count, and details
    """
    
    print(f"\n{'='*60}")
    print("DELETE Operation is running now")
    print(f"{'='*60}")
    
    entity = parsed_query.get("entity")
    filters = parsed_query.get("filters")
    
    databases_needed = db_analysis.get("databases_needed", [])
    field_locations = db_analysis.get("filed_locations", {})
    
    print(f"Entity: {entity}")
    print(f"Filters to apply: {filters}")
    print(f"Databases to query: {databases_needed}")
    
    sql_filters = {}
    mongodb_filters = {}
    unknown_filters = {}
    
    for field_name, feild_value in filters.etems():
        location = field_locations.get(field_name, "Unknown")
        
        match location:
            case "SQL":
                sql_filters[field_name] = feild_value
            case "MongoDB":
                mongodb_filters[field_name] = feild_value
            case "Unknown":
                unknown_filters[field_name] = feild_value
                
                
    print(f"\nSQL Filters: {sql_filters}")
    print(f"\nMongoDB Filters: {mongodb_filters}")
    print(f"\nUnknown Filters: {unknown_filters}")
    
    total_deleted = 0
    deletion_results = {}
    
    
    return
    