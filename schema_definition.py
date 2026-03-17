"""
- Takes in input from the user and creates a schema definition for the same in initial_schema.json
- The schema defination is in the form of a dictionary with the following keys
    - 

- Input: From the user
- Output: Initial schema definition in initial_schema.json
"""

import json

defined_field_names = [
    "string", "integer", "float", "boolean", "object",
    "array_string", "array_int", "array_float" 
]
defined_formats = [
    "email", "date-time", "date", "time", "uri", "uuid", "ipv4", "ipv6", "hostname"
]

def take_user_input():
    schema_properties = {}
    
    while True:
        more_to_add = input("Do you have any more fields to define? (Y/N): ")
        if (more_to_add == "Y"):
            field_name = input("Please enter the name of your field: ")
            if not field_name:
                print("Please enter a non empty field name, Try again:")
                continue
                
            
            while True:
                field_type = input("Please enter the datatype of this field: ")
                if (field_type.lower() not in defined_field_names):
                    print("Please enter a valid datatype from the following list:")
                    print(defined_field_names)
                else:
                    field_type = field_type.lower()
                    break;
            field_metadata = {"type": field_type}
            if (field_type == "string"):
                condition = input("Is this string a specific format? (email, date-time, uri, uud): (Y/N)")
                if (condition == "Y"):
                    while True:
                        format_type = input(f"Please enter a specific format: (press 'N' to exit the loop)")
                        if (format_type == "N"):
                            field_metadata["specific_format"] = "null"
                            break
                        if (format_type in defined_formats):
                            field_metadata["specific_format"] = format_type
                            break
                        else:
                            print("Please enter a valid format from the following:")
                            print(defined_formats)
                else:
                    field_metadata["specific_format"] = "null"
            else:
                field_metadata["specific_format"] = "null"
                    
            
            
            condition_unique = input(f"Is {field_name} unique? (Y/N):")
            if condition_unique == "Y":
                condition_unique = True
            else:
                condition_unique = False
            field_metadata["unique"] = (condition_unique)
                
            condition_not_null = input(f"Is {field_name} NOT NULL? (Y/N):")
            if condition_not_null == "Y":
                condition_not_null = True
            else:
                condition_not_null = False
            field_metadata["not_null"] = (condition_not_null)
            
            schema_properties[field_name] = field_metadata
            
        elif (more_to_add == "N"):
            print("Schema defination is done!!")
            break
            
        else:
            print("Please enter a valid input")   
                  
    final_json = json.dumps(schema_properties, indent=4)
    print(final_json)
    
    with open('initial_schema.json', 'w') as f:
        json.dump(schema_properties, f, indent=4)

def modify():
    with open('initial_schema.json', 'r') as f:
        data = json.load(f)
        print(data)
        
    while True:
        print("Write the function you need to implement:")
        print("To exit the loop, type: done")
        print("To delete a field, type: delete")
        print("To modify a field, type: change")
        command = input("Your command: ")
        
        if (command == "done"):
            print("Exiting the loop")
            break
        elif (command == "delete"):
            field_name = input("Enter the name of field to delete: ")
            if field_name in data:
                print("Current field:", field_name)
                del data[field_name]
                print(f"sucessfully deleted the field: {field_name}")
                with open('initial_schema.json', 'w') as f:
                    json.dump(data, f, indent=4)
                continue
            print(f"The field name {field_name} does not exist")
        elif command == "change":
            while True:
                field_name = input("Enter the name of field to modify: ")
                value = False
                if field_name in data:
                    print("Current field:", field_name)
                else:
                    print("No such field found, try again")
                    continue
                
                field_type = input("Please enter the datatype of this field: ")
                if (field_type.lower() not in defined_field_names):
                    print("Please enter a valid datatype from the following list:")
                    print(defined_field_names)
                else:
                    field_type = field_type.lower()
                    break;
            field_metadata = {"type": field_type}
            if (field_type == "string"):
                condition = input("Is this string a specific format? (email, date-time, uri, uud): (Y/N)")
                if (condition == "Y"):
                    while True:
                        format_type = input(f"Please enter a specific format: (press 'N' to exit the loop)")
                        if (format_type == "N"):
                            field_metadata["specific_format"] = "null"
                            break
                        if (format_type in defined_formats):
                            field_metadata["specific_format"] = format_type
                            break
                        else:
                            print("Please enter a valid format from the following:")
                            print(defined_formats)
                else:
                    field_metadata["specific_format"] = "null"
            else:
                field_metadata["specific_format"] = "null"
            
            
            condition_unique = input(f"Is {field_name} unique? (Y/N):")
            if condition_unique == "Y":
                condition_unique = True
            else:
                condition_unique = False
            field_metadata["unique"] = (condition_unique)
                
            condition_not_null = input(f"Is {field_name} NOT NULL? (Y/N):")
            if condition_not_null == "Y":
                condition_not_null = True
            else:
                condition_not_null = False
            field_metadata["not_null"] = (condition_not_null)
            
            data[field_name] = field_metadata
            
            with open('initial_schema.json', 'w') as f:
                json.dump(data, f, indent=4)
        else:
            print("Please enter a valid command")
            continue

def give_user_choice():
    while True:
        print("You have the following commands.")
        print("If you haven't defined your schema yet then type: create")
        print("If you want to again start defining then type: redefine")
        print("If you are done with defining then type: done")
        print("If you want to modify the schema then type: modify")
        command = input("Enter your command: ")
        command = command.lower()
        if command == "done":
            break
        elif command == "create":
            take_user_input()
        elif command == "redefine":
            take_user_input()
        elif command == "modify":
            modify()
        else:
            print("Please type a valid input:")
            print("If you haven't defined your schema yet then type: create")
            print("If you want to again start defining then type: redefine")
            print("If you are done with defining then type: done")
            print("If you want to modify the schema then type: modify")
            
        
if __name__ == "__main__":
    give_user_choice()