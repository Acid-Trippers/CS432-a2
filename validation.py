"""
- Validates the data against the initial schema defined in initial_schema.json and creates a new schema definition in analyzed_schema.json
- Calculates the similarity between the initial schema and the analyzed schema and prints the similarity score
- The similarity score is calculated using Semnatic similarity
- If similarity coefficient is less than a threshold, then the data is considered to be not similar to the initial schema and the user is notified
- Final metadata is stored in final_metadata.json according to the similarity score and the analyzed schema

- Input: Data to be validated and initial schema definition in initial_schema.json
- Output: Analyzed schema definition in analyzed_schema.json, similarity score printed and final metadata in metadata.json
"""

