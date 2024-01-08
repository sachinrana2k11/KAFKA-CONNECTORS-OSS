import pandas as pd
import json
from pandasql import sqldf

# Example JSON object
json_data = {
    "people": [
        {"name": "John", "age": 30, "city": "New York"},
        {"name": "Alice", "age": 25, "city": "Los Angeles"},
        {"name": "Bob", "age": 35, "city": "Chicago"}
    ]
}


# Create a DataFrame from the JSON data
df = pd.json_normalize(json_data, 'people')


# SQL-like query using pandasql
query = "SELECT * FROM df WHERE name = 'John'"
query_result = sqldf(query).to_json(orient='records')

# Print the result
print(query_result)
