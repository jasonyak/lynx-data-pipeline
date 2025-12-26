from dagster import asset
import json
import os

@asset
def save_json(filter_and_enrich):
    """Saves the processed data to a JSON file."""
    os.makedirs("data", exist_ok=True)
    output_path = "data/processed_data.json"
    
    with open(output_path, "w") as f:
        json.dump(filter_and_enrich, f, indent=2)
        
    return output_path
