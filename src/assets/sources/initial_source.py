from dagster import asset

@asset
def generate_raw_data(context):
    """Generates a list of dummy data."""
    data = [
        {"id": 1, "name": "Alice", "value": 10},
        {"id": 2, "name": "Bob", "value": 20},
        {"id": 3, "name": "Charlie", "value": 30},
        {"id": 1, "name": "Alice (Duplicate)", "value": 10}, # Duplicate for testing
    ]
    
    # Add preview to UI
    context.add_output_metadata({
        "preview": data,
        "count": len(data)
    })
    
    return data
