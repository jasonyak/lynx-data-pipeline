from dagster import asset
import pandas as pd
from datetime import datetime

@asset
def filter_and_enrich(generate_raw_data):
    """Filters duplicates and adds a timestamp."""
    df = pd.DataFrame(generate_raw_data)
    
    # Remove duplicates based on 'id'
    df = df.drop_duplicates(subset=["id"])
    
    # Enrich with timestamp
    df["processed_at"] = datetime.now().isoformat()
    
    return df.to_dict("records")
