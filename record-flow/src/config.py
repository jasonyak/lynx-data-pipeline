# Configuration constants for the processing pipeline

# DO NOT CHANGE WITHOUT PERMISSION
GEMINI_MODEL_ID = "gemini-3-flash-preview"

# File paths
INPUT_FILE = "data/unified_daycares.jsonl"
OUTPUT_FILE = "data/output.jsonl"
STATE_FILE = "data/processing_state.json"

# Pricing (USD per 1M tokens)
PRICING = {
    "gemini": {
        "input": 0.50,
        "output": 3.00
    }
}
