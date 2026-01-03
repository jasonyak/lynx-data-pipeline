"""State persistence for processing pipeline."""
import json
import os

from config import STATE_FILE


def load_state() -> int:
    """Load the last processed index from the state file."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
                return state.get("last_processed_index", -1)
        except json.JSONDecodeError:
            print(f"Warning: Corrupt state file {STATE_FILE}. Starting from scratch.")
            return -1
    return -1


def save_state(index: int):
    """Save the current processed index to the state file."""
    with open(STATE_FILE, 'w') as f:
        json.dump({"last_processed_index": index}, f)
