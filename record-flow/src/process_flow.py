import json
import os
import time
import argparse
from enrichment.google_places import find_and_enrich
from enrichment.gemini_search import enrich_with_gemini

from collections import defaultdict

# Configurations
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

def load_state():
    """But loads the last processed index from the state file."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
                return state.get("last_processed_index", -1)
        except json.JSONDecodeError:
            print(f"Warning: Corrupt state file {STATE_FILE}. Starting from scratch.")
            return -1
    return -1

def save_state(index):
    """Saves the current processed index to the state file."""
    with open(STATE_FILE, 'w') as f:
        json.dump({"last_processed_index": index}, f)

def process_record(record, cost_tracker):
    """
    Process a single record: Enrich with Google Places, then Gemini Search.
    Returns None if the record should be dropped.
    """
    try:
        # Enrich with Google Places
        record = find_and_enrich(record)
        
        # Check if Google Place was found
        google_data = record.get("google_data", {})
        if google_data.get("status") == "NOT_FOUND":
            print(f"Skipping {record.get('id')}: Google Place not found.")
            return None
            
        # Check for website in strict mode: must have website in either source
        state_website = record.get("contact", {}).get("website")
        google_website = google_data.get("contact", {}).get("website")
        
        if not state_website and not google_website:
            print(f"Skipping {record.get('id')}: No website available.")
            return None

        # Enrich with Gemini (Insider Profile)
        # Only run if we haven't dropped it
        record, usage_stats = enrich_with_gemini(record)

        # Track usage
        if usage_stats:
            cost_tracker["gemini_enrichment"]["input"] += usage_stats.get("input_tokens", 0)
            cost_tracker["gemini_enrichment"]["output"] += usage_stats.get("output_tokens", 0)
            
    except Exception as e:
        # Don't fail the whole pipeline if enrichment crashes, just log it (or print here)
        print(f"Enrichment failed for record {record.get('id', 'unknown')}: {e}")
        
    return record

def main():
    parser = argparse.ArgumentParser(description="Process daycare records.")
    parser.add_argument("--resume", action="store_true", help="Resume from the last processed index.")
    args = parser.parse_args()

    # Ensure data directory exists for state/output if input is elsewhere (though input is in data/)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    start_time = time.time()
    
    # Initialize Cost Tracker
    # Structure: step_name -> {'input': int, 'output': int}
    cost_tracker = defaultdict(lambda: {"input": 0, "output": 0})
    
    if args.resume:
        last_processed_index = load_state()
        print(f"Resuming processing from index {last_processed_index + 1}...")
    else:
        last_processed_index = -1
        print("Starting processing from the beginning...")
        # Optional: Clear output file to ensure a fresh start if not resuming?
        # User didn't explicitly ask to clear output, but "start from beginning" usually implies overwriting or a fresh run.
        # However, append mode is used below. If we don't clear, we'll append duplicates.
        # Let's truncate the file if we are starting fresh.
        with open(OUTPUT_FILE, 'w') as f:
            pass # Create/Truncate file


    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file {INPUT_FILE} not found.")
        return

    processed_count = 0
    
    with open(INPUT_FILE, 'r') as infile, open(OUTPUT_FILE, 'a') as outfile:
        for i, line in enumerate(infile):
            # Skip lines already processed
            if i <= last_processed_index:
                continue

            try:
                line = line.strip()
                if not line:
                    continue
                    
                record = json.loads(line)
                processed_result = process_record(record, cost_tracker)
                
                if processed_result:
                    outfile.write(json.dumps(processed_result) + "\n")
                    # Flush to ensure data is written before state update (crash consistency)
                    outfile.flush()
                
                # Checkpoint state immediately as requested
                save_state(i)
                processed_count += 1
                
                # feedback every 10 records
                if processed_count % 10 == 0:
                     print(f"Processed line {i}...", end='\r')

            except json.JSONDecodeError:
                print(f"Error decoding JSON at line {i}. Skipping.")
            except Exception as e:
                print(f"Error processing line {i}: {e}")
                # For now, we raise to stop on unknown errors, preserving the state at the last good one
                raise e

    elapsed_time = time.time() - start_time
    print(f"\nProcessing complete. Processed {processed_count} new records in {elapsed_time:.2f} seconds.")
    
    # Print Cost Summary
    print("\n=== Token Usage & Cost Estimate ===")
    total_cost = 0.0
    
    # Hardcoded mapping to pricing keys for now
    step_pricing_map = {
        "gemini_enrichment": "gemini"
    }
    
    for step, tokens in cost_tracker.items():
        input_tokens = tokens["input"]
        output_tokens = tokens["output"]
        
        pricing_key = step_pricing_map.get(step)
        if pricing_key and pricing_key in PRICING:
            rates = PRICING[pricing_key]
            input_cost = (input_tokens / 1_000_000) * rates["input"]
            output_cost = (output_tokens / 1_000_000) * rates["output"]
            step_cost = input_cost + output_cost
            total_cost += step_cost
            
            print(f"Step: {step}")
            print(f"  Input Tokens:  {input_tokens:,}")
            print(f"  Output Tokens: {output_tokens:,}")
            print(f"  Estimated Cost: ${step_cost:.4f}")
        else:
             print(f"Step: {step} (No pricing data)")
             print(f"  Input Tokens:  {input_tokens:,}")
             print(f"  Output Tokens: {output_tokens:,}")
             
    print(f"-----------------------------------")
    print(f"Total Estimated Cost: ${total_cost:.4f}")
    print(f"===================================")

if __name__ == "__main__":
    main()
