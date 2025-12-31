import json
import logging
import os
import time
import argparse
from enrichment.google_places import find_and_enrich
from enrichment.gemini_search import enrich_with_gemini
from enrichment.gemini_finalizer import enrich_with_gemini_finalizer
from scraping.scraper import WebsiteScraper
from analysis.local_ai import LocalRefiner

from collections import defaultdict

# Configurations
INPUT_FILE = "data/unified_daycares.jsonl"
OUTPUT_FILE = "data/output.jsonl"
STATE_FILE = "data/processing_state.json"

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

def process_record(record, cost_tracker, scraper=None, local_refiner=None):
    """
    Process a single record:
    1. Enrich with Google Places (already done in batch, but check if needed)
    2. Deep Scraping (images/text)
    3. Local Refinement (CLIP/Dedupe) -> Reduces data for Gemini
    4. Enrich with Gemini (using refined data)
    """
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
            logger.debug(f"Skipping {record.get('id')}: No website available.")
            return None

        # Enrich with Gemini (Insider Profile - "Research Phase")
        # Only run if we haven't dropped it
        record, search_usage = enrich_with_gemini(record)
        if search_usage:
            cost_tracker["gemini_search"]["input"] += search_usage.get("input_tokens", 0)
            cost_tracker["gemini_search"]["output"] += search_usage.get("output_tokens", 0)

        # Website Scraping (Deep)
        if scraper:
             # Prefer website from Google, then state record
             target_url = google_website or state_website
             if target_url:
                 try:
                     logger.debug(f"Scraping website: {target_url} ...")
                     raw_scraped_data = scraper.scrape(target_url)
                     
                     # Local Refinement (Simulate 'intelligence')
                     if local_refiner and raw_scraped_data and raw_scraped_data.get("assets_found", 0) > 0:
                         logger.debug(f"Refining scraped data for {target_url}...")
                         
                         # 1. Filter Images
                         all_images = [a['local_path'] for a in raw_scraped_data.get('assets', []) if a['type'] == 'image']
                         top_images = local_refiner.rank_images(all_images, top_n=10)
                         
                         # 2. Filter PDFs
                         all_pdfs = [a['local_path'] for a in raw_scraped_data.get('assets', []) if a['type'] == 'pdf']
                         top_pdfs = local_refiner.filter_pdfs(all_pdfs, top_n=5)
                         
                         # 3. Refine Text
                         all_text_files = [a['local_path'] for a in raw_scraped_data.get('assets', []) if a['type'] == 'text']
                         # Text files are in base_dir, so dirname gets the base_dir
                         domain_dir = os.path.dirname(all_text_files[0]) if all_text_files else None
                         clean_text_path = None
                         if domain_dir:
                             clean_text_path = os.path.join(domain_dir, "cleaned_content.txt")
                             clean_text_path = local_refiner.refine_text(all_text_files, clean_text_path)
                         
                         # Construct final refined object
                         refined_data = {
                             "root_url": raw_scraped_data.get("root_url"),
                             "timestamp": raw_scraped_data.get("timestamp"),
                             "verified_images": top_images,
                             "pdf_assets": top_pdfs,
                             "derived_body_text_path": clean_text_path,
                             "raw_stats": {
                                 "original_images": len(all_images),
                                 "original_text_files": len(all_text_files)
                             }
                         }
                         record["scraped_data"] = refined_data
                     else:
                         # Fallback to raw if logic fails or no assets
                         record["scraped_data"] = raw_scraped_data

                 except Exception as e:
                    logger.debug(f"Scraping failed for {target_url}: {e}. Dropping record.")
                    return None
        
        # [NEW] Final Synthesis (Gemini Finalizer)
        # Takes all data (Google, Research, Scraped) and builds final record
        logger.info(f"Finalizing record for {record.get('name')} with Gemini...")
        record, final_usage = enrich_with_gemini_finalizer(record)
        if final_usage:
             cost_tracker["gemini_finalizer"]["input"] += final_usage.get("input_tokens", 0)
             cost_tracker["gemini_finalizer"]["output"] += final_usage.get("output_tokens", 0)
            
    except Exception as e:
        # Don't fail the whole pipeline if enrichment crashes, just log it (or print here)
        logger.error(f"Enrichment failed for record {record.get('id', 'unknown')}: {e}")
        
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
    
    # Initialize Scraper & Local AI
    scraper = WebsiteScraper()
    local_refiner = LocalRefiner()
    
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
                processed_result = process_record(record, cost_tracker, scraper=scraper, local_refiner=local_refiner)
                
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
        "gemini_search": "gemini",
        "gemini_finalizer": "gemini"
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
