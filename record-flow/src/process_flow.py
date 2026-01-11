"""
Core processing pipeline for daycare record enrichment.

Pipeline Steps:
1. Google Places enrichment
2. Gemini research phase
3. Website scraping
4. Local refinement (CLIP image ranking)
5. Gemini final synthesis
"""
import json
import logging
import os
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Tuple

# Configure logging before other imports
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)
logging.getLogger("google_genai.models").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# Local imports
import copy
from config import INPUT_FILE, OUTPUT_FILE, RETRY_FILE
from utils import (
    ThreadSafeCostTracker,
    ThreadSafeOutputWriter,
    ThreadSafeRetryWriter,
    ProgressReporter,
    ThreadSafeRefiner,
    get_thread_scraper,
    load_state,
    save_state,
    print_cost_summary,
)
from enrichment.google_places import find_and_enrich
from enrichment.gemini_search import enrich_with_gemini
from enrichment.gemini_finalizer import enrich_with_gemini_finalizer


def process_record(
    record: dict,
    cost_tracker: ThreadSafeCostTracker,
    refiner: ThreadSafeRefiner,
    retry_writer: ThreadSafeRetryWriter,
) -> Optional[dict]:
    """
    Process a single record through all enrichment stages.
    Returns None if the record should be dropped.
    """
    record_id = record.get('id', 'Unknown')
    logger.info(f"Processing: {record_id} - {record.get('name', 'Unknown')}")

    # Save original record for retry file if needed
    original_record = copy.deepcopy(record)

    try:
        # Step 1: Google Places enrichment
        record = find_and_enrich(record)
        if record is None:
            # find_and_enrich now returns None if not found or mismatch
            # We skip logging here because find_and_enrich logs why it dropped it
            return None

        google_data = record.get("google_data", {})

        # Validation: Check if found
        if google_data.get("status") == "NOT_FOUND":
            logger.info(f"[{record_id}] Skipping - Google Place not found")
            return None

        # Validation: Check business status
        business_status = google_data.get("business_status")
        if not business_status or business_status == "CLOSED_PERMANENTLY":
            logger.info(f"[{record_id}] Skipping - Invalid status: {business_status}")
            return None

        # Validation: relaxed to allow missing websites
        state_website = record.get("contact", {}).get("website")
        google_website = google_data.get("contact", {}).get("website")

        # Step 2: Gemini research phase
        record, search_usage = enrich_with_gemini(record)
        if search_usage:
            cost_tracker.add("gemini_search",
                search_usage.get("input_tokens", 0),
                search_usage.get("output_tokens", 0))

        # Check for gemini_search failure
        gemini_search_data = record.get("gemini_search_data", {})
        if gemini_search_data.get("status") == "ERROR":
            error_msg = gemini_search_data.get("error", "Unknown error")
            retry_writer.write(original_record, "gemini_search", error_msg)
            logger.warning(f"[{record_id}] Written to retry file (gemini_search failed)")

        # Step 3: Website scraping
        # Step 3: Website scraping and status check
        target_url = google_website or state_website
        website_active = False

        if target_url:
            scraper = get_thread_scraper()
            raw_scraped_data = scraper.scrape(target_url, record_id=record_id)
            
            # Trust the scraper's assessment (which is cached)
            website_active = raw_scraped_data.get("website_active", False)
                
            # Step 4: Local refinement (CLIP image ranking)
            if website_active and raw_scraped_data and raw_scraped_data.get("assets_found", 0) > 0:
                record["scraped_data"] = _refine_scraped_data(raw_scraped_data, refiner)
            else:
                record["scraped_data"] = raw_scraped_data
        else:
            record["scraped_data"] = None

        # Step 5: Gemini final synthesis
        logger.info(f"[{record_id}] Finalizing with Gemini...")
        record, final_usage = enrich_with_gemini_finalizer(record)
        if final_usage:
            cost_tracker.add("gemini_finalizer",
                final_usage.get("input_tokens", 0),
                final_usage.get("output_tokens", 0))

        # Check for finalizer failure
        finalized = record.get("finalized_record", {})
        if finalized.get("error") or finalized.get("error_crash"):
            error_msg = finalized.get("error") or finalized.get("error_crash", "Unknown error")
            retry_writer.write(original_record, "gemini_finalizer", error_msg)
            logger.warning(f"[{record_id}] Written to retry file (gemini_finalizer failed)")
            return None  # Don't output records with failed finalization

        return record

    except Exception as e:
        logger.error(f"[{record_id}] Processing failed: {e}")
        return None


def _refine_scraped_data(raw_data: dict, refiner: ThreadSafeRefiner) -> dict:
    """Refine scraped data using CLIP for images and keyword filtering for PDFs."""
    assets = raw_data.get('assets', [])

    # Filter images using CLIP
    all_images = [a['local_path'] for a in assets if a['type'] == 'image']
    top_images = refiner.rank_images(all_images, top_n=10)

    # Pass full asset objects to allow filtering by original_url
    all_pdf_assets = [a for a in assets if a['type'] == 'pdf']
    top_pdfs = refiner.filter_pdfs(all_pdf_assets, top_n=5)

    # Refine text content
    all_text_files = [a['local_path'] for a in assets if a['type'] == 'text']
    clean_text_path = None
    if all_text_files:
        domain_dir = os.path.dirname(all_text_files[0])
        clean_text_path = os.path.join(domain_dir, "cleaned_content.txt")
        clean_text_path = refiner.refine_text(all_text_files, clean_text_path, pdf_files=top_pdfs)

    return {
        "root_url": raw_data.get("root_url"),
        "timestamp": raw_data.get("timestamp"),
        "verified_images": top_images,
        "pdf_assets": top_pdfs,
        "derived_body_text_path": clean_text_path,
        "website_active": raw_data.get("website_active", False),
        "raw_stats": {
            "original_images": len(all_images),
            "original_text_files": len(all_text_files)
        }
    }


def main():
    parser = argparse.ArgumentParser(description="Process daycare records.")
    parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint")
    parser.add_argument("--limit", type=int, default=None, help="Stop after N records")
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers (default: 4)")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    start_time = time.time()

    # Initialize resources
    cost_tracker = ThreadSafeCostTracker()
    refiner = ThreadSafeRefiner()

    # Handle resume/fresh start
    if args.resume:
        start_index = load_state() + 1
        print(f"Resuming from index {start_index}...")
    else:
        start_index = 0
        print("Starting fresh...")
        open(OUTPUT_FILE, 'w').close()
        open(RETRY_FILE, 'w').close()  # Clear retry file on fresh start

    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file {INPUT_FILE} not found.")
        return

    output_writer = ThreadSafeOutputWriter(OUTPUT_FILE)
    retry_writer = ThreadSafeRetryWriter(RETRY_FILE)

    # Load records
    records_to_process = []
    with open(INPUT_FILE, 'r') as f:
        for i, line in enumerate(f):
            if i < start_index:
                continue
            if args.limit and len(records_to_process) >= args.limit:
                break
            line = line.strip()
            if line:
                try:
                    records_to_process.append((i, json.loads(line)))
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON at line {i}, skipping")

    total = len(records_to_process)
    if total == 0:
        print("No records to process.")
        return

    print(f"Processing {total} records with {args.workers} workers...")

    # Checkpointing
    max_index_completed = start_index - 1
    index_lock = threading.Lock()

    # Progress reporting
    progress = ProgressReporter(total, cost_tracker)
    progress.start()

    def process_and_write(index_record: Tuple[int, dict]):
        nonlocal max_index_completed
        index, record = index_record
        try:
            result = process_record(record, cost_tracker, refiner, retry_writer)
            if result:
                output_writer.write(result)
            with index_lock:
                max_index_completed = max(max_index_completed, index)
            progress.increment()
        except Exception as e:
            logger.error(f"Record {index} failed: {e}")
            progress.increment()

    # Execute parallel processing
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        executor.map(process_and_write, records_to_process)

    # Cleanup
    progress.stop()
    save_state(max_index_completed)
    output_writer.close()
    retry_writer.close()

    elapsed = time.time() - start_time
    retry_count = retry_writer.get_written_count()
    print(f"\nComplete. Wrote {output_writer.get_written_count()} records in {elapsed:.2f}s")
    if retry_count > 0:
        print(f"Failed records written to {RETRY_FILE}: {retry_count}")
    print_cost_summary(cost_tracker.get_snapshot())


if __name__ == "__main__":
    main()
