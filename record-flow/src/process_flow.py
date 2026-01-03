import json
import logging
# Configure Logging immediately to avoid being overridden by imports
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)


import os
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Tuple
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

# Logging configured at top of file


# Suppress google_genai AFC info logs
logging.getLogger("google_genai.models").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# Pricing (USD per 1M tokens)
PRICING = {
    "gemini": {
        "input": 0.50,
        "output": 3.00
    }
}


# Thread-safe wrapper classes for parallel processing
class ThreadSafeCostTracker:
    """Thread-safe cost tracker replacing defaultdict."""
    def __init__(self):
        self._lock = threading.Lock()
        self._data = defaultdict(lambda: {"input": 0, "output": 0})

    def add(self, step: str, input_tokens: int, output_tokens: int):
        with self._lock:
            self._data[step]["input"] += input_tokens
            self._data[step]["output"] += output_tokens

    def get_snapshot(self) -> dict:
        with self._lock:
            return dict(self._data)


class ThreadSafeOutputWriter:
    """Thread-safe file writer for parallel record output."""
    def __init__(self, output_path: str):
        self._lock = threading.Lock()
        self._file = open(output_path, 'a')
        self._written_count = 0

    def write(self, record: dict):
        with self._lock:
            self._file.write(json.dumps(record) + "\n")
            self._file.flush()
            self._written_count += 1

    def get_written_count(self) -> int:
        with self._lock:
            return self._written_count

    def close(self):
        self._file.close()


class ProgressReporter:
    """Background thread that prints progress every 5 seconds."""
    def __init__(self, total: int, cost_tracker: ThreadSafeCostTracker):
        self._total = total
        self._cost_tracker = cost_tracker
        self._completed = 0
        self._lock = threading.Lock()
        self._start_time = time.time()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._report_loop, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        self._thread.join(timeout=1)

    def increment(self):
        with self._lock:
            self._completed += 1

    def _report_loop(self):
        while not self._stop_event.wait(timeout=5.0):
            self._print_progress()
        self._print_progress()  # Final report

    def _print_progress(self):
        with self._lock:
            completed = self._completed

        remaining = self._total - completed
        pct = (completed / self._total * 100) if self._total > 0 else 0

        elapsed = time.time() - self._start_time
        if completed > 0:
            rate = completed / elapsed
            eta_seconds = remaining / rate if rate > 0 else 0
            eta_str = f"{eta_seconds:.0f}s"
        else:
            eta_str = "calculating..."

        # Get current cost
        cost_snapshot = self._cost_tracker.get_snapshot()
        total_cost = self._calculate_cost(cost_snapshot)

        print(f"\n[Progress] {completed}/{self._total} ({pct:.1f}%) | "
              f"Remaining: {remaining} | ETA: {eta_str} | "
              f"Cost: ${total_cost:.4f}")

    def _calculate_cost(self, cost_snapshot: dict) -> float:
        total = 0.0
        for step, tokens in cost_snapshot.items():
            input_cost = (tokens["input"] / 1_000_000) * PRICING["gemini"]["input"]
            output_cost = (tokens["output"] / 1_000_000) * PRICING["gemini"]["output"]
            total += input_cost + output_cost
        return total


class ThreadSafeRefiner:
    """Thread-safe wrapper for LocalRefiner with lock-based access."""
    def __init__(self):
        self._lock = threading.Lock()
        self._refiner = LocalRefiner()

    def rank_images(self, *args, **kwargs):
        with self._lock:
            return self._refiner.rank_images(*args, **kwargs)

    def filter_pdfs(self, *args, **kwargs):
        with self._lock:
            return self._refiner.filter_pdfs(*args, **kwargs)

    def refine_text(self, *args, **kwargs):
        with self._lock:
            return self._refiner.refine_text(*args, **kwargs)


# Thread-local storage for per-thread scrapers
_thread_local = threading.local()

def get_thread_scraper() -> WebsiteScraper:
    """Get or create thread-local scraper instance."""
    if not hasattr(_thread_local, 'scraper'):
        _thread_local.scraper = WebsiteScraper()
    return _thread_local.scraper


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

def process_record(
    record: dict,
    cost_tracker: ThreadSafeCostTracker,
    refiner: ThreadSafeRefiner,
) -> Optional[dict]:
    """
    Process a single record through all enrichment stages.
    Uses thread-local scraper and shared thread-safe refiner.
    Returns None if the record should be dropped.
    """
    try:
        record_id = record.get('id', 'Unknown')
        logger.info(f"Processing started for record ID: {record_id} - {record.get('name', 'Unknown')}")

        # Enrich with Google Places
        record = find_and_enrich(record)

        # Check if Google Place was found
        google_data = record.get("google_data", {})
        if google_data.get("status") == "NOT_FOUND":
            logger.info(f"Skipping {record.get('id')}: Google Place not found.")
            return None

        # Status Filter
        business_status = google_data.get("business_status")
        if not business_status or business_status == "CLOSED_PERMANENTLY":
            logger.info(f"[{record_id}] Skipping {record.get('name')} - Invalid Status: {business_status}")
            return None

        # Check for website in strict mode: must have website in either source
        state_website = record.get("contact", {}).get("website")
        google_website = google_data.get("contact", {}).get("website")

        if not state_website and not google_website:
            logger.info(f"[{record_id}] Skipping {record.get('name')} - No website available.")
            return None

        # Enrich with Gemini (Research Phase)
        record, search_usage = enrich_with_gemini(record)
        if search_usage:
            cost_tracker.add(
                "gemini_search",
                search_usage.get("input_tokens", 0),
                search_usage.get("output_tokens", 0)
            )

        # Website Scraping (Deep) - use thread-local scraper
        scraper = get_thread_scraper()
        target_url = google_website or state_website
        if target_url:
            try:
                logger.debug(f"[{record_id}] Scraping website: {target_url} ...")
                raw_scraped_data = scraper.scrape(target_url, record_id=record_id)

                # Local Refinement using thread-safe refiner
                if raw_scraped_data and raw_scraped_data.get("assets_found", 0) > 0:
                    logger.debug(f"[{record_id}] Refining scraped data for {target_url}...")

                    # 1. Filter Images
                    all_images = [a['local_path'] for a in raw_scraped_data.get('assets', []) if a['type'] == 'image']
                    top_images = refiner.rank_images(all_images, top_n=10)

                    # 2. Filter PDFs
                    all_pdfs = [a['local_path'] for a in raw_scraped_data.get('assets', []) if a['type'] == 'pdf']
                    top_pdfs = refiner.filter_pdfs(all_pdfs, top_n=5)

                    # 3. Refine Text
                    all_text_files = [a['local_path'] for a in raw_scraped_data.get('assets', []) if a['type'] == 'text']
                    domain_dir = os.path.dirname(all_text_files[0]) if all_text_files else None
                    clean_text_path = None
                    if domain_dir:
                        clean_text_path = os.path.join(domain_dir, "cleaned_content.txt")
                        clean_text_path = refiner.refine_text(all_text_files, clean_text_path)

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
                    # Fallback to raw if no assets
                    record["scraped_data"] = raw_scraped_data

            except Exception as e:
                logger.debug(f"[{record_id}] Scraping failed for {target_url}: {e}. Dropping record.")
                return None

        # Final Synthesis (Gemini Finalizer)
        logger.info(f"[{record_id}] Finalizing record for {record.get('name')} with Gemini...")
        record, final_usage = enrich_with_gemini_finalizer(record)
        if final_usage:
            cost_tracker.add(
                "gemini_finalizer",
                final_usage.get("input_tokens", 0),
                final_usage.get("output_tokens", 0)
            )

    except Exception as e:
        logger.error(f"Enrichment failed for record {record.get('id', 'unknown')}: {e}")
        return None

    return record

def print_cost_summary(cost_snapshot: dict):
    """Print final cost summary from cost tracker snapshot."""
    print("\n=== Token Usage & Cost Estimate ===")
    total_cost = 0.0

    step_pricing_map = {
        "gemini_search": "gemini",
        "gemini_finalizer": "gemini"
    }

    for step, tokens in cost_snapshot.items():
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


def main():
    parser = argparse.ArgumentParser(description="Process daycare records.")
    parser.add_argument("--resume", action="store_true", help="Resume from the last processed index.")
    parser.add_argument("--limit", type=int, default=None, help="Stop after processing N records.")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel worker threads (default: 4)")
    args = parser.parse_args()

    # Ensure data directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    start_time = time.time()

    # Initialize thread-safe resources
    cost_tracker = ThreadSafeCostTracker()
    refiner = ThreadSafeRefiner()

    # Handle resume/fresh start
    if args.resume:
        start_index = load_state() + 1
        print(f"Resuming processing from index {start_index}...")
    else:
        start_index = 0
        print("Starting processing from the beginning...")
        # Truncate output file for fresh start
        with open(OUTPUT_FILE, 'w') as f:
            pass

    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file {INPUT_FILE} not found.")
        return

    # Initialize thread-safe output writer
    output_writer = ThreadSafeOutputWriter(OUTPUT_FILE)

    # Load records to process
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
                    logger.warning(f"Error decoding JSON at line {i}. Skipping.")

    total = len(records_to_process)
    if total == 0:
        print("No records to process.")
        return

    print(f"Loaded {total} records to process with {args.workers} workers...")

    # Track max index for checkpointing
    max_index_completed = start_index - 1
    index_lock = threading.Lock()

    # Initialize and start progress reporter (prints every 5s)
    progress = ProgressReporter(total, cost_tracker)
    progress.start()

    def process_and_write(index_record: Tuple[int, dict]):
        nonlocal max_index_completed
        index, record = index_record
        try:
            result = process_record(record, cost_tracker, refiner)
            if result:
                output_writer.write(result)

            with index_lock:
                max_index_completed = max(max_index_completed, index)

            # Update progress counter
            progress.increment()

        except Exception as e:
            logger.error(f"Record {index} failed: {e}")
            progress.increment()  # Still count as processed

    # Execute parallel processing
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        executor.map(process_and_write, records_to_process)

    # Stop progress reporter and do final cleanup
    progress.stop()
    save_state(max_index_completed)
    output_writer.close()

    elapsed_time = time.time() - start_time
    written_count = output_writer.get_written_count()
    print(f"\nProcessing complete. Wrote {written_count} records in {elapsed_time:.2f} seconds.")

    # Print final cost summary
    print_cost_summary(cost_tracker.get_snapshot())

if __name__ == "__main__":
    main()
