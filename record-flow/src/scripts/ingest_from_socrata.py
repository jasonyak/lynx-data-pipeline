"""
Direct ingestion of daycare data from Socrata APIs to Supabase.

This script fetches data from Texas and Washington state Socrata APIs,
applies standardization and filtering logic, and stores records directly
in the state_ingestions Supabase table.
"""

import os
import sys
import json
import argparse
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Set, Tuple
import requests
from dotenv import load_dotenv
from supabase import create_client, Client

# Import Standardizer from unify_data.py
from unify_data import Standardizer

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Constants
SOCRATA_ENDPOINTS = {
    "TX": "https://data.texas.gov/resource/bc5r-88dy.json",
    "WA": "https://data.wa.gov/resource/was8-3ni8.json"
}

BATCH_SIZE = 100000  # API fetch batch size
DB_BATCH_SIZE = 100  # Database upsert batch size
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0  # seconds
API_TIMEOUT = 30  # seconds


def setup_supabase() -> Client:
    """
    Initialize Supabase client with environment variables.

    Returns:
        Supabase client instance

    Raises:
        ValueError: If required environment variables are missing
    """
    url = os.environ.get("LYNX_SUPABASE_URL")
    key = os.environ.get("LYNX_SUPABASE_KEY")

    if not url or not key:
        raise ValueError(
            "LYNX_SUPABASE_URL and LYNX_SUPABASE_KEY environment variables must be set."
        )

    return create_client(url, key)


def fetch_from_socrata(
    state: str,
    limit: Optional[int] = None,
    offset: int = 0,
    timeout: int = API_TIMEOUT
) -> List[Dict[str, Any]]:
    """
    Fetch records from Socrata API with pagination and retry logic.

    Args:
        state: State code ('TX' or 'WA')
        limit: Maximum number of records to fetch (None for all)
        offset: Starting offset for pagination
        timeout: Request timeout in seconds

    Returns:
        List of raw records from Socrata API

    Raises:
        requests.exceptions.RequestException: If all retries fail
    """
    if state not in SOCRATA_ENDPOINTS:
        raise ValueError(f"Invalid state: {state}. Must be TX or WA")

    endpoint = SOCRATA_ENDPOINTS[state]
    all_records = []
    current_offset = offset

    logger.info(f"Fetching {state} records from Socrata API...")

    while True:
        # Determine batch size for this request
        if limit is not None:
            remaining = limit - len(all_records)
            if remaining <= 0:
                break
            batch_limit = min(BATCH_SIZE, remaining)
        else:
            batch_limit = BATCH_SIZE

        # Build request parameters
        params = {
            "$limit": batch_limit,
            "$offset": current_offset,
            "$order": ":id"  # Consistent ordering for pagination
        }

        # Retry logic with exponential backoff
        for attempt in range(MAX_RETRIES):
            try:
                logger.debug(
                    f"  Fetching {state} batch: offset={current_offset}, "
                    f"limit={batch_limit} (attempt {attempt + 1}/{MAX_RETRIES})"
                )

                response = requests.get(
                    endpoint,
                    params=params,
                    timeout=timeout
                )
                response.raise_for_status()

                batch = response.json()

                if not batch:
                    logger.info(f"  No more records to fetch for {state}")
                    return all_records

                all_records.extend(batch)
                current_offset += len(batch)

                logger.info(
                    f"  Fetched {len(batch)} records from {state} "
                    f"(total: {len(all_records)})"
                )

                # Break retry loop on success
                break

            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_BASE_DELAY * (2 ** attempt)
                    logger.warning(
                        f"  Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}. "
                        f"Retrying in {delay}s..."
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        f"  Failed to fetch {state} records after {MAX_RETRIES} attempts: {e}"
                    )
                    raise

        # Check if we got fewer records than requested (last page)
        if len(batch) < batch_limit:
            logger.info(f"  Reached end of {state} dataset")
            break

    logger.info(f"Completed fetching {len(all_records)} records from {state}")
    return all_records


def process_and_filter_record(
    record: Dict[str, Any],
    state: str,
    standardizer: Standardizer,
    seen_entries: Set[Tuple[str, str]],
    drop_counts: Dict[str, int],
    args: argparse.Namespace
) -> Optional[Dict[str, Any]]:
    """
    Standardize and filter a single record using logic from unify_data.py.

    Applies all filters from unify_data.py lines 285-366:
    - Invalid types (Agency, Residential)
    - Exclusion keywords in name
    - Zero capacity
    - Missing contact info
    - Missing name or address
    - Inactive status
    - Duplicate detection
    - Optional CLI filters (city, state, zip)

    Args:
        record: Raw record from Socrata API
        state: State code ('TX' or 'WA')
        standardizer: Standardizer instance
        seen_entries: Set of (name, address) tuples for duplicate detection
        drop_counts: Dictionary to track dropped record reasons
        args: CLI arguments with optional filters

    Returns:
        Standardized record dict or None if filtered out
    """
    try:
        # Standardize based on state
        if state == "TX":
            unified = standardizer.standardize_tx(record)
        elif state == "WA":
            unified = standardizer.standardize_wa(record)
        else:
            logger.warning(f"Unknown state: {state}")
            return None

        # Filter A: Invalid Types
        if unified.get("type") in ["Agency", "Residential"]:
            drop_counts["filtered_type"] += 1
            return None

        # Filter B: Exclusion Keywords (Name Only - safer than raw record)
        name = unified.get("name", "").lower()
        exclude_keywords = [
            "child placing", "residential treatment",
            "placement agency", "adoption", "foster care"
        ]
        if any(k in name for k in exclude_keywords):
            drop_counts["filtered_keyword"] += 1
            return None

        # Filter C: Capacity (must be > 0 if present)
        cap = unified.get("capacity")
        if cap is not None and cap == 0:
            drop_counts["filtered_capacity"] += 1
            return None

        # Filter D: Contact Info (Must have at least ONE contact method)
        contact = unified.get("contact", {})
        if not any([contact.get("phone"), contact.get("email"), contact.get("website")]):
            drop_counts["filtered_contact"] += 1
            return None

        # Check 1: Name
        if unified.get("name") == "Unknown" or not unified.get("name"):
            drop_counts["missing_name"] += 1
            return None

        # Check 2: Address
        address_obj = unified.get("address", {})
        full_address = address_obj.get("full", "")
        if not full_address:
            drop_counts["missing_address"] += 1
            return None

        # CLI Filter: City
        if args.city:
            record_city = address_obj.get("city", "").lower().strip()
            target_city = args.city.lower().strip()
            if record_city != target_city:
                drop_counts["filtered_city"] += 1
                return None

        # CLI Filter: State (double-check record integrity)
        # Only filter if a specific state is requested (not "ALL")
        if args.state and args.state != "ALL":
            record_state = address_obj.get("state", "").upper().strip()
            target_state = args.state.upper().strip()
            if record_state != target_state:
                drop_counts["filtered_state"] += 1
                return None

        # CLI Filter: Zip
        if args.zip:
            record_zip = address_obj.get("zip", "")
            if not record_zip:
                drop_counts["filtered_zip"] += 1
                return None

            target_zip = args.zip.strip()
            # Use startswith to allow "78750" to match "78750-1234"
            if not record_zip.startswith(target_zip):
                drop_counts["filtered_zip"] += 1
                return None

        # Check 3: Status (must be Active)
        if unified.get("status") != "Active":
            drop_counts["inactive"] += 1
            return None

        # Check 4: Duplicate Detection (content-based fingerprint)
        fingerprint = (unified.get("name"), full_address)
        if fingerprint in seen_entries:
            drop_counts["duplicate"] += 1
            return None
        else:
            seen_entries.add(fingerprint)

        return unified

    except Exception as e:
        logger.error(f"Error processing record: {e}")
        drop_counts["processing_error"] = drop_counts.get("processing_error", 0) + 1
        return None


def transform_for_db(
    unified: Dict[str, Any],
    batch_id: uuid.UUID,
    api_fetch_timestamp: datetime
) -> Dict[str, Any]:
    """
    Transform unified record to database schema format.

    Args:
        unified: Standardized record from Standardizer
        batch_id: UUID for this ingestion batch
        api_fetch_timestamp: Timestamp when data was fetched from API

    Returns:
        Dictionary matching state_ingestions table schema
    """
    return {
        "daycare_id": unified["id"],
        "source_state": unified["source_state"],
        "original_record": unified.get("original_record"),
        "name": unified["name"],
        "type": unified.get("type"),
        "status": unified.get("status"),
        "license_date": unified.get("license_date"),
        "address": json.dumps(unified.get("address", {})),
        "contact": json.dumps(unified.get("contact", {})),
        "capacity": unified.get("capacity"),
        "ages_served": unified.get("ages_served"),
        "schedule": json.dumps(unified.get("schedule", {})),
        "ingestion_batch_id": str(batch_id),
        "api_fetch_timestamp": api_fetch_timestamp.isoformat()
    }


def upsert_to_supabase(
    supabase: Client,
    records: List[Dict[str, Any]],
    batch_id: uuid.UUID,
    api_fetch_timestamp: datetime,
    batch_size: int = DB_BATCH_SIZE
) -> Tuple[int, int]:
    """
    Upsert records to Supabase in batches with error handling.

    Args:
        supabase: Supabase client instance
        records: List of unified records to upsert
        batch_id: UUID for this ingestion batch
        api_fetch_timestamp: Timestamp when data was fetched from API
        batch_size: Number of records per batch

    Returns:
        Tuple of (success_count, failure_count)
    """
    success_count = 0
    failure_count = 0

    # Transform all records to DB format
    db_records = [
        transform_for_db(r, batch_id, api_fetch_timestamp)
        for r in records
    ]

    # Process in batches
    for i in range(0, len(db_records), batch_size):
        batch = db_records[i:i + batch_size]

        try:
            # Attempt batch upsert
            result = supabase.table("state_ingestions").upsert(
                batch,
                on_conflict="daycare_id"
            ).execute()

            success_count += len(batch)
            logger.info(f"  Upserted batch of {len(batch)} records")

        except Exception as e:
            logger.warning(f"  Batch upsert failed: {e}. Trying individual upserts...")

            # Fallback to individual upserts
            for record in batch:
                try:
                    supabase.table("state_ingestions").upsert(
                        record,
                        on_conflict="daycare_id"
                    ).execute()
                    success_count += 1

                except Exception as ind_error:
                    logger.error(
                        f"    Failed to upsert {record['daycare_id']}: {ind_error}"
                    )
                    failure_count += 1

    return success_count, failure_count


def main():
    """Main orchestration function with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="Ingest daycare data from Socrata APIs to Supabase",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run with small limit
  python src/scripts/ingest_from_socrata.py --dry-run --limit 10

  # Single state test
  python src/scripts/ingest_from_socrata.py --state TX --limit 100

  # Full ingestion
  python src/scripts/ingest_from_socrata.py

  # Filter by city
  python src/scripts/ingest_from_socrata.py --state TX --city Austin
        """
    )

    parser.add_argument(
        "--state",
        type=str,
        choices=["TX", "WA", "ALL"],
        default="ALL",
        help="Which state(s) to ingest (default: ALL)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of records per state"
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Starting offset for resuming interrupted runs"
    )
    parser.add_argument(
        "--city",
        type=str,
        default=None,
        help="Filter by city name (case-insensitive)"
    )
    parser.add_argument(
        "--zip",
        type=str,
        default=None,
        help="Filter by zip code"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Process data without writing to database"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DB_BATCH_SIZE,
        help=f"Records per database batch (default: {DB_BATCH_SIZE})"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Configure logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Initialize components
    logger.info("=" * 60)
    logger.info("Starting Socrata API Ingestion to Supabase")
    logger.info("=" * 60)

    # Generate batch ID and timestamp
    batch_id = uuid.uuid4()
    api_fetch_timestamp = datetime.now(timezone.utc)

    logger.info(f"Batch ID: {batch_id}")
    logger.info(f"Timestamp: {api_fetch_timestamp.isoformat()}")
    logger.info(f"Dry Run: {args.dry_run}")
    logger.info(f"State Filter: {args.state}")
    if args.city:
        logger.info(f"City Filter: {args.city}")
    if args.zip:
        logger.info(f"Zip Filter: {args.zip}")
    if args.limit:
        logger.info(f"Limit: {args.limit} records per state")
    logger.info("")

    # Initialize Supabase (skip if dry run)
    supabase = None
    if not args.dry_run:
        try:
            supabase = setup_supabase()
            logger.info("Connected to Supabase")
        except Exception as e:
            logger.error(f"Failed to connect to Supabase: {e}")
            return 1

    # Initialize Standardizer
    standardizer = Standardizer()

    # Determine which states to process
    states_to_process = ["TX", "WA"] if args.state == "ALL" else [args.state]

    # Track overall statistics
    total_fetched = 0
    total_processed = 0
    total_success = 0
    total_failure = 0

    overall_drop_counts = {
        "missing_name": 0,
        "missing_address": 0,
        "inactive": 0,
        "duplicate": 0,
        "filtered_type": 0,
        "filtered_keyword": 0,
        "filtered_capacity": 0,
        "filtered_contact": 0,
        "filtered_city": 0,
        "filtered_state": 0,
        "filtered_zip": 0,
        "processing_error": 0
    }

    # Global duplicate tracking across states
    seen_entries = set()

    # Process each state
    for state in states_to_process:
        logger.info("")
        logger.info("-" * 60)
        logger.info(f"Processing {state}")
        logger.info("-" * 60)

        try:
            # Fetch raw records from Socrata
            raw_records = fetch_from_socrata(
                state=state,
                limit=args.limit,
                offset=args.offset
            )
            total_fetched += len(raw_records)

            if not raw_records:
                logger.info(f"No records fetched for {state}")
                continue

            # Process and filter records
            logger.info(f"Processing and filtering {len(raw_records)} records...")

            processed_records = []
            for idx, record in enumerate(raw_records):
                if (idx + 1) % 100 == 0:
                    logger.info(f"  Processed {idx + 1}/{len(raw_records)}...")

                unified = process_and_filter_record(
                    record=record,
                    state=state,
                    standardizer=standardizer,
                    seen_entries=seen_entries,
                    drop_counts=overall_drop_counts,
                    args=args
                )

                if unified:
                    processed_records.append(unified)

            total_processed += len(processed_records)

            logger.info(f"Filtered to {len(processed_records)} valid records")

            # Upsert to database (skip if dry run)
            if not args.dry_run and processed_records:
                logger.info(f"Upserting {len(processed_records)} records to Supabase...")

                success, failure = upsert_to_supabase(
                    supabase=supabase,
                    records=processed_records,
                    batch_id=batch_id,
                    api_fetch_timestamp=api_fetch_timestamp,
                    batch_size=args.batch_size
                )

                total_success += success
                total_failure += failure

                logger.info(f"Upsert complete: {success} success, {failure} failures")

            elif args.dry_run:
                logger.info(f"[Dry Run] Would upsert {len(processed_records)} records")
                total_success += len(processed_records)

        except Exception as e:
            logger.error(f"Error processing {state}: {e}", exc_info=True)
            continue

    # Print final summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("Ingestion Complete")
    logger.info("=" * 60)
    logger.info(f"Total Records Fetched: {total_fetched}")
    logger.info(f"Total Records Processed: {total_processed}")
    logger.info(f"Total Records Upserted: {total_success}")
    if total_failure > 0:
        logger.info(f"Total Failures: {total_failure}")
    logger.info("")
    logger.info("Dropped Records Summary:")
    for reason, count in sorted(overall_drop_counts.items()):
        if count > 0:
            logger.info(f"  {reason}: {count}")

    logger.info("")
    logger.info("Done!")

    return 0 if total_failure == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
