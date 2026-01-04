import os
import json
import argparse
import mimetypes
from pathlib import Path
from supabase import create_client, Client
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Constants
BUCKET_NAME = "daycare-media"
INPUT_FILE = "data/output.jsonl"

def setup_supabase() -> Client:
    url = os.environ.get("LYNX_SUPABASE_URL")
    key = os.environ.get("LYNX_SUPABASE_KEY")
    
    if not url or not key:
        raise ValueError("LYNX_SUPABASE_URL and LYNX_SUPABASE_KEY environment variables must be set.")
    
    return create_client(url, key)

def ensure_bucket_exists(supabase: Client, bucket_name: str):
    """Ensures the storage bucket exists."""
    try:
        buckets = supabase.storage.list_buckets()
        existing_names = [b.name for b in buckets]
        if bucket_name not in existing_names:
            print(f"Creating bucket: {bucket_name}")
            supabase.storage.create_bucket(bucket_name, options={"public": True})
        else:
            print(f"Bucket {bucket_name} already exists.")
    except Exception as e:
        print(f"Error checking/creating bucket: {e}")
        # Continue and hopfully it works or specific upload fails

def upload_file(supabase: Client, local_path: str, destination_path: str, content_type: str = None) -> Optional[str]:
    """Uploads a file to Supabase Storage and returns the public URL."""
    path_obj = Path(local_path)
    if not path_obj.exists():
        print(f"Warning: File not found: {local_path}")
        return None

    if not content_type:
        content_type, _ = mimetypes.guess_type(local_path)
    
    try:
        with open(local_path, 'rb') as f:
            file_bytes = f.read()
        
        # Upsert file
        supabase.storage.from_(BUCKET_NAME).upload(
            path=destination_path,
            file=file_bytes,
            file_options={"content-type": content_type or "application/octet-stream", "upsert": "true"}
        )
        
        # Get public URL
        public_url = supabase.storage.from_(BUCKET_NAME).get_public_url(destination_path)
        return public_url
    except Exception as e:
        print(f"Error uploading {local_path}: {e}")
        return None

def process_record(supabase: Client, line: str, dry_run: bool = False):
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        print("Skipping invalid JSON line")
        return

    # Extract key objects
    finalized = data.get("finalized_record")
    if not finalized:
        print("Skipping record without finalized_record")
        return

    # Check for crash error
    if finalized.get("error_crash"):
        print(f"Skipping crashed record: {finalized.get('name', 'Unknown')}")
        return
        
    daycare_id = finalized.get("daycare_id")
    if not daycare_id:
        print("Skipping record without daycare_id")
        return

    print(f"Processing daycares: {daycare_id} - {finalized.get('name')}")

    if dry_run:
        print("  [Dry Run] processing skipped.")
        return

    # 1. Handle Images (Thumbnail + Gallery)
    # Map local paths to uploaded URLs
    
    # Thumbnail
    thumbnail_url = finalized.get("thumbnail_url")
    if thumbnail_url and not thumbnail_url.startswith("http"):
        # Upload local
        fname = Path(thumbnail_url).name
        dest = f"{daycare_id}/params/{fname}" # Store params? Or just root/thumbnail?
        # Let's keep a structure: {daycare_id}/{filename}
        dest = f"{daycare_id}/{fname}"
        new_url = upload_file(supabase, thumbnail_url, dest)
        if new_url:
            finalized["thumbnail_url"] = new_url

    # Upsert Daycare
    # We use the keys from finalized_record which should match the table columns mostly.
    # We assume 'finalized_record' schema matches 'daycares' table columns.
    # Filter out keys that might not exist in table or complex objects if needed.
    
    upsert_data = {
        "daycare_id": daycare_id,
        "name": finalized.get("name"),
        "trust_score": finalized.get("trust_score"),
        "score_breakdown": finalized.get("score_breakdown"),
        "review_score": finalized.get("review_score"),
        "review_count": finalized.get("review_count"),
        "min_age_months": finalized.get("min_age_months"),
        "max_age_months": finalized.get("max_age_months"),
        "program_type": finalized.get("program_type"),
        "meals_provided": finalized.get("meals_provided"),
        "snacks_provided": finalized.get("snacks_provided"),
        "teacher_student_ratio": finalized.get("teacher_student_ratio"),
        "cameras": finalized.get("cameras"),
        "secure_entry": finalized.get("secure_entry"),
        "availability_status": finalized.get("availability_status"),
        "price_start": finalized.get("price_start"),
        "price_end": finalized.get("price_end"),
        "certifications": finalized.get("certifications"),
        "capacity": finalized.get("capacity"),
        "operating_hours": finalized.get("operating_hours"),
        "is_internal": finalized.get("is_internal", False),
        "thumbnail_url": finalized.get("thumbnail_url"),
        "headline": finalized.get("headline"),
        "sub_headline": finalized.get("sub_headline"),
        "description": finalized.get("description"),
        "search_tags": finalized.get("search_tags"),
        "insights": finalized.get("insights"),
        "google_maps_url": finalized.get("google_maps_url"),
        "google_place_id": finalized.get("google_place_id"),
        "website_url": finalized.get("website_url"),
        "email": finalized.get("email"),
        "director_name": finalized.get("director_name"),
        "phone": finalized.get("phone"),
        "address": finalized.get("address"),
        "city": finalized.get("city"),
        "state": finalized.get("state"),
        "zip": finalized.get("zip"),
        "country": finalized.get("country", "US"),
        "latitude": finalized.get("latitude"),
        "longitude": finalized.get("longitude"),
        # location is GEOGRAPHY, Supabase-py handles it if we pass a GeoJSON-like dict or WKT? 
        # Usually PostGIS via JSON needs specific format or a raw query. 
        # For simplicity, supabase-py upsert usually takes JSON. 
        # Let's try passing the standard PostGIS GeoJSON format if we can.
        # Or format: "SRID=4326;POINT(lon lat)" string.
    }
    
    lat = finalized.get("latitude")
    lng = finalized.get("longitude")
    if lat and lng and lat != 0 and lng != 0:
        upsert_data["location"] = f"SRID=4326;POINT({lng} {lat})"

    try:
        supabase.table("daycares").upsert(upsert_data).execute()
        print(f"  Upserted daycare {daycare_id}")
    except Exception as e:
        print(f"  Error upserting daycare {daycare_id}: {e}")
        # If daycare fails, related tables will fail due to FK.
        return

    # 2. Handle Reviews
    google_data = data.get("google_data", {})
    reviews = google_data.get("reviews", [])
    
    review_rows = []
    for r in reviews:
        # Check duplicate logic handled by UNIQUE constraints and ON CONFLICT DO NOTHING (or UPDATE)
        # We'll map fields
        row = {
            "daycare_id": daycare_id,
            "source": "google",
            "author_name": r.get("author_name"),
            "rating": r.get("rating"),
            "text": r.get("text"),
            "published_time": convert_timestamp(r.get("time")), # Needs conversion
        }
        review_rows.append(row)
        
    if review_rows:
        try:
            # Upsert reviews? Or Insert with ignore duplicates?
            # on_conflict="daycare_id,source,author_name,published_time"
            supabase.table("daycare_reviews").upsert(
                review_rows, 
                on_conflict="daycare_id,source,author_name,published_time"
            ).execute()
            print(f"  Upserted {len(review_rows)} reviews")
        except Exception as e:
            print(f"  Error inserting reviews: {e}")

    # 3. Handle Assets
    photos = google_data.get("photos", [])
    asset_rows = []
    
    for i, photo_path in enumerate(photos):
        fname = Path(photo_path).name
        dest = f"{daycare_id}/google_photos/{fname}"
        
        # Decide: Do we re-upload if it exists? 'upload_file' handles upsert.
        # Performance: Maybe skip if needed, but for now just upload.
        public_url = upload_file(supabase, photo_path, dest)
        
        if public_url:
            asset_rows.append({
                "daycare_id": daycare_id,
                "url": public_url,
                "type": "image",
                "source": "google_photo"
            })
    
    # Also handle PDF assets from scraped data if any (not in example JSON but nice to have)
    
    if asset_rows:
        try:
            # We don't have a good unique key for assets other than ID. verify duplicates?
            # For now, we always insert. This will duplicate assets on re-runs.
            # To avoid duplicates, we could Delete existing assets for this daycare and re-insert?
            # Or assume the URL is unique enough? 
            # Let's delete existing 'google_photo' assets for this daycare first to be clean.
            supabase.table("daycare_assets").delete().eq("daycare_id", daycare_id).eq("source", "google_photo").execute()
            
            supabase.table("daycare_assets").insert(asset_rows).execute()
            print(f"  Inserted {len(asset_rows)} assets")
        except Exception as e:
            print(f"  Error inserting assets: {e}")

    # 4. Handle Enrichments
    enrichment_rows = []
    gemini_data = data.get("gemini_search_data", {})
    
    # gemini_search_data has keys like 'safety', 'reputation' object
    for key, value in gemini_data.items():
        if key == "operational_info":
            continue
            
        if isinstance(value, dict):
            # summary, sources
            enrichment_rows.append({
                "daycare_id": daycare_id,
                "source": "gemini_search",
                "type": key,
                "summary": value.get("summary"),
                "sources": value.get("sources") # JSONB
            })
            
    if enrichment_rows:
        try:
             # Delete existing enrichments for clean slate on re-run
            supabase.table("daycare_enrichments").delete().eq("daycare_id", daycare_id).eq("source", "gemini_search").execute()
            
            supabase.table("daycare_enrichments").insert(enrichment_rows).execute()
            print(f"  Inserted {len(enrichment_rows)} enrichments")
        except Exception as e:
            print(f"  Error inserting enrichments: {e}")


def convert_timestamp(ts):
    from datetime import datetime
    if not ts: return None
    try:
        return datetime.fromtimestamp(int(ts)).isoformat()
    except:
        return None

def main():
    parser = argparse.ArgumentParser(description="Populate Supabase from output.jsonl")
    parser.add_argument("--input", default=INPUT_FILE, help="Input JSONL file")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of records")
    parser.add_argument("--dry-run", action="store_true", help="Do not upload/insert")
    args = parser.parse_args()

    supabase = setup_supabase()
    ensure_bucket_exists(supabase, BUCKET_NAME)

    count = 0
    with open(args.input, 'r') as f:
        for line in f:
            if not line.strip(): continue
            process_record(supabase, line, args.dry_run)
            count += 1
            if args.limit > 0 and count >= args.limit:
                break

if __name__ == "__main__":
    main()
