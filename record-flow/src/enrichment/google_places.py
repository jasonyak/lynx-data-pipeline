import os
import requests
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GOOGLE_PLACES_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY")

import hashlib
import json

def _get_cache_path(query):
    """Generates a cache file path based on the MD5 hash of the query."""
    cache_dir = os.path.join("data", "cache", "google_places")
    os.makedirs(cache_dir, exist_ok=True)
    
    query_hash = hashlib.md5(query.encode("utf-8")).hexdigest()
    return os.path.join(cache_dir, f"{query_hash}.json")

def find_and_enrich(record):
    """
    Orchestrates the enrichment of a daycare record with Google Places data.
    Includes caching to prevent redundant API calls.
    """
    if not GOOGLE_PLACES_API_KEY:
        logger.warning("GOOGLE_PLACES_API_KEY not set. Skipping enrichment.")
        return record

    if "google_data" in record:
        # Already enriched?
        return record

    name = record.get("name")
    
    # Construct address from available fields
    address_parts = []
    if record.get("address"):
        addr = record["address"]
        # Handle if address is a dict or string (based on input data seen earlier)
        if isinstance(addr, dict):
            if addr.get("street"): address_parts.append(addr["street"])
            if addr.get("city"): address_parts.append(addr["city"])
            if addr.get("state"): address_parts.append(addr["state"])
            if addr.get("zip"): address_parts.append(str(addr["zip"]))
        else:
            address_parts.append(str(addr))
            
    address_query = ", ".join(address_parts)
    full_query = f"{name} {address_query}".strip()
    logger.info(f"[{record.get('id')}] Generated Query: '{full_query}' for record {name}")

    if not full_query:
        logger.debug(f"Insufficient data to search for record {record.get('id')}")
        return record

    # --- Caching Logic ---
    cache_path = _get_cache_path(full_query)
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r") as f:
                cached_data = json.load(f)
            logger.debug(f"[{record.get('id')}] Cache HIT for {name}")
            record["google_data"] = cached_data
            return record
        except Exception as e:
            logger.warning(f"Failed to read cache for {name}, re-fetching: {e}")

    try:
        place_id = _search_place(full_query)
        if place_id:
            logger.debug(f"[{record.get('id')}] Found Place ID {place_id} for {name}")
            details = _get_place_details(place_id)
            if details:
                record["google_data"] = details
                # Save to cache
                try:
                    with open(cache_path, "w") as f:
                        json.dump(details, f)
                except Exception as e:
                    logger.warning(f"Failed to write cache for {name}: {e}")
        else:
            logger.debug(f"[{record.get('id')}] No Google Place found for {name}")
            not_found_data = {"status": "NOT_FOUND", "searched_query": full_query}
            record["google_data"] = not_found_data
            # Cache NOT_FOUND result too, to avoid re-searching
            try:
                with open(cache_path, "w") as f:
                    json.dump(not_found_data, f)
            except Exception as e:
                logger.warning(f"Failed to write cache for {name}: {e}")
            
    except Exception as e:
        logger.error(f"[{record.get('id')}] Error enriching {name}: {e}")

    return record

def _search_place(query):
    # ... (unchanged) ...
    """
    Uses Text Search (New) or Find Place (Legacy) to get a Place ID.
    Using Text Search (New) is often more robust but costs more. 
    Let's use Find Place (Legacy) or Text Search (Legacy) for now as it's standard.
    actually, 'Find Place From Text' is cheapest if we just get ID.
    """
    url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    params = {
        "input": query,
        "inputtype": "textquery",
        "fields": "place_id",
        "key": GOOGLE_PLACES_API_KEY
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    if data.get("status") == "OK" and data.get("candidates"):
        return data["candidates"][0]["place_id"]
    return None

import shutil
from io import BytesIO
from PIL import Image

# ... (rest of imports)

def _download_and_process_image(url, output_path, max_width=1000):
    # ... (unchanged) ...
    """
    Downloads an image from a URL, resizes it if needed, and saves it.
    """
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        image = Image.open(BytesIO(response.content))
        
        # Convert to RGB if necessary (e.g. PNG with transparency)
        if image.mode in ("RGBA", "P"):
            image = image.convert("RGB")
            
        # Resize if too wide
        if image.width > max_width:
            ratio = max_width / image.width
            new_height = int(image.height * ratio)
            image = image.resize((max_width, new_height), Image.Resampling.LANCZOS)
            
        image.save(output_path, "JPEG", quality=85)
        return True
    except Exception as e:
        logger.error(f"Failed to download/process image to {output_path}: {e}")
        return False

def _get_place_details(place_id):
    """
    Fetches details for a specific Place ID and downloads images.
    """
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    # Fields to fetch
    fields = [
        "place_id",
        "name",
        "business_status", # Added field
        "formatted_address",
        "formatted_phone_number",
        "website",
        "rating",
        "user_ratings_total",
        "geometry", # for lat/lng
        "opening_hours",
        "photo",
        "reviews",
        "url"
    ]
    
    params = {
        "place_id": place_id,
        "fields": ",".join(fields),
        "key": GOOGLE_PLACES_API_KEY
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    if data.get("status") == "OK":
        result = data["result"]
        
        # Prepare Image Directory
        image_dir = os.path.join("data", "cache", "google_places", "images", place_id)
        if os.path.exists(image_dir):
            shutil.rmtree(image_dir)
        os.makedirs(image_dir, exist_ok=True)
        
        # Structure the data
        structured_data = {
            "place_id": result.get("place_id"),
            "name": result.get("name"),
            "business_status": result.get("business_status"), # Added field
            "google_maps_url": result.get("url"),
            "address": result.get("formatted_address"),
            "contact": {
                "phone": result.get("formatted_phone_number"),
                "website": result.get("website")
            },
            "rating": {
                "stars": result.get("rating"),
                "count": result.get("user_ratings_total")
            },
            "reviews": [{k: v for k, v in r.items() if k != "relative_time_description"} for r in result.get("reviews", [])],
            "photos": [],
            "operating_hours": result.get("opening_hours", {}),
            "street_view_metadata": {}
        }

        # 1. Download Street View
        if "geometry" in result and "location" in result["geometry"]:
            loc = result["geometry"]["location"]
            lat, lng = loc.get("lat"), loc.get("lng")
            structured_data["street_view_metadata"] = {"lat": lat, "lng": lng}
            
            street_view_url = f"https://maps.googleapis.com/maps/api/streetview?size=600x300&location={lat},{lng}&key={GOOGLE_PLACES_API_KEY}"
            street_view_path = os.path.join(image_dir, "street_view.jpg")
            
            if _download_and_process_image(street_view_url, street_view_path):
                structured_data["street_view_path"] = street_view_path

        # 2. Download Place Photos (Max 5)
        if "photos" in result:
            photos_to_download = result["photos"][:5]
            for i, p in enumerate(photos_to_download):
                ref = p.get("photo_reference")
                if not ref: continue
                
                photo_url = f"https://maps.googleapis.com/maps/api/place/photo?maxwidth=1000&photo_reference={ref}&key={GOOGLE_PLACES_API_KEY}"
                photo_path = os.path.join(image_dir, f"photo_{i}.jpg")
                
                if _download_and_process_image(photo_url, photo_path):
                     structured_data["photos"].append(photo_path)

        return structured_data
        
    return None

