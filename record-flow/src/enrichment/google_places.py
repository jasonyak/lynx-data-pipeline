import os
import requests
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GOOGLE_PLACES_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY")

def find_and_enrich(record):
    """
    Orchestrates the enrichment of a daycare record with Google Places data.
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

    if not full_query:
        logger.debug(f"Insufficient data to search for record {record.get('id')}")
        return record

    try:
        place_id = _search_place(full_query)
        if place_id:
            logger.debug(f"Found Place ID {place_id} for {name}")
            details = _get_place_details(place_id)
            if details:
                record["google_data"] = details
        else:
            logger.debug(f"No Google Place found for {name}")
            # Optionally record that we tried and failed so we don't retry forever?
            record["google_data"] = {"status": "NOT_FOUND", "searched_query": full_query}
            
    except Exception as e:
        logger.error(f"Error enriching {name}: {e}")

    return record

def _search_place(query):
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
        "formatted_address",
        "formatted_phone_number",
        "website",
        "rating",
        "user_ratings_total",
        "geometry", # for lat/lng
        "opening_hours",
        "photo",
        "reviews"
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
        image_dir = os.path.join("data", "images", place_id)
        if os.path.exists(image_dir):
            shutil.rmtree(image_dir)
        os.makedirs(image_dir, exist_ok=True)
        
        # Structure the data
        structured_data = {
            "place_id": result.get("place_id"),
            "name": result.get("name"),
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
