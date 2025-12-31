import os
import io
import json
import logging
import base64
from typing import List, Dict, Any, Tuple
from config import GEMINI_MODEL_ID
from google import genai
from google.genai import types

# Try importing PIL for image resizing
try:
    from PIL import Image
except ImportError:
    Image = None

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("GEMINI_API_KEY")
if API_KEY:
    try:
        client = genai.Client(api_key=API_KEY)
    except Exception as e:
        logger.error(f"Failed to initialize Gemini Client: {e}")
        client = None
else:
    logger.warning("GEMINI_API_KEY not found. Gemini Finalizer will be skipped.")
    client = None

FINAL_SCHEMA = """
{
  "marketing_content": {
    "headline": "4-7 words. The 'Title'. Specific and weirdly clear. (e.g. 'Montessori Home with Large Yard' or 'Bright Horizons at The Domain').",
    "sub_headline": "1 sentence. The 'Hook'. Key logistics + vibe. (e.g. 'Full-time care for infants to pre-k with a focus on outdoor play and organic meals.').",
    "description": "2 paragraphs. The 'Details'. Informative, warm, and natural. Tells the story of the program, the director, and the space without sounding like a brochure."
  },
  "structured_data": {
    "philosophy": "Enum: ['Montessori', 'Reggio', 'Play-Based', 'Academic', 'Faith-Based', 'Waldorf', 'General']",
    "schedule_type": "Enum: ['Full-Time', 'Part-Time', 'Both']",
    "price_range": "Enum: ['$', '$$', '$$$', '$$$$']",
    "availability_status": "Enum: ['Waitlist', 'Open Enrollment', 'Call to Confirm']",
    "min_age_months": "Integer or null",
    "max_age_months": "Integer or null",
    "meals_provided": "Boolean",
    "snacks_provided": "Boolean"
  },
  "parent_survival_guide": {
    "communication_method": "Enum: ['App (Photos/Updates)', 'Email/Text', 'Paper Daily Sheet', 'Verbal Only']",
    "potty_training_support": "Enum: ['Fully Supported', 'Assisted', 'Must Be Trained']",
    "screen_time_policy": "Enum: ['Zero Screen Time', 'Educational Only', 'TV/Movie Time Occasional']"
  },
  "search_tags": [
    "List of 5-10 standardized tags (e.g., 'cloth-diaper-friendly', 'camera-access', 'organic-food', 'security-guard')"
  ],
  "insider_insight": {
    "sentiment_summary": "A 2-3 sentence summary of parent reputation.",
    "atmosphere": "Single-word vibe check (e.g., 'Academic', 'Cozy', 'Chaotic', 'Strict').",
    "red_flags": ["List of potential concerns"],
    "parent_tips": ["Helpful hints"]
  },
  "media_selection": {
    "best_thumbnail_path": "String (best representative image path from inputs, EXACT MATCH)",
    "selection_reason": "String"
  },
  "ranking": {
    "trust_score": "Integer (0-100)",
    "score_breakdown": {
      "safety_and_ratio": "Integer (0-30)",
      "teacher_quality": "Integer (0-30)",
      "learning_and_growth": "Integer (0-25)",
      "cleanliness_facilities": "Integer (0-15)"
    },
    "ranking_tier": "Enum: ['Top Rated', 'Verified', 'Standard', 'Needs Review']"
  }
}
"""

def _resize_image_to_bytes(image_path: str, max_dim: int = 500) -> bytes:
    """
    Resizes image so max dimension is max_dim, converts to JPEG bytes.
    Returns bytes or None if failed.
    """
    if not Image:
        logger.warning("Pillow not installed, skipping image resize.")
        return None
        
    try:
        with Image.open(image_path) as img:
            # Convert to RGB if needed
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")
                
            width, height = img.size
            if width > max_dim or height > max_dim:
                ratio = min(max_dim / width, max_dim / height)
                new_size = (int(width * ratio), int(height * ratio))
                img = img.resize(new_size, Image.Resampling.LANCZOS)
                
            buffer = io.BytesIO()
            img.save(buffer, format="JPEG", quality=85)
            return buffer.getvalue()
    except Exception as e:
        logger.warning(f"Failed to process image {image_path}: {e}")
        return None

def enrich_with_gemini_finalizer(record: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, int]]:
    """
    Performs the final synthesis using Gemini.
    """
    usage_stats = {"input_tokens": 0, "output_tokens": 0}
    
    if not client:
        return record, usage_stats
        
    try:
        # 1. Gather Text Context
        context_parts = []
        context_parts.append(f"Daycare Basic Data: {json.dumps(record, default=str)}")
        
        # Scraped Text
        scraped_data = record.get("scraped_data", {})
        text_path = scraped_data.get("derived_body_text_path")
        if text_path and os.path.exists(text_path):
            try:
                with open(text_path, "r") as f:
                    content = f.read(20000) # Truncate massive files
                    context_parts.append(f"Website Content: {content}")
            except Exception as e:
                logger.warning(f"Failed to read text path {text_path}: {e}")
        
        # Gemini Research Data
        gemini_search = record.get("gemini_search_data", {})
        if gemini_search:
            context_parts.append(f"Insider Research Data: {json.dumps(gemini_search)}")

        # 2. Gather Images
        # Candidates: verified_images (Scraper) + photos (Google Places)
        image_candidates = []
        if scraped_data.get("verified_images"):
            image_candidates.extend(scraped_data["verified_images"])
        
        google_data = record.get("google_data", {})
        if google_data.get("photos"):
            image_candidates.extend(google_data["photos"])
            
        # Dedupe
        image_candidates = list(set(image_candidates))
        
        # Limit total images to reasonable number (e.g. 10) to save tokens
        image_candidates = image_candidates[:10]
        
        inline_images = []
        for path in image_candidates:
            img_bytes = _resize_image_to_bytes(path)
            if img_bytes:
                inline_images.append({
                    "mime_type": "image/jpeg",
                    "data": img_bytes # SDK handles bytes directly for inline_data
                })
        
        # 3. Construct Prompt
        prompt_text = f"""
        You are an expert childcare analyst.
        Analyze the provided data (Basic Record, Research, Website Content) and photos.
        
        Goal: Create the final, user-facing record for a premium daycare marketplace.
        
        CRITICAL: 
        1. Be factual but warm. No AI slop. 
        2. Parents care about Safety, Love, and Learning.
        3. For 'marketing_content', follow the "Essential Trio" format strictly.
        4. Choose the best thumbnail from the provided images and return its EXACT original path from the input list.
        
        Schema:
        {FINAL_SCHEMA}
        
        Return ONLY valid JSON.
        """
        
        # 4. Call Gemini
        contents = [prompt_text] + context_parts
        
        # Add images to contents. 
        # For google-genai SDK, usually mix text and types.Part
        # We need to construct types.Content or list of parts
        
        final_contents = []
        final_contents.append(prompt_text)
        for part in context_parts:
            final_contents.append(part)
            
        # Add images as Parts
        for img_obj in inline_images:
            final_contents.append(types.Part.from_bytes(data=img_obj["data"], mime_type="image/jpeg"))

        response = client.models.generate_content(
            model=GEMINI_MODEL_ID,
            contents=final_contents,
            config=types.GenerateContentConfig(
                response_mime_type="application/json"
            )
        )
        
        # 5. Parse Response
        if response.usage_metadata:
            usage_stats["input_tokens"] = response.usage_metadata.prompt_token_count or 0
            usage_stats["output_tokens"] = response.usage_metadata.candidates_token_count or 0
            
        try:
             text = response.text.strip()
             
             # Clean Markdown Code Blocks
             if text.startswith("```json"):
                 text = text[7:]
             elif text.startswith("```"):
                 text = text[3:]
             if text.endswith("```"):
                 text = text[:-3]
                 
             text = text.strip()
             
             final_data = json.loads(text)
             record["finalized_record"] = final_data
             logger.info(f"Finalized record for {record.get('name')}")
        except Exception as e:
            logger.error(f"Failed to parse final JSON: {e}")
            logger.debug(f"Raw Text: {response.text}") # Debug log
            record["finalized_record"] = {"error": str(e), "raw": response.text[:1000]}

    except Exception as e:
        logger.error(f"Finalizer failed: {e}")
        record["finalized_record"] = {"error_crash": str(e)}

    return record, usage_stats
