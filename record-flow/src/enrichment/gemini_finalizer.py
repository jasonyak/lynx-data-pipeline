import os
import io
import json
import logging
import time
from typing import List, Dict, Any, Tuple, Optional, Literal
from config import GEMINI_MODEL_ID
from google import genai
from google.genai import types
from pydantic import BaseModel, Field

# Retry configuration
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0  # seconds

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

# --- Pydantic Models for Structured Output ---

class MarketingContent(BaseModel):
    headline: str = Field(description="4-7 words. The 'Title'. Specific and weirdly clear. (e.g. 'Montessori Home with Large Yard' or 'Bright Horizons at The Domain').")
    sub_headline: str = Field(description="1 sentence. The 'Hook'. Key logistics + vibe. (e.g. 'Full-time care for infants to pre-k with a focus on outdoor play and organic meals.').")
    description: str = Field(max_length=600, description="STRICT limit 600 chars. 2 paragraphs max. The 'Details'. Informative, warm, and natural. Tells the story of the program, the director, and the space without sounding like a brochure.")

class StructuredData(BaseModel):
    program_type: Literal['Montessori', 'Reggio', 'Waldorf', 'Play-based', 'Academic', 'Religious', 'Nature-based', 'Language Immersion']
    availability_status: Literal['Waitlist', 'Open Enrollment', 'Call to Confirm']
    min_age_months: Optional[int]
    max_age_months: Optional[int]
    meals_provided: Optional[bool] = Field(default=None, description="Whether meals are provided")
    snacks_provided: Optional[bool] = Field(default=None, description="Whether snacks are provided")
    price_start: Optional[int] = Field(default=None, description="Estimated monthly tuition start price in dollars")
    price_end: Optional[int] = Field(default=None, description="Estimated monthly tuition end price in dollars")
    teacher_student_ratio: Optional[str] = Field(default=None, description="Teacher to student ratio (e.g., '1:4', '1:6')")
    cameras: Optional[bool] = Field(default=None, description="Whether cameras are available for parents")
    secure_entry: Optional[bool] = Field(default=None, description="Whether secure entry/access control exists")
    certifications: List[str] = Field(default_factory=list, description="List of certifications (e.g., 'NAEYC', 'Texas Rising Star')")

class InsiderInsight(BaseModel):
    sentiment_summary: str = Field(description="A 2-3 sentence summary of parent reputation.")
    atmosphere: str = Field(description="Single-word vibe check (e.g., 'Academic', 'Cozy', 'Chaotic', 'Strict').")
    red_flags: List[str] = Field(description="List of potential concerns")
    parent_tips: List[str] = Field(description="Helpful hints")

class MediaSelection(BaseModel):
    best_thumbnail_path: str = Field(description="Best representative image path from inputs, EXACT MATCH")
    selection_reason: str

class CategoryScore(BaseModel):
    score: int = Field(ge=0, le=30, description="Score for this category.")
    improvement_tip: str = Field(description="One sentence actionable tip for the DAYCARE OWNER to improve this score. (e.g. 'Add staff bios to your website', 'Upload bright indoor photos').")

class ScoreBreakdown(BaseModel):
    teacher_quality: CategoryScore = Field(description="Max 30 pts. Staff tenure, ratios, bios, specific reviews.")
    parent_reputation: CategoryScore = Field(description="Max 20 pts. Social proof, review volume, rating consistency.")
    safety_and_transparency: CategoryScore = Field(description="Max 25 pts. Licensing, cameras, policies, prices listed.")
    facility_environment: CategoryScore = Field(description="Max 25 pts. Cleanliness, natural light, outdoor space, equipment quality.")

class Ranking(BaseModel):
    trust_score: int = Field(ge=0, le=100)
    trust_score_explanation: str = Field(description="One sentence summary for a PARENT explaining the score. (e.g. 'Excellent facilities and staff, but lacks transparent pricing online.')")
    score_breakdown: ScoreBreakdown
    ranking_tier: Literal['Top Rated', 'Verified', 'Standard', 'Needs Review'] = Field(description="Top Rated (95+), Verified (80-94), Standard (50-79), Needs Review (<50).")

class DaycareRecord(BaseModel):
    marketing_content: MarketingContent
    structured_data: StructuredData
    search_tags: List[str] = Field(description="List of 0-5 standardized tags (e.g., 'bilingual', 'organic-food', 'outdoor-play')")
    insider_insight: InsiderInsight
    media_selection: MediaSelection
    ranking: Ranking

def _build_finalized_record(gemini_response: Dict[str, Any], record: Dict[str, Any], image_candidates: List[str]) -> Dict[str, Any]:
    """
    Flatten Gemini's nested response and merge with pipeline data
    to create a database-ready record.
    """
    google_data = record.get("google_data", {})
    contact = record.get("contact", {})
    address_data = record.get("address", {})

    return {
        # Identity
        "daycare_id": record.get("id"),
        "name": record.get("name"),

        # Scoring (from Gemini ranking)
        "trust_score": gemini_response.get("ranking", {}).get("trust_score"),
        "trust_score_explanation": gemini_response.get("ranking", {}).get("trust_score_explanation"),
        "score_breakdown": gemini_response.get("ranking", {}).get("score_breakdown"),
        "ranking_tier": gemini_response.get("ranking", {}).get("ranking_tier"),

        # Google reviews
        "review_score": google_data.get("rating", {}).get("stars"),
        "review_count": google_data.get("rating", {}).get("count"),

        # Program details (from Gemini structured_data)
        "min_age_months": gemini_response.get("structured_data", {}).get("min_age_months"),
        "max_age_months": gemini_response.get("structured_data", {}).get("max_age_months"),
        "program_type": gemini_response.get("structured_data", {}).get("program_type"),
        "meals_provided": gemini_response.get("structured_data", {}).get("meals_provided"),
        "snacks_provided": gemini_response.get("structured_data", {}).get("snacks_provided"),
        "teacher_student_ratio": gemini_response.get("structured_data", {}).get("teacher_student_ratio"),
        "cameras": gemini_response.get("structured_data", {}).get("cameras"),
        "secure_entry": gemini_response.get("structured_data", {}).get("secure_entry"),
        "availability_status": gemini_response.get("structured_data", {}).get("availability_status"),
        "price_start": gemini_response.get("structured_data", {}).get("price_start"),
        "price_end": gemini_response.get("structured_data", {}).get("price_end"),
        "certifications": gemini_response.get("structured_data", {}).get("certifications", []),

        # From pipeline
        "capacity": record.get("capacity"),
        "operating_hours": google_data.get("operating_hours"),
        "is_internal": False,

        # Marketing (from Gemini)
        "thumbnail_url": gemini_response.get("media_selection", {}).get("best_thumbnail_path"),
        "headline": gemini_response.get("marketing_content", {}).get("headline"),
        "sub_headline": gemini_response.get("marketing_content", {}).get("sub_headline"),
        "description": _enforce_length_limit(gemini_response.get("marketing_content", {}).get("description"), record.get("id"), 600),
        "search_tags": gemini_response.get("search_tags", []),

        # Insights (from Gemini, stored as JSONB)
        "insights": gemini_response.get("insider_insight"),

        # Links
        "google_maps_url": google_data.get("google_maps_url"),
        "google_place_id": google_data.get("place_id"),
        "website_url": google_data.get("contact", {}).get("website") or contact.get("website"),
        "website_active": record.get("scraped_data", {}).get("website_active", True), # Default true for fallback compatibility

        # Contact
        "email": contact.get("email"),
        "director_name": contact.get("director_name"),
        "phone": google_data.get("contact", {}).get("phone") or contact.get("phone"),

        # Location
        "address": google_data.get("address") or address_data.get("street"),
        "city": address_data.get("city"),
        "state": address_data.get("state"),
        "zip": address_data.get("zip"),
        "country": "US",
        "latitude": google_data.get("street_view_metadata", {}).get("lat"),
        "longitude": google_data.get("street_view_metadata", {}).get("lng"),

        # Assets
        "photos": image_candidates,
    }


def _enforce_length_limit(text: str, record_id: Any, max_length: int = 600) -> str:
    """
    Truncates text to max_length, attempting to cut at the last sentence end.
    """
    if not text:
        return ""
    
    if len(text) <= max_length:
        return text

    logger.info(f"[{record_id}] Description truncated from {len(text)} to {max_length} chars.")
        
    # Take a slice slightly longer than max_length to find the best cut point? 
    # Actually, we must cut strictly AT or BEFORE max_length.
    truncated = text[:max_length]
    
    # Check for sentence endings in the last 100 chars
    # We look for the last occurrence of '.', '!', or '?'
    last_period = truncated.rfind('.')
    last_exclaim = truncated.rfind('!')
    last_question = truncated.rfind('?')
    
    best_cut = max(last_period, last_exclaim, last_question)
    
    # If we found a sentence ending reasonably close to the end (within last 150 chars), cut there.
    # Otherwise, we might be cutting in the middle of a really long sentence or paragraph.
    if best_cut > max_length - 150:
        return truncated[:best_cut+1]
        
    # Fallback: Cut at the last space to avoid splitting a word
    last_space = truncated.rfind(' ')
    if last_space != -1:
        return truncated[:last_space] + "..."
        
    # Worst case: hard cut
    return truncated + "..."

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
        # Note: Schema is not included in the text prompt anymore, it's passed via config directly.
        prompt_text = f"""
You are a "Vigilant Parent" AI agent. You are skeptical, protective, and data-driven.
You have visited 20 schools and know exactly what to look for.
Analyze the provided data (Basic Record, Research, Website Content) and photos to grade this daycare.

Goal: Create the final, user-facing record for a premium daycare marketplace.

CRITICAL: NO HALLUCINATIONS
- Parents will read this. Wrong info damages trust and wastes their time.
- If data is NOT explicitly stated in the source material, use null.
- NEVER guess prices, ratios, certifications, or safety features.
- Only include search_tags for attributes clearly evidenced in the data.
- For marketing_content, only describe what you can verify from the sources.

STRUCTURED DATA RULES:
- program_type: Choose best fit. 'Play-based' is standard for most home daycares/centers without specific pedagogy. 'Academic' for curriculum-heavy. 'Religious' for faith-based. 'Nature-based' for forest schools. 'Language Immersion' for bilingual.
- price_start/price_end: null unless exact prices are stated
- teacher_student_ratio: null unless explicitly stated (e.g., "1:4 ratio")
- cameras/secure_entry: null unless explicitly mentioned
- certifications: Only include if named specifically (e.g., "NAEYC accredited", "Texas Rising Star")
- meals_provided/snacks_provided: null unless explicitly stated

MARKETING CONTENT:
- headline/sub_headline: Factual, not aspirational
- description: Only include details found in source data. No filler phrases.

INSIDER INSIGHT:
- sentiment_summary: Summarize actual reviews/testimonials. If none exist, say "No parent reviews available."
- red_flags: Only cite verifiable concerns (inspection reports, reviews)
- parent_tips: Only if genuinely useful info exists in the data

THUMBNAIL SELECTION:
The thumbnail must make a parent stop scrolling and click. In a list of 20 daycares, this image is your only chance to stand out.
Selection criteria:
1. SHOW THE DIFFERENTIATOR - Match the headline.
2. UNIQUE > GENERIC - A distinctive reading nook beats a standard classroom.
3. WARMTH & LIFE - Spaces that look lived-in and loved. Natural light, color, texture.
4. INSTANT RECOGNITION - Parent should immediately understand what kind of place this is.
Avoid: Generic, Exterior, Logos, Dark/Blurry.
Return the EXACT original path from the input image list.

SCORING PHILOSOPHY: "The Balanced Quarters"
- **Total Score = sum of 4 Categories (Max 25 pts each).**
- **Strict Evidence Gates**: You cannot score high without proof.
- **Start at 0** for every category. Adding points requires evidence.

SCORING BREAKDOWN (100 pts):

1. Safety & Transparency (Max 25 pts)
   - **Key Evidence**: License + Prices.
   - **25 pts**: "Open Book" (License Verified AND **Prices Listed**).
   - **15 pts**: "Standard" (Licensed, but "Call for details").
   - **0-10 pts**: "Opaque" (No license info found or very sparse).

2. Facility & Environment (Max 25 pts)
   - **Key Evidence**: Interior Photos.
   - **CRITICAL GATE**: **No Interior Photos = Max 5 pts.**
   - **25 pts**: "Premium Spaces" (Bright natural light, organized, happy vibes in photos).
   - **10-20 pts**: "Standard School" (Safe/clean but generic or fluorescent lighting).
   - **0-5 pts**: "Invisible/Exterior Only" (No photos or only outside).

3. Teacher & Staff (Max 25 pts)
   - **Key Evidence**: Staff Bios or Specific Reviews.
   - **CRITICAL GATE**: **No Bios/Names = Max 10 pts.**
   - **25 pts**: "Real Humans" (Staff Bios w/ photos AND/OR Specific praise in reviews).
   - **15-20 pts**: "Good Team" (Mentioned as nice, but no specific bios).
   - **0-10 pts**: "Unknown/Generic" (No names, no bios).

4. Parent Reputation (Max 25 pts)
   - **Key Evidence**: Reviews.
   - **CRITICAL GATE**: **0 Reviews = 0 pts.**
   - **25 pts**: "Community Favorite" (10+ reviews, 4.8+ stars).
   - **15-20 pts**: "Good Standing" (Positive rating, but fewer reviews).
   - **0 pts**: "Ghost" (0 reviews).



For each scoring category, provide a specific "improvement_tip" for the OWNER.
IMPORTANT: Wording must be platform-agnostic (e.g., "Upload verified photos to your profile", "Update your listing"). DO NOT reference "your website".
For the overall score, provide a "trust_score_explanation" for the PARENT.
"""
        
        # 4. Call Gemini
        # We need to explicitly wrap images as Parts
        final_contents = []
        final_contents.append(prompt_text)
        
        for part in context_parts:
            final_contents.append(part)
            
        # Add images as Parts
        for img_obj in inline_images:
            final_contents.append(types.Part.from_bytes(data=img_obj["data"], mime_type="image/jpeg"))

        # Call Gemini with retry logic
        response = None
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                response = client.models.generate_content(
                    model=GEMINI_MODEL_ID,
                    contents=final_contents,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=DaycareRecord,
                        safety_settings=[
                            types.SafetySetting(
                                category="HARM_CATEGORY_DANGEROUS_CONTENT",
                                threshold="BLOCK_NONE"
                            ),
                            types.SafetySetting(
                                category="HARM_CATEGORY_HATE_SPEECH",
                                threshold="BLOCK_NONE"
                            ),
                            types.SafetySetting(
                                category="HARM_CATEGORY_HARASSMENT",
                                threshold="BLOCK_NONE"
                            ),
                            types.SafetySetting(
                                category="HARM_CATEGORY_SEXUALLY_EXPLICIT",
                                threshold="BLOCK_NONE"
                            ),
                        ]
                    )
                )
                break  # Success, exit retry loop
            except Exception as e:
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_BASE_DELAY * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"[{record.get('id')}] Finalizer attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    raise last_error  # Re-raise on final attempt
        
        # 5. Parse Response
        if response.usage_metadata:
            usage_stats["input_tokens"] = response.usage_metadata.prompt_token_count or 0
            usage_stats["output_tokens"] = response.usage_metadata.candidates_token_count or 0
            
        try:
             # With Structured Outputs, the parsed object is often available directly,
             # but to be safe and consistent with standard text handling:
             text = response.text.strip()
             gemini_data = json.loads(text)

             # Flatten Gemini response and merge with pipeline data
             record["finalized_record"] = _build_finalized_record(gemini_data, record, image_candidates)
             logger.info(f"[{record.get('id')}] Finalized record for {record.get('name')}")
             
        except Exception as e:
            logger.error(f"Failed to parse final JSON (even with structured output): {e}")
            logger.debug(f"Raw Text: {response.text}")
            record["finalized_record"] = {"error": str(e), "raw": response.text[:1000]}

    except Exception as e:
        logger.error(f"Finalizer failed: {e}")
        record["finalized_record"] = {"error_crash": str(e)}

    return record, usage_stats
