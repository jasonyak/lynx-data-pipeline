import os
import io
import json
import logging
from typing import List, Dict, Any, Tuple, Optional, Literal
from config import GEMINI_MODEL_ID
from google import genai
from google.genai import types
from pydantic import BaseModel, Field

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
    description: str = Field(max_length=600, description="Max 600 chars. 2 paragraphs. The 'Details'. Informative, warm, and natural. Tells the story of the program, the director, and the space without sounding like a brochure.")

class StructuredData(BaseModel):
    program_type: Literal['Montessori', 'Reggio', 'Waldorf', 'Other']
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

class ScoreBreakdown(BaseModel):
    safety_and_ratio: int = Field(ge=0, le=30)
    teacher_quality: int = Field(ge=0, le=30)
    learning_and_growth: int = Field(ge=0, le=25)
    cleanliness_facilities: int = Field(ge=0, le=15)

class Ranking(BaseModel):
    trust_score: int = Field(ge=0, le=100)
    score_breakdown: ScoreBreakdown
    ranking_tier: Literal['Top Rated', 'Verified', 'Standard', 'Needs Review'] = Field(description="Strict Tiers: Top Rated (95-100, Flawless), Verified (80-94, Great), Standard (50-79, Safe/Average), Needs Review (<50, Red Flags).")

class DaycareRecord(BaseModel):
    marketing_content: MarketingContent
    structured_data: StructuredData
    search_tags: List[str] = Field(description="List of 0-5 standardized tags (e.g., 'bilingual', 'organic-food', 'outdoor-play')")
    insider_insight: InsiderInsight
    media_selection: MediaSelection
    ranking: Ranking


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
You are a hyper-vigilant, data-driven parent who has visited 20 schools and is hard to impress.
Analyze the provided data (Basic Record, Research, Website Content) and photos to grade this daycare.

Goal: Create the final, user-facing record for a premium daycare marketplace.

CRITICAL: NO HALLUCINATIONS
- Parents will read this. Wrong info damages trust and wastes their time.
- If data is NOT explicitly stated in the source material, use null.
- NEVER guess prices, ratios, certifications, or safety features.
- Only include search_tags for attributes clearly evidenced in the data.
- For marketing_content, only describe what you can verify from the sources.

STRUCTURED DATA RULES:
- program_type: Use 'Other' unless Montessori/Reggio/Waldorf is explicitly mentioned
- price_start/price_end: null unless exact prices are stated
- teacher_student_ratio: null unless explicitly stated (e.g., "1:4 ratio")
- cameras/secure_entry: null unless explicitly mentioned
- certifications: Only include if named specifically (e.g., "NAEYC accredited", "Texas Rising Star")
- meals_provided/snacks_provided: null unless explicitly stated

SCORING RULES:
1. BASELINE IS 50 (AVERAGE): A score of 50 means "Licensed, Safe, Standard". It meets legal minimums.
2. EVIDENCE REQUIRED: No proof = No points. If 'cameras' or 'low ratios' aren't explicitly stated, assume they don't exist.
3. BE A SKEPTIC: Do not give benefit of doubt. Higher scores (60-80) require specific proof of quality.
4. TOP TIERS ARE RARE: 80+ is "Verified" (Great). 95+ is "Top Rated" (Unicorn/Flawless).

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
1. SHOW THE DIFFERENTIATOR - Match the headline. If it's "Montessori with Large Backyard," show the backyard or Montessori materials, not a generic classroom.
2. UNIQUE > GENERIC - A distinctive reading nook beats a standard classroom. A treehouse beats a plastic play structure.
3. WARMTH & LIFE - Spaces that look lived-in and loved. Natural light, color, texture.
4. INSTANT RECOGNITION - Parent should immediately understand what kind of place this is.

Avoid:
- Generic classroom that could be anywhere
- Exterior/building shots (looks like a business, not a home for kids)
- Logos or marketing graphics
- Dark, blurry, or cluttered images
- Empty sterile spaces

Return the EXACT original path from the input image list.
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
        
        # 5. Parse Response
        if response.usage_metadata:
            usage_stats["input_tokens"] = response.usage_metadata.prompt_token_count or 0
            usage_stats["output_tokens"] = response.usage_metadata.candidates_token_count or 0
            
        try:
             # With Structured Outputs, the parsed object is often available directly, 
             # but to be safe and consistent with standard text handling:
             text = response.text.strip()
             final_data = json.loads(text)
             
             record["finalized_record"] = final_data
             logger.info(f"[{record.get('id')}] Finalized record for {record.get('name')}")
             
        except Exception as e:
            logger.error(f"Failed to parse final JSON (even with structured output): {e}")
            logger.debug(f"Raw Text: {response.text}")
            record["finalized_record"] = {"error": str(e), "raw": response.text[:1000]}

    except Exception as e:
        logger.error(f"Finalizer failed: {e}")
        record["finalized_record"] = {"error_crash": str(e)}

    return record, usage_stats
