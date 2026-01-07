import os
import json
import logging
import time
from google import genai
from google.genai import types
from config import GEMINI_MODEL_ID

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0  # seconds

# Configure Gemini API
API_KEY = os.environ.get("GEMINI_API_KEY")
if API_KEY:
    try:
        client = genai.Client(api_key=API_KEY)
    except Exception as e:
        logger.error(f"Failed to initialize Gemini Client: {e}")
        client = None
else:
    logger.warning("GEMINI_API_KEY not found. Gemini enrichment will be skipped.")
    client = None

# Simplified Schema: Only summaries, no nested source lists
SCHEMA_INSTRUCTION = """
Return a valid JSON object. The schema must strictly follow this structure:

{
  "safety_summary": "string (2-4 sentences summarizing any safety concerns, violations, legal issues, or news. If nothing found, say 'No safety concerns found.')",
  "reputation_summary": "string (2-4 sentences summarizing overall reputation from reviews. Include sentiment and common themes.)",
  "staff_summary": "string or null (2-4 sentences about employee experiences, turnover, management. Null if no employee reviews found.)",
  "operational_info": {
    "years_in_operation": "string or null",
    "philosophy": "string or null (e.g. Montessori, Play-based, Reggio)",
    "languages": ["string"],
    "ages_served": "string or null",
    "pricing_mentions": "string or null",
    "waitlist_info": "string or null",
    "hours": "string or null",
    "meals": "string or null",
    "facilities": ["string"]
  }
}

CRITICAL RULES:
- Use null for unknown fields.
- Do NOT make inferences or assumptions - report only what is explicitly stated in sources.
- Summaries must be based on direct evidence from sources, not your interpretation.
"""

def enrich_with_gemini(record):
    """
    Uses Gemini with Google Search to conduct a background check on a daycare.
    Prioritizes safety signals, reputation, and staff insights.
    Uses Grounding Metadata for verified sources.
    """
    if not client:
        return record, {"input_tokens": 0, "output_tokens": 0}
        
    usage_stats = {"input_tokens": 0, "output_tokens": 0}

    try:
        # Construct search query
        name = record.get("name", "")
        google_data = record.get("google_data", {})
        address = google_data.get("address") 
        if not address:
             contact = record.get("address", {}) 
             if isinstance(contact, dict):
                 address = f"{contact.get('street', '')}, {contact.get('city', '')}, {contact.get('state', '')}"
             else:
                 address = str(contact)
        
        prompt = f"""
        You are conducting a background check on a daycare for parents researching childcare options.

        Research '{name}' located at '{address}'.

        Search for:
        - News articles (any incidents, closures, awards, or mentions)
        - State licensing/inspection records or violations
        - BBB complaints
        - Google, Yelp, and Facebook reviews
        - Reddit and parent forum discussions
        - Employee reviews from any source

        PRIORITIES (in order of importance):
        1. SAFETY: Any news about incidents, violations, complaints, or legal issues
        2. REPUTATION: What are parents and employees actually saying?
        3. OPERATIONAL: Basic info like pricing, hours, philosophy (only if found)

        IMPORTANT - Only report verifiable facts:
        - Do NOT infer, speculate, or generalize
        - Do NOT make assumptions about things not explicitly stated
        - If information is unclear or ambiguous, omit it
        
        VERIFICATION REQUIREMENT:
        - You MUST verify every specific claim (dates, numbers, scores, licenses).
        - If you cannot find a source for a specific detail, do NOT include it.
        - The goal is 100% accuracy with verifiable sources.

        {SCHEMA_INSTRUCTION}
        """

        # Generate content with Google Search Tool (with retry)
        response = None
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                response = client.models.generate_content(
                    model=GEMINI_MODEL_ID,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        tools=[types.Tool(google_search=types.GoogleSearch())],
                        response_mime_type="text/plain"
                    )
                )
                break  # Success, exit retry loop
            except Exception as e:
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_BASE_DELAY * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"[{record.get('id')}] Gemini search attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    raise last_error  # Re-raise on final attempt
        
        # Extract Token Usage
        if response.usage_metadata:
            usage_stats["input_tokens"] = response.usage_metadata.prompt_token_count or 0
            usage_stats["output_tokens"] = response.usage_metadata.candidates_token_count or 0

        # Extract JSON
        try:
             text = response.text.strip()
             if text.startswith("```json"):
                 text = text[7:-3]
             elif text.startswith("```"):
                 text = text[3:-3]
             
             gemini_data = json.loads(text)
             
             # --- Grounding Metadata Extraction ---
             verified_sources = []
             if response.candidates:
                 candidate = response.candidates[0]
                 
                 gm = getattr(candidate, 'grounding_metadata', None)
                 
                 if gm and hasattr(gm, 'grounding_chunks') and gm.grounding_chunks:
                     processed_urls = set()
                     for chunk in gm.grounding_chunks:
                         if chunk.web and chunk.web.uri:
                             uri = chunk.web.uri
                             if uri in processed_urls: 
                                 continue # Dedupe
                             
                             title = chunk.web.title or uri
                             verified_sources.append({
                                 "url": uri,
                                 "title": title,
                                 "source_name": title # Fallback/Duplicate for schema
                             })
                             processed_urls.add(uri)
                             
             gemini_data["verified_sources"] = verified_sources

             record["gemini_search_data"] = gemini_data
             logger.debug(f"[{record.get('id')}] Gemini enriched: {name}")
             
        except Exception as e:
            logger.warning(f"[{record.get('id')}] Failed to parse Gemini JSON or extract grounding for {name}: {e}")
            # Safeguard against missing text
            raw = getattr(response, 'text', str(response))
            record["gemini_search_data"] = {"status": "FAILED_PARSE", "raw": raw[:200]}

    except Exception as e:
        logger.error(f"[{record.get('id')}] Gemini enrichment failed for {record.get('id')}: {e}")
        record["gemini_search_data"] = {"status": "ERROR", "error": str(e)}

    return record, usage_stats
