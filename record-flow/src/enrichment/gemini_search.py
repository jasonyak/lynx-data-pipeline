import os
import json
import logging
from google import genai
from google.genai import types
from config import GEMINI_MODEL_ID

logger = logging.getLogger(__name__)

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

# Define the "Insider Profile" Schema using strict JSON typing for the prompt
# Note: With new SDK, we might potentially use response_schema in future, 
# but for now text extraction with prompt instructions is robust enough for this use case.
SCHEMA_INSTRUCTION = """
Return a standard JSON object. The schema must strictly follow this structure:

{
  "educational_profile": {
    "philosophy": "string or null (e.g. Montessori, Reggio, Play-based)",
    "programs": {
      "languages": ["string"],
      "summer_camp": "boolean or null",
      "part_time": "boolean or null"
    }
  },
  "operational_reality": {
    "waitlist_status": "string or null (e.g. 'Long waitlist', 'Now enrolling')",
    "pricing": {
      "mention": "string or null (e.g. '$300/week')",
      "source": "string or null (e.g. 'Yelp review 2023')"
    },
    "schedule": "string or null (e.g. 'Follows ISD calendar')"
  },
  "amenities": {
    "meals": "string or null (e.g. 'Organic provided', 'Packed lunch required')",
    "security": {
        "cameras_streaming": "boolean or null",
        "secure_entry": "boolean or null"
    },
    "facilities": ["string (e.g. 'Splash pad', 'Indoor gym')"]
  },
  "community_intelligence": {
    "reputation": "string or null (e.g. 'Hidden gem', 'Avoid', 'New/Unknown')",
    "staff_sentiment": "string or null (e.g. 'High turnover mentioned in multiple reviews')",
    "red_flags": ["string (e.g. '2021 lawsuit regarding safety')"],
    "parent_gotchas": ["string (e.g. 'Strict late pickup fees', 'Parking nightmare')"]
  },
  "verification": {
      "source_urls": ["string (url)"],
      "key_quotes": [
          {
              "quote": "string (exact text from source)",
              "url": "string (source url)",
              "context": "string (what this quote supports, e.g. 'waitlist')"
          }
      ]
  }
}

Use null if information is not found. Do not approximate or guess.
"""

def enrich_with_gemini(record):
    """
    Uses Gemini with Google Search to find qualitative "Insider Profile" data.
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
        Research the daycare '{name}' located at '{address}'. 
        Search for official websites, parent forums (Reddit, local groups), employee reviews (Glassdoor, Indeed), and news articles.
        
        Your goal is to build an "Insider Profile" containing qualitative data that official records miss.
        Focus on:
        1. **Educational Philosophy**: Is it Montessori, Play-based, etc.?
        2. **Operational Reality**: Waitlists, Pricing, Schedule.
        3. **Amenities**: Cameras, Meals, Facilities.
        4. **Community Intelligence**: Unfiltered reputation, staff turnover issues, lawsuits/scandals ("Red Flags"), and logistical complaints ("Gotchas").
        5. **Verification**: Find exact quotes and source URLs to support your findings.

        {SCHEMA_INSTRUCTION}
        """

        # Generate content with Google Search Tool
        response = client.models.generate_content(
            model=GEMINI_MODEL_ID,
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[types.Tool(google_search=types.GoogleSearch())]
            )
        )
        
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
             
             record["gemini_search_data"] = gemini_data
             logger.debug(f"[{record.get('id')}] Gemini enriched: {name}")
             
        except Exception as e:
            logger.warning(f"[{record.get('id')}] Failed to parse Gemini JSON for {name}: {e}")
            # Safeguard against missing text
            raw = getattr(response, 'text', str(response))
            record["gemini_search_data"] = {"status": "FAILED_PARSE", "raw": raw[:200]}

    except Exception as e:
        logger.error(f"[{record.get('id')}] Gemini enrichment failed for {record.get('id')}: {e}")
        record["gemini_search_data"] = {"status": "ERROR", "error": str(e)}

    return record, usage_stats
