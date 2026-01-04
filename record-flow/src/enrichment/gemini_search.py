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

# Define the "Background Check" Schema for parent-focused daycare research
SCHEMA_INSTRUCTION = """
Return a valid JSON object. The schema must strictly follow this structure:

{
  "safety": {
    "summary": "string (2-4 sentences summarizing any safety concerns, violations, legal issues, or news. If nothing found, say 'No safety concerns found.')",
    "sources": [
      {
        "text": "string (exact quote or key finding)",
        "url": "string",
        "source_name": "string (e.g. 'Austin American-Statesman', 'BBB', 'Texas HHS')"
      }
    ]
  },
  "reputation": {
    "summary": "string (2-4 sentences summarizing overall reputation from reviews. Include sentiment and common themes.)",
    "sources": [
      {
        "text": "string (exact quote from review)",
        "url": "string",
        "source_name": "string (e.g. 'Google Reviews', 'Yelp', 'Facebook', 'Reddit')"
      }
    ]
  },
  "staff_insights": {
    "summary": "string or null (2-4 sentences about employee experiences, turnover, management. Null if no employee reviews found.)",
    "sources": [
      {
        "text": "string (exact quote from employee review)",
        "url": "string",
        "source_name": "string (name of the website where review was found)"
      }
    ]
  },
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
- Use null for unknown fields. Use empty arrays [] if no sources found.
- Only include information you can verify with a source URL.
- Do NOT make inferences or assumptions - report only what is explicitly stated in sources.
- Summaries must be based on direct evidence from sources, not your interpretation.
"""

def enrich_with_gemini(record):
    """
    Uses Gemini with Google Search to conduct a background check on a daycare.
    Prioritizes safety signals, reputation, and staff insights over operational details.
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
        2. REPUTATION: What are parents and employees actually saying? Include exact quotes.
        3. OPERATIONAL: Basic info like pricing, hours, philosophy (only if found)

        IMPORTANT - Only report verifiable facts:
        - Use direct quotes wherever possible
        - Include the source URL for every claim
        - Do NOT infer, speculate, or generalize
        - Do NOT make assumptions about things not explicitly stated
        - If information is unclear or ambiguous, omit it
        - If you find nothing concerning, report empty arrays for those fields

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
