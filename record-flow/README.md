## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Commands

### Data Pipeline

#### 1. Fetch Raw Data
```bash
curl -G "https://data.wa.gov/resource/was8-3ni8.json" --data-urlencode '$limit=50000'
curl -G "https://data.texas.gov/resource/bc5r-88dy.json" --data-urlencode '$limit=50000'
```

#### 2. Unify Data
```bash
# Basic run
python src/scripts/unify_data.py

# Limit results (useful for testing)
python src/scripts/unify_data.py --limit 10

# Limit + Random sample
python src/scripts/unify_data.py --limit 10 --random

# Filter by location (State, City, Zip)
python src/scripts/unify_data.py --state TX --city Austin --zip 78758
```

#### 3. Process Flow (Enrichment)
```bash
# Run all steps
python src/process_flow.py

# Resume from last state (if crashed/stopped)
python src/process_flow.py --resume

# Run with specific worker count (default: 4)
python src/process_flow.py --workers 8

# Resume + Workers
python src/process_flow.py --resume --workers 10
```

#### 4. Populate Supabase
```bash
# Run all
python src/scripts/populate_supabase.py

# Test run (limit records)
python src/scripts/populate_supabase.py --limit 1

# Dry run (no DB changes)
python src/scripts/populate_supabase.py --dry-run
```

### Utilities
```bash
# Scraper Test
python src/scraping/scraper.py --url https://example.com
```


## V0 Supabase Tables

### 1. daycares
Main daycare records with all enriched data ready for database insert

**Identity & Scoring**

| Field | Type | Description |
|-------|------|-------------|
| daycare_id | TEXT PRIMARY KEY | Unique identifier from source state data |
| name | TEXT NOT NULL | Daycare name |
| trust_score | INTEGER | Overall trust score (0-100) from Gemini |
| score_breakdown | JSONB | Score components: {safety_and_ratio, teacher_quality, learning_and_growth, cleanliness_facilities} |
| review_score | NUMERIC(2,1) | Google Places rating (1-5 scale) |
| review_count | INTEGER | Number of Google reviews |

**Program Details**

| Field | Type | Description |
|-------|------|-------------|
| min_age_months | INTEGER | Minimum age in months |
| max_age_months | INTEGER | Maximum age in months |
| program_type | TEXT | Program type: 'Montessori', 'Reggio', 'Waldorf', 'Other' |
| meals_provided | BOOLEAN | Whether meals are provided |
| snacks_provided | BOOLEAN | Whether snacks are provided |
| teacher_student_ratio | TEXT | Teacher to student ratio (e.g., '1:4') |
| cameras | BOOLEAN | Whether cameras are available for parents |
| secure_entry | BOOLEAN | Whether secure entry/access control exists |
| availability_status | TEXT | Status: 'Waitlist', 'Open Enrollment', 'Call to Confirm' |
| price_start | INTEGER | Estimated monthly tuition start price (USD) |
| price_end | INTEGER | Estimated monthly tuition end price (USD) |
| certifications | TEXT[] | Array of certifications (e.g., 'NAEYC', 'Texas Rising Star') |
| capacity | INTEGER | Maximum number of children |
| operating_hours | JSONB | Google Places hours structure with weekday_text array |
| is_internal | BOOLEAN DEFAULT FALSE | Whether this is an internal/managed daycare |
| is_claimed | BOOLEAN DEFAULT FALSE | Whether the daycare has been claimed by an owner |

**Marketing Content**

| Field | Type | Description |
|-------|------|-------------|
| thumbnail_url | TEXT | Primary image URL/path |
| headline | TEXT NOT NULL | Marketing headline (4-7 words) |
| sub_headline | TEXT NOT NULL | Marketing subheadline (1 sentence) |
| description | TEXT NOT NULL | Full description (max 600 chars, 2 paragraphs) |
| search_tags | JSONB | Array of standardized search tags (0-5 tags), GIN indexed |

**Insights**

| Field | Type | Description |
|-------|------|-------------|
| insights | JSONB | Object with: sentiment_summary, atmosphere, red_flags[], parent_tips[] |

**Links & Contact**

| Field | Type | Description |
|-------|------|-------------|
| google_maps_url | TEXT | Google Maps URL |
| google_place_id | TEXT | Google Places API ID for lookups |
| website_url | TEXT | Daycare website URL |
| email | TEXT | Contact email |
| director_name | TEXT | Director/owner name |
| phone | TEXT | Contact phone number |

**Location**

| Field | Type | Description |
|-------|------|-------------|
| address | TEXT | Full street address |
| city | TEXT | City name |
| state | TEXT | 2-letter state code |
| zip | TEXT | ZIP/postal code |
| country | TEXT DEFAULT 'US' | Country code |
| latitude | NUMERIC(10,7) | Latitude coordinate |
| longitude | NUMERIC(10,7) | Longitude coordinate |
| location | GEOGRAPHY(POINT, 4326) | PostGIS point (computed from lat/lng) |

**Note:** Most fields are nullable as they come from LLM generation and may not always be found in source data.

---

### 2. daycare_enrichments
Research insights from Gemini search phase

| Field | Type | Description |
|-------|------|-------------|
| id | UUID PRIMARY KEY DEFAULT gen_random_uuid() | Unique enrichment ID |
| daycare_id | TEXT NOT NULL REFERENCES daycares(daycare_id) | Foreign key to daycares |
| source | TEXT NOT NULL | Source of enrichment (e.g., 'gemini_search') |
| type | TEXT NOT NULL | Type: 'safety', 'reputation', 'staff_insights', 'operational_info' |
| summary | TEXT | Summary text of findings |
| sources | JSONB | Array of objects with {text, url, source_name} |
| created_at | TIMESTAMPTZ DEFAULT NOW() | When enrichment was created |

---

### 3. daycare_assets
Media files (images, PDFs, documents)

| Field | Type | Description |
|-------|------|-------------|
| id | UUID PRIMARY KEY DEFAULT gen_random_uuid() | Unique asset ID |
| daycare_id | TEXT NOT NULL REFERENCES daycares(daycare_id) | Foreign key to daycares |
| url | TEXT NOT NULL | File URL or path |
| type | TEXT NOT NULL | Asset type: 'image', 'pdf', 'text' |
| source | TEXT NOT NULL | Source: 'google_photo', 'google_street_view', 'website', 'daycare_uploaded' |
| created_at | TIMESTAMPTZ DEFAULT NOW() | When asset was added |

---

### 4. daycare_reviews
Parent reviews from Google and direct submissions

| Field | Type | Description |
|-------|------|-------------|
| id | UUID PRIMARY KEY DEFAULT gen_random_uuid() | Unique review ID |
| daycare_id | TEXT NOT NULL REFERENCES daycares(daycare_id) | Foreign key to daycares |
| source | TEXT NOT NULL | Source: 'google', 'direct' |
| author_name | TEXT | Reviewer name |
| rating | INTEGER CHECK (rating >= 1 AND rating <= 5) | Star rating 1-5 |
| text | TEXT | Review content |
| published_time | TIMESTAMPTZ | When review was published |
| created_at | TIMESTAMPTZ DEFAULT NOW() | When record was created |


