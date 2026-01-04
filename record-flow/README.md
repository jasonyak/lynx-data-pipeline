## Commands

To run the unify data script:

```bash
python src/scripts/unify_data.py --limit 10
python src/scripts/unify_data.py --limit 5 --random
python src/scripts/unify_data.py --state TX --city Austin --zip 78758
```

To run the process flow script:

```bash
python src/process_flow.py --resume
python src/process_flow.py
python src/process_flow.py --workers 8
```

To run the scraper script:

```bash
python src/scraping/scraper.py --url https://example.com
```

To get raw data

```bash
curl -G "https://data.wa.gov/resource/was8-3ni8.json" \
  --data-urlencode '$limit=50000'


curl -G "https://data.texas.gov/resource/bc5r-88dy.json" \
  --data-urlencode '$limit=50000'
```

### Price Range

$: <$1000/mo
$$: $1000-$1800/mo
$$$: $1800-$2800/mo
$$$$: >$2800/mo

## V0 Supabase Tables

### daycares


* daycare_id
* name

* trust_score
* safety_and_ratio
* teacher_quality
* learning_and_growth
* cleanliness_facilities

* review_score

* min_age_months
* max_age_months
* program_type
* meals_provided
* snacks_provided
* teacher_student_ratio
* cameras
* secure_entry
* availability_status
* price_start
* price_end
* certifications 
* is_internal

* thumbnail_url
* headline
* sub_headline
* description
* search_tags JSONB (Array of strings with GIN Indexed array)

* google_maps_url
* website_url

* email
* director
* phone
* address
* city
* state
* zip
* country
* latitude
* longitude
* location

### daycare_enrichments

* id
* daycare_id
* source (gemini_search)
* type (safet, reputation, or staff_insights)
* summary
* sources JSONB (Array of objects with text, url, name)

### daycare_assets

* id
* daycare_id
* url
* type (image, pdf, text)
* source (google_photo, google_street_view, website, daycare_uploaded)
* width
* height


### daycare_reviews

* id
* daycare_id
* source (google, direct)
* author_name
* rating
* text
* published_time


