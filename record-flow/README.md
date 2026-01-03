### Commands

To run the unify data script:

```bash
python src/scripts/unify_data.py --limit 10
python src/scripts/unify_data.py --limit 5 --random
python src/scripts/unify_data.py --state TX --city Austin

```

To run the process flow script:

```bash
python src/process_flow.py --resume
python src/process_flow.py
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
