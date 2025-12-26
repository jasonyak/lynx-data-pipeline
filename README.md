# Lynx Data Pipeline for Lynx App - Connecting parents to daycares

Lynx is codename for a new startup that will be a marketplace for daycares. Like Zillow but for daycares. We make it easy for parents to find the best daycare for their needs.


## What each jobs does

We have the follow jobs: state ingestions, website scraping enrichment, google places enrichment, google search enrichment, and final enrichment.

The state ingestions is just a job that pulls health care data from the state's website and puts it into sqlite. I think we want to do light filter where there must be a valid website URL provided.

The Website Scraping Enrichment Job reads from the ingestion table to get the URL and the does a deep scrape of the website to extract images, content, and pdfs. It also performs a dedup as some daycares just us the same website.It then calls Gemini to extract key information and to filter out non relevant images. The output of the LLM is put into sqlite. This output is:
* Summary of the website
* Price range estimate
* Amenities
* Programs
* Philosophy
* Schedule
* Meals
* Languages
* Security Features
* CTA_URL
* Social Links
* Relevant Images (To filter out blank, random icons, or other non-relevant images)


The Google Places Enrichment Job read from the ingestion table and calls Google Placs to get reviews, photos, and other information. The output is stored into sqlite.

The Google Search Enrichment Job read from the ingestion table and calls Google Search to get search summary, relevant links, and grounding chunks. The output is stored into sqlite.

The Final Enrichment Job read from ingestion table, website scraping enrinchment table, google places enrinchment table, and google search enrinchment table. It then calles Gemini to do:
1. Sends all the images to Gemini to do a relevance filter again and to choose the best one to represent the daycare as a thumbnail
2. We need to combine the orginal ingestion, website scraping enrichment, google places enrichment, and google search enrichment into a complete description of the daycare
3. Similarly we need to generate an safety description.

The supabase sink job will push the data to our active tables in supabase.

###Tables

daycares
* Everything in the current table but with new columns for description and safety_description.

websites
* This is renamed from the enrichment table. quality_score needs to be dropped.

google_places
* unchanges

reviews
* unchanged

images
* unchanges

google_search
* unchanges

