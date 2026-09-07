# Book Scraper (ETL, web-scraping source)

A separate project from `exam-analytics-pipeline/` — this one's about
learning data *acquisition* (scraping) rather than orchestration/streaming.

Target: `books.toscrape.com`, a site built specifically for scraping
practice — real HTML, real pagination, no ToS/legal concerns while
learning the mechanics.

## Status

- **Verified**: `scripts/scrape_books.py` correctly parses title, price,
  availability, and star rating — tested against `scripts/sample_page.html`
  (a saved sample built from real data pulled from the live site).
- **Not yet done**: fetching pages live (blocked in the chat sandbox that
  built this, should work fine from a normal machine), pagination across
  all ~50 pages, transform (clean price strings, standardize categories),
  and load (into Postgres).

## Run the verified part

```bash
cd scripts
python3 scrape_books.py
```

## Next steps

1. Add a real `fetch_page(url)` function using `requests` (with a
   `User-Agent` header and a short delay between requests) and confirm it
   works against the live site.
2. Loop across all pages (`https://books.toscrape.com/catalogue/page-N.html`)
   instead of the one saved sample.
3. Add transform (price string → float, category from the book's detail
   page) and load (into Postgres) to complete the ETL pattern — same shape
   as `exam-analytics-pipeline/scripts/pipeline_functions.py`, different
   data source.
