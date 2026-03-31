# CLAUDE.md — SEO Crawler Project

## What this is
FastAPI-based SEO audit tool. Crawls any website, runs full technical SEO analysis, persists results in SQLite, serves CSV/JSON/Markdown exports. Built for SMB audits by an agency.

## Module map (8 files — do NOT consolidate)
```
main.py        FastAPI routes + background job runner (DO NOT put logic here)
full_audit.py  FullTechnicalAudit class — state holder + method binder only
crawler.py     Async HTTP fetch + per-page parsing (httpx + BeautifulSoup)
analyzers.py   Post-crawl analysis, redirect checks, external link health
pagespeed.py   Google PageSpeed Insights API (CrUX + Lighthouse lab)
reporters.py   generate_summary(), generate_client_report(), generate_internal_report()
db.py          SQLite helpers: init_db, db_create_job, db_update_job, db_get_job, db_list_jobs, db_purge_old_jobs
config.py      Constants, _USER_AGENTS, _SITEMAP_PATHS, SUMMARY_KEYS, random_headers(), PAGESPEED_API_KEY (from .env)
js_worker.py   Playwright subprocess (separate process, not imported)
```

## Architecture pattern
All module functions take `self` (FullTechnicalAudit instance) as first arg. They are bound as class methods in full_audit.py:
```python
run_crawl = _crawler.run_crawl   # bound at class level, not instance
```
Never turn modules into classes. Never add logic to main.py. This pattern exists to keep files under token read limits.

## Critical technical decisions (do not revert)
| Decision | Reason |
|---|---|
| `http2=False` on all AsyncClient | SMB sites return 403 with HTTP/2 |
| `Accept-Encoding: gzip, deflate` (no `br`) | Brotli causes garbled HTML on some hosts |
| `threading.Lock()` not `asyncio.Lock()` | cross-event-loop safe; asyncio.Lock crashes in background tasks |
| `sys.executable` in subprocess | ensures js_worker.py uses same venv Python |
| `domcontentloaded + 15000ms` in Playwright | `networkidle` times out on heavy SPAs |
| 8 concurrent threads | was hardcoded to 3 — caused slow crawls |

## Key state on FullTechnicalAudit instance
```python
self.site_url       # original URL, stripped trailing /
self.origin         # scheme://netloc
self.domain         # netloc only
self.pages_data     # List[Dict] — one dict per crawled page
self.results        # Dict — audit output, populated by each audit_*() call
self._lock          # threading.Lock — used in crawler._crawl_page
self._url_depths    # Dict[url, int] — for orphan detection
self._link_targets  # set — all URLs that have been linked to
self._external_links_seen  # set — dedup external link checks
```

## config.py — SUMMARY_KEYS is the single source of truth
Any new crawl_summary metric MUST be added to SUMMARY_KEYS in config.py first. The empty fallback dict is built from this list. Current keys (35 total):
`total_pages, status_2xx, status_3xx, status_4xx, status_5xx, redirect_chains, missing_titles, long_titles, titles_too_short, no_meta_desc, meta_desc_too_long, meta_desc_too_short, no_h1, multi_h1, noindex_pages, images_missing_alt_total, pages_with_missing_alt, pages_without_og, pages_without_twitter, pages_with_mixed_content, pages_not_https, slow_server_response, avg_response_time_ms, avg_page_size_kb, pages_with_schema, thin_content_pages, orphan_pages_count, urls_with_uppercase, urls_too_long, urls_with_underscores, urls_with_special_chars, trailing_slash_issues_count, broken_external_links_count, pages_with_hreflang, canonical_issues_count`

## Per-page fields (pages_data entries)
`url, final_url, status_code, title, meta_description, h1, canonical, robots_meta, og_tags{}, twitter_tags{}, images_missing_alt, word_count, paragraph_count, response_time_ms, page_size_kb, internal_links[], external_links[], hreflang[], schema_types[], mixed_content, is_https, url_has_uppercase, url_too_long, url_has_underscores, url_has_special_chars, has_pagination, used_js_render`

## API endpoints summary
```
POST   /api/audit                    start background job → {job_id, status}
GET    /api/status/{job_id}          poll job
GET    /api/jobs?limit=50            list jobs
GET    /api/export/{job_id}/csv      streaming CSV download
GET    /api/export/{job_id}/json     full JSON download
GET    /api/report/{job_id}/client   client-facing Markdown
GET    /api/report/{job_id}/internal internal technical Markdown
DELETE /api/jobs/purge?days=30       delete jobs older than N days → {deleted, older_than_days}
GET    /test-simple?url=             sync quick audit (no DB, 50 pages)
GET    /summary?url=                 sync + plain text summary
GET    /debug-pages?url=             raw pages_data
```

## JS worker flow (subprocess, not import)
1. main.py sorts pages_data by word_count ascending
2. Counts pages with word_count < 200 → sets max_js = clamp(3, 8, count)
3. Sends JSON payload via stdin to `js_worker.py`
4. Worker renders top N pages with Playwright, returns enriched pages_data via stdout
5. main.py re-runs: audit_robots_txt → analyze_crawl → audit_redirect_setup → audit_broken_external_links → run_pagespeed_sample

## PageSpeed fields per URL
`performance_score (0-100 Lighthouse lab), lcp_ms, inp_ms, cls (CrUX, Lighthouse fallback), fcp_ms, tbt_ms, speed_index_ms, cwv_category`
Sampled: homepage + up to 2 more pages (3 total). Strategy: mobile only.

## DB schema (audits.db, SQLite)
`job_id (PK), status, url, lead_name, created_at, updated_at, result_json, summary, pages_data_json, js_enrichment_used, error`
Indexes: `idx_jobs_status` on status, `idx_jobs_created_at` on created_at.
Purge: `db_purge_old_jobs(days=30)` — call via `DELETE /api/jobs/purge?days=N`.

## What is NOT built yet (deliberate deferrals)
- API key auth on endpoints (user deferred)
- Rate limiting (user deferred)
- GSC OAuth integration (needs UI first)
- Crawl diffing (compare two job IDs)
- Internal PageRank scoring
- DataForSEO backlink integration

## Backlinks — permanent limitation
Cannot detect inbound backlinks without crawling the web. We only see outbound external links. DataForSEO is the recommended integration path.

## Environment setup
API key is loaded from `.env` (never committed). Copy `.env.example` → `.env` and fill in the key:
```
PAGESPEED_API_KEY=your_key_here
```
`python-dotenv` and `lxml` must be installed in the venv:
```bash
venv/Scripts/pip install python-dotenv lxml
```

## Running
```bash
uvicorn main:app --host 0.0.0.0 --port 8000
# or
docker build -t seo-crawler . && docker run -p 8000:8000 seo-crawler
```
