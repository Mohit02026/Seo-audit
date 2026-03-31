# SEO Crawler — Technical Audit Tool

A Python-based SEO auditing API built with FastAPI. Crawls any website and produces a full technical SEO audit: on-page signals, Core Web Vitals, broken links, redirect health, sitemap/robots checks, canonical conflicts, hreflang coverage, schema markup, and more. Results are persisted in SQLite and exportable as CSV, JSON, or Markdown reports.

---

## Architecture

The project is split into 8 focused modules:

```
main.py          FastAPI routes + background job orchestration
full_audit.py    FullTechnicalAudit class — wires all modules together
crawler.py       Async HTTP crawler (httpx + BeautifulSoup)
analyzers.py     Post-crawl analysis, redirect checks, external link health
pagespeed.py     Google PageSpeed Insights API integration
reporters.py     Plain-text summary + client/internal Markdown reports
db.py            SQLite helpers (jobs, status, results)
config.py        Constants, user agents, SUMMARY_KEYS
js_worker.py     Playwright subprocess for JS-rendered pages
```

---

## Requirements

- Python 3.11+
- Dependencies in `requirements.txt`:
  ```
  fastapi==0.104.1
  uvicorn==0.24.0
  httpx[http2]==0.27.0
  beautifulsoup4==4.12.2
  pandas>=2.2.3
  playwright==1.58.0
  ```

---

## Setup

### Local

```bash
# 1. Create and activate a virtual environment
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Install Playwright browser (Chromium only)
playwright install chromium

# 4. Start the server
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### Docker

```bash
# Build
docker build -t seo-crawler .

# Run
docker run -p 8000:8000 seo-crawler
```

The Dockerfile handles all Playwright/Chromium system dependencies automatically.

---

## Configuration

Edit `config.py` to set your Google PageSpeed Insights API key:

```python
PAGESPEED_API_KEY = "your-key-here"
```

Get a free key at: https://developers.google.com/speed/docs/insights/v5/get-started

If the key is left blank or invalid, PageSpeed results will be skipped — all other audit data is still collected.

---

## API Reference

Base URL: `http://localhost:8000`

---

### POST `/api/audit`

Start an audit as a background job. Returns immediately with a `job_id`.

**Request body:**

```json
{
  "url": "https://example.com",
  "lead_name": "Client Name",
  "max_pages": 200,
  "js_render": true
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `url` | string | required | Full URL including scheme |
| `lead_name` | string | `"Test Lead"` | Label for reports and job list |
| `max_pages` | integer | `200` | Maximum pages to crawl |
| `js_render` | boolean | `true` | Enable Playwright JS rendering for thin pages |

**Response:**

```json
{
  "job_id": "3f8a91c2-...",
  "status": "queued"
}
```

**Example:**

```bash
curl -X POST http://localhost:8000/api/audit \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com", "lead_name": "Example Co", "max_pages": 100}'
```

---

### GET `/api/status/{job_id}`

Poll the status of a running or completed job.

**Response fields:**

| Field | Description |
|---|---|
| `status` | `queued` / `running` / `completed` / `error` |
| `url` | Site that was audited |
| `lead_name` | Label passed at creation |
| `created_at` | ISO timestamp |
| `updated_at` | ISO timestamp |
| `js_enrichment_used` | Whether JS rendering ran on any page |
| `error` | Error message if status is `error` |
| `result` | Full audit JSON (only when `completed`) |
| `summary` | Plain-text 12-section summary (only when `completed`) |

**Example:**

```bash
curl http://localhost:8000/api/status/3f8a91c2-...
```

---

### GET `/api/jobs`

List recent jobs.

**Query params:**

| Param | Default | Description |
|---|---|---|
| `limit` | `50` | Max jobs to return |

**Example:**

```bash
curl "http://localhost:8000/api/jobs?limit=20"
```

---

### GET `/api/export/{job_id}/csv`

Download all crawled pages as a CSV file. One row per page, all fields included.

```bash
curl http://localhost:8000/api/export/3f8a91c2-.../csv -o audit.csv
```

---

### GET `/api/export/{job_id}/json`

Download the full audit as a JSON file, including `result`, `summary`, and raw `pages_data`.

```bash
curl http://localhost:8000/api/export/3f8a91c2-.../json -o audit.json
```

---

### GET `/api/report/{job_id}/client`

Returns a client-facing Markdown report. Designed to be sent to clients — plain language, 🔴/🟡/✅ signals, no internal implementation details.

```bash
curl http://localhost:8000/api/report/3f8a91c2-.../client
```

---

### GET `/api/report/{job_id}/internal`

Returns a full technical Markdown report with per-page tables, all metrics, and implementation notes. For internal/agency use.

```bash
curl http://localhost:8000/api/report/3f8a91c2-.../internal
```

---

### GET `/test-simple?url=...`

Synchronous single-URL audit (no job queue). Crawls up to 50 pages and returns the full result inline. Useful for quick tests.

```bash
curl "http://localhost:8000/test-simple?url=https://example.com"
```

---

### GET `/summary?url=...`

Same as `/test-simple` but also returns the plain-text 12-section summary alongside the JSON result.

---

### GET `/debug-pages?url=...`

Returns raw `pages_data` array — every crawled page with all extracted fields. Useful for debugging extraction issues.

---

## What Gets Audited

### On-Page Signals (per page)
- Title tag: presence, length (short < 30, long > 60)
- Meta description: presence, length (short < 70, long > 160)
- H1: presence, multiple H1s
- Canonical tag: self-referencing, mismatch vs. final URL, pointing outside crawl
- Robots meta: noindex, nofollow
- OG tags: og:title, og:description, og:image
- Twitter card tags
- Images: alt text missing count
- Mixed content (HTTP assets on HTTPS page)
- Hreflang tags: presence and target languages
- Schema markup: JSON-LD @type values detected
- Pagination: rel=prev/next links
- Word count, paragraph count (thin content detection)
- Response time (ms) and page size (KB)
- HTTP status code and final URL after redirects

### URL Health
- Uppercase letters in path
- Path length > 115 characters
- Underscores in path
- Special characters in path
- Trailing slash inconsistency across the site

### Site-Wide Checks
- robots.txt: exists, allows homepage, crawl-delay directive
- Sitemap: found at `/sitemap.xml`, `/sitemap_index.xml`, `/wp-sitemap.xml`, `/news-sitemap.xml`
- Redirect setup: www ↔ non-www, HTTP → HTTPS
- Broken external links: HEAD/GET check on up to 80 external links
- Duplicate titles across site
- Duplicate meta descriptions across site
- Orphan pages (pages with no internal links pointing to them)
- Canonical conflicts: pages whose canonical points outside the crawled set

### Core Web Vitals (via Google PageSpeed Insights)
Sampled on up to 5 pages (homepage + highest-traffic pages):

| Metric | Source |
|---|---|
| Performance Score | Lighthouse lab (0–100) |
| LCP (Largest Contentful Paint) | CrUX field data, Lighthouse fallback |
| INP (Interaction to Next Paint) | CrUX field data |
| CLS (Cumulative Layout Shift) | CrUX field data |
| FCP (First Contentful Paint) | Lighthouse lab |
| TBT (Total Blocking Time) | Lighthouse lab |
| Speed Index | Lighthouse lab |
| CWV Category | GOOD / NEEDS_IMPROVEMENT / POOR |

---

## JS Rendering

When `js_render: true`, a Playwright subprocess (`js_worker.py`) re-renders pages that appear to have thin content (word count < 200). The worker:

1. Receives sorted pages (lowest word count first)
2. Renders up to `max(3, min(8, thin_page_count))` pages using Chromium
3. Returns enriched `pages_data` — the main audit then re-runs analysis on the enriched set

This targets JS-rendered pages (React/Vue/Angular SPAs) without rendering the entire site through a headless browser, keeping runtime reasonable.

---

## Data Persistence

All jobs are stored in `audits.db` (SQLite) in the project root. The schema:

| Column | Description |
|---|---|
| `job_id` | UUID primary key |
| `status` | queued / running / completed / error |
| `url` | Target site URL |
| `lead_name` | Label |
| `created_at` | ISO timestamp |
| `updated_at` | ISO timestamp |
| `result_json` | Full audit result (JSON text) |
| `summary` | Plain-text 12-section summary |
| `pages_data_json` | Raw per-page crawl data (JSON text) |
| `js_enrichment_used` | 0 or 1 |
| `error` | Error message if failed |

The database is created automatically on first startup. The file persists between restarts.

---

## Typical Workflow

```
1.  POST /api/audit          →  get job_id
2.  GET  /api/status/{id}    →  poll until status = "completed"
3a. GET  /api/report/{id}/client    →  send to client (Markdown)
3b. GET  /api/report/{id}/internal  →  internal review
4.  GET  /api/export/{id}/csv       →  import into spreadsheet
```

---

## Known Limitations

- **Backlinks** — Cannot detect inbound backlinks. This requires crawling the full web. Only outbound external links are checked for health. For backlink data, integrate DataForSEO's API.
- **Authentication** — No API key auth on endpoints currently. Do not expose this publicly without adding auth.
- **Rate limiting** — No built-in rate limiting. Run behind a reverse proxy (nginx/caddy) if exposing externally.
- **JavaScript-heavy SPAs** — JS rendering is sampled, not full-site. Deep SPA content (pagination, filters, infinite scroll) may not be fully captured.
- **PageSpeed quota** — The free PageSpeed API key has a daily request limit. Sampling is capped at 5 pages per audit to stay within limits.
- **Google Search Console** — GSC API is scoped to verified site owners only. Integration requires an OAuth flow and a UI for the consent screen — not yet implemented.
