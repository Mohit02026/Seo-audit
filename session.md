# session.md — Current Project State

_Last updated: 2026-03-31_

## Version: Phase 3 + Phase 4 + Code Quality pass (complete)

## What is done

### Phase 1 — Core crawler
- Async httpx crawler with semaphore concurrency (8 threads)
- robots.txt respect + crawl-delay
- Per-page extraction: title, meta, H1, canonical, robots meta, OG, Twitter, images, schema, hreflang, pagination, word count, links, status codes, response time, page size

### Phase 2 — JS enrichment
- js_worker.py subprocess using Playwright/Chromium
- Smart prioritisation: sorts by word_count ascending, renders only likely JS pages (max 3–8)
- Uses `domcontentloaded + 15000ms` wait (networkidle was too slow)

### Phase 3 — Persistence + exports + reports
- SQLite via db.py (audits.db auto-created)
- Background job queue (POST /api/audit → job_id → poll status)
- CSV and JSON streaming exports
- Client-facing Markdown report (🔴/🟡/✅ signals, plain language)
- Internal Markdown report (per-page table, all metrics)
- 12-section plain-text summary

### Phase 4 — Advanced signals
- Google PageSpeed Insights: 8 metrics per URL (performance_score, lcp_ms, inp_ms, cls, fcp_ms, tbt_ms, speed_index_ms, cwv_category)
- Canonical conflict detection (canonical_points_outside_crawl, canonical_mismatch)
- Hreflang aggregation (pages_with_hreflang count)
- URL hygiene: uppercase, length >115, underscores, special chars
- Trailing slash inconsistency detection
- Broken external link checking (HEAD/GET on up to 80 links)
- Redirect setup audit (www↔non-www, HTTP→HTTPS)
- Orphan page detection
- Duplicate title / duplicate meta detection

### Refactor (complete)
- Split from 2 large files into 8 focused modules
- Method binding pattern in full_audit.py
- All modules stay under token read limits

### Code quality pass (complete)
- API key moved out of source — loaded from `.env` via `python-dotenv` (`config.py`)
- `.gitignore` added — covers `.env`, `__pycache__`, `*.db`, `.venv/`
- HTML parser switched from `html.parser` to `lxml` — 3-5x parse speedup (`crawler.py`)
- URL input validation on `AuditRequest` — rejects non-http/https with 422 before crawl starts (`main.py`)
- Structured logging added (`logging.basicConfig` in `main.py`, `logger` in `crawler.py`, `pagespeed.py`, `analyzers.py`) — prints kept alongside
- `except Exception` blocks narrowed: `_fetch()` and `_crawl_page()` log with `exc_info=True`; `pagespeed.py` splits into `TimeoutException` / `ConnectError` / broad fallback
- DB indexes added on `status` and `created_at` columns (`db.py`)
- `db_purge_old_jobs(days=30)` function + `DELETE /api/jobs/purge` endpoint added
- Content-Type check before BeautifulSoup parse — silently skips PDFs, images, feeds (`crawler.py`)

### Docs (complete)
- README.md — full usage guide
- CLAUDE.md — persistent context for Claude sessions
- session.md — this file

## Deliberately deferred (user decision)
- API key authentication on endpoints
- Rate limiting
- GSC OAuth flow
- UI (needed before GSC can be used by others)
- PageSpeed key rotation (10 keys) + increased sample size (10 URLs, mobile + desktop)

## Next logical steps (priority order)
1. **Crawl diffing** — compare two job_ids, surface changes (new broken pages, title changes, score delta). Pure logic on existing data, no new APIs needed.
2. **Internal PageRank scoring** — rank pages by internal link equity using existing link graph. No external API needed.
3. **GSC OAuth integration** — requires: Google Cloud project, OAuth consent screen, callback endpoint, token storage, and a minimal UI for the auth flow. Client adds their own GSC property.
4. **DataForSEO backlink API** — pay-per-query, pull 1 call per audit. Needs API key config and a new results field.
5. **API auth + rate limiting** — add before any public exposure.

## Last test result
Site: growthdrivendigital.com — 50 pages crawled, JS enrichment used, all audit sections populated successfully.

## Known working curl pattern
```bash
# Start audit
curl -X POST http://localhost:8000/api/audit \
  -H "Content-Type: application/json" \
  -d '{"url":"https://example.com","lead_name":"Test","max_pages":100}'

# Poll until completed
curl http://localhost:8000/api/status/<job_id>

# Get client report
curl http://localhost:8000/api/report/<job_id>/client

# Download CSV
curl http://localhost:8000/api/export/<job_id>/csv -o audit.csv
```

## File sizes (approx, for token planning)
| File | Lines |
|---|---|
| main.py | ~275 |
| full_audit.py | 152 |
| crawler.py | ~360 |
| analyzers.py | ~340 |
| reporters.py | ~540 |
| pagespeed.py | ~95 |
| db.py | ~90 |
| config.py | 56 |
| js_worker.py | ~120 |

## How to start a new session efficiently
1. Read CLAUDE.md (architecture, decisions, patterns) — do this first
2. Read session.md (this file — current state + next steps)
3. Only read specific module files if editing that module
4. Do NOT read all 8 modules at session start — read only what's needed
