"""
analyzers.py — robots.txt, sitemap, crawl analysis, redirect checks, external links.
All functions take the FullTechnicalAudit instance as first arg.
"""
import asyncio
import logging
from collections import Counter
from typing import Any, Dict, List
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)

import httpx
import pandas as pd

from config import _SITEMAP_PATHS, SUMMARY_KEYS, random_headers


# ---------------------------------------------------------------------------
# Robots.txt
# ---------------------------------------------------------------------------

def audit_robots_txt(self):
    try:
        allows_homepage = self.robot_parser.can_fetch("*", self.site_url)
    except Exception:
        allows_homepage = True

    self.results["robots_txt"] = {
        "exists": self.robots_exists,
        "allows_homepage": allows_homepage,
        "crawl_delay": self._crawl_delay if self._crawl_delay else None,
    }


# ---------------------------------------------------------------------------
# Sitemap
# ---------------------------------------------------------------------------

def audit_sitemap(self):
    headers = random_headers(self._ua)
    for path in _SITEMAP_PATHS:
        sitemap_url = f"{self.origin}{path}"
        try:
            r = httpx.get(sitemap_url, headers=headers, follow_redirects=True, timeout=10)
            if r.status_code != 200:
                continue
            root = ET.fromstring(r.content)
            tag = root.tag.lower()
            is_index = "sitemapindex" in tag
            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            locs = root.findall("sm:sitemap/sm:loc" if is_index else "sm:url/sm:loc", ns)
            if not locs:
                locs = root.findall(".//loc")
            self.results["sitemap"] = {
                "exists": True,
                "url": sitemap_url,
                "is_index": is_index,
                "url_count": len(locs),
            }
            return
        except ET.ParseError:
            self.results["sitemap"] = {
                "exists": True, "url": sitemap_url, "error": "invalid_xml"
            }
            return
        except Exception:
            continue
    self.results["sitemap"] = {"exists": False, "checked_paths": _SITEMAP_PATHS}


# ---------------------------------------------------------------------------
# Crawl analysis
# ---------------------------------------------------------------------------

def analyze_crawl(self):
    empty = {k: 0 for k in SUMMARY_KEYS}

    if not self.pages_data:
        self.results["crawl_summary"] = empty
        self.results["broken_internal_links"] = []
        self.results["duplicate_titles"] = {}
        self.results["duplicate_meta_descriptions"] = {}
        self.results["pages_missing_alt"] = []
        self.results["orphan_pages"] = []
        self.results["trailing_slash_issues"] = []
        return

    df = pd.DataFrame(self.pages_data)
    df_valid = df[df["status_code"].notna()].copy()
    df_valid["status_code"] = df_valid["status_code"].astype(int)

    def col(name, default=False):
        return df_valid[name] if name in df_valid.columns else pd.Series([default] * len(df_valid))

    summary: Dict[str, Any] = {
        "total_pages":              int(len(df_valid)),
        "status_2xx":               int(len(df_valid[df_valid["status_code"] < 300])),
        "status_3xx":               int(len(df_valid[(df_valid["status_code"] >= 300) & (df_valid["status_code"] < 400)])),
        "status_4xx":               int(len(df_valid[(df_valid["status_code"] >= 400) & (df_valid["status_code"] < 500)])),
        "status_5xx":               int(len(df_valid[df_valid["status_code"] >= 500])),
        "redirect_chains":          int(col("redirect_chain", 0).sum()),
        "missing_titles":           int(len(df_valid[col("title", "") == ""])),
        "long_titles":              int(len(df_valid[col("title_length", 0) > 60])),
        "titles_too_short":         int(len(df_valid[col("title_too_short", False)])),
        "no_meta_desc":             int(len(df_valid[~col("has_meta_desc", False)])),
        "meta_desc_too_long":       int(len(df_valid[col("meta_desc_too_long", False)])),
        "meta_desc_too_short":      int(len(df_valid[col("meta_desc_too_short", False)])),
        "no_h1":                    int(len(df_valid[col("h1_count", 0) == 0])),
        "multi_h1":                 int(len(df_valid[col("h1_count", 0) > 1])),
        "noindex_pages":            int(len(df_valid[col("noindex", False)])),
        "images_missing_alt_total": int(col("images_missing_alt", 0).sum()),
        "pages_with_missing_alt":   int(len(df_valid[col("images_missing_alt", 0) > 0])),
        "pages_without_og":         int(len(df_valid[~col("has_og", False)])),
        "pages_without_twitter":    int(len(df_valid[~col("has_twitter_card", False)])),
        "pages_with_mixed_content": int(len(df_valid[col("mixed_content", False)])),
        "pages_not_https":          int(len(df_valid[~col("is_https", True)])),
        "avg_response_time_ms":     int(col("response_time_ms", 0).mean()) if "response_time_ms" in df_valid.columns else 0,
        "slow_server_response":     int(len(df_valid[col("response_time_ms", 0) > 3000])),
        "avg_page_size_kb":         round(float(col("page_size_kb", 0).mean()), 1) if "page_size_kb" in df_valid.columns else 0.0,
        "pages_with_schema":        int(len(df_valid[col("has_schema", False)])),
        "thin_content_pages":       int(len(df_valid[col("is_thin_content", False)])),
        "urls_with_uppercase":      int(col("url_has_uppercase", False).sum()),
        "urls_too_long":            int(col("url_too_long", False).sum()),
        "urls_with_underscores":    int(col("url_has_underscores", False).sum()),
        "urls_with_special_chars":  int(col("url_has_special_chars", False).sum()),
        "broken_external_links_count": 0,  # updated after audit_broken_external_links
        "pages_with_hreflang":      int(col("has_hreflang", False).sum()),
        "canonical_issues_count":   0,     # updated below
    }

    # Canonical conflict detection
    crawled_urls: set = set()
    for page in self.pages_data:
        crawled_urls.add(page.get("url", "").rstrip("/"))
        crawled_urls.add(page.get("final_url", "").rstrip("/"))

    canonical_issues: List[Dict[str, str]] = []
    for page in self.pages_data:
        canonical = page.get("canonical", "").rstrip("/")
        url_raw = page.get("url", "").rstrip("/")
        final_raw = page.get("final_url", url_raw).rstrip("/")
        if not canonical or canonical == url_raw or canonical == final_raw:
            continue
        if canonical not in crawled_urls:
            canonical_issues.append({
                "url": page.get("url", ""),
                "canonical": page.get("canonical", ""),
                "issue": "canonical_points_outside_crawl",
            })
        else:
            canonical_issues.append({
                "url": page.get("url", ""),
                "canonical": page.get("canonical", ""),
                "final_url": page.get("final_url", ""),
                "issue": "canonical_mismatch",
            })
    self.results["canonical_issues"] = canonical_issues[:30]
    summary["canonical_issues_count"] = len(canonical_issues)

    # Trailing slash inconsistency
    trailing_slash_issues: List[Dict[str, str]] = []
    for page in self.pages_data:
        u = page.get("url", "")
        fu = page.get("final_url", u)
        if u and fu and u != fu:
            pu = urlparse(u)
            pfu = urlparse(fu)
            if (pu.netloc == pfu.netloc
                    and pu.path.rstrip("/") == pfu.path.rstrip("/")
                    and pu.path != pfu.path):
                trailing_slash_issues.append({"requested": u, "final": fu})
    self.results["trailing_slash_issues"] = trailing_slash_issues[:20]
    summary["trailing_slash_issues_count"] = len(trailing_slash_issues)

    # Orphan pages
    orphan_urls: List[str] = []
    seed = self.site_url.rstrip("/")
    for page in self.pages_data:
        u = page.get("url", "").rstrip("/")
        fu = page.get("final_url", u).rstrip("/")
        if u == seed or fu == seed:
            continue
        original_url = page.get("url", "")
        final_url = page.get("final_url", "")
        if original_url not in self._link_targets and final_url not in self._link_targets:
            orphan_urls.append(original_url)
    self.results["orphan_pages"] = orphan_urls[:20]
    summary["orphan_pages_count"] = len(orphan_urls)

    self.results["crawl_summary"] = summary

    # Duplicates
    title_counts = Counter(df_valid["title"].tolist() if "title" in df_valid.columns else [])
    self.results["duplicate_titles"] = dict(
        list({t: c for t, c in title_counts.items() if t and c > 1}.items())[:10]
    )
    if "meta_description" in df_valid.columns:
        desc_counts = Counter(df_valid["meta_description"].tolist())
        self.results["duplicate_meta_descriptions"] = dict(
            list({d: c for d, c in desc_counts.items() if d and c > 1}.items())[:10]
        )
    else:
        self.results["duplicate_meta_descriptions"] = {}

    broken = df_valid[df_valid["status_code"] >= 400][["url", "status_code"]]
    self.results["broken_internal_links"] = broken.to_dict(orient="records")

    if "images_missing_alt" in df_valid.columns:
        missing_alt_df = df_valid[df_valid["images_missing_alt"] > 0][
            ["url", "images_total", "images_missing_alt"]
        ].head(20)
        self.results["pages_missing_alt"] = missing_alt_df.to_dict(orient="records")
    else:
        self.results["pages_missing_alt"] = []


# ---------------------------------------------------------------------------
# Broken external links
# ---------------------------------------------------------------------------

async def _check_external_links_async(self):
    urls = list(self._external_links_seen)[:80]
    sem = asyncio.Semaphore(8)

    async def check_one(client: httpx.AsyncClient, url: str):
        try:
            r = await client.head(
                url, follow_redirects=True, timeout=8,
                headers=random_headers(self._ua),
            )
            if r.status_code == 405:
                r = await client.get(
                    url, follow_redirects=True, timeout=8,
                    headers=random_headers(self._ua),
                )
            if r.status_code >= 400:
                return {"url": url, "status_code": r.status_code}
        except (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError):
            pass
        except Exception:
            pass
        return None

    async def bounded(url: str):
        async with sem:
            return await check_one(client, url)

    async with httpx.AsyncClient(http2=False, timeout=10) as client:
        results = await asyncio.gather(*[bounded(u) for u in urls])

    broken = [r for r in results if r is not None]
    self.results["broken_external_links"] = broken
    if "crawl_summary" in self.results:
        self.results["crawl_summary"]["broken_external_links_count"] = len(broken)
    logger.info("External links checked=%d broken=%d", len(urls), len(broken))
    print(f"🔗 External links checked: {len(urls)}, broken: {len(broken)}")


def audit_broken_external_links(self):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(asyncio.run, self._check_external_links_async()).result()
        else:
            loop.run_until_complete(self._check_external_links_async())
    except RuntimeError:
        asyncio.run(self._check_external_links_async())


# ---------------------------------------------------------------------------
# Redirect setup check (www, http→https)
# ---------------------------------------------------------------------------

def audit_redirect_setup(self):
    parsed = urlparse(self.site_url)
    domain = parsed.netloc
    checks: Dict[str, Any] = {}

    # www ↔ non-www
    alt_domain = domain[4:] if domain.startswith("www.") else f"www.{domain}"
    test_url = f"{parsed.scheme}://{alt_domain}"
    try:
        r = httpx.get(test_url, follow_redirects=False, timeout=8, headers=random_headers(self._ua))
        if r.status_code in (301, 302, 307, 308):
            checks["www_redirect"] = {
                "status": "ok",
                "from": test_url,
                "redirects_to": r.headers.get("location", ""),
                "status_code": r.status_code,
            }
        elif r.status_code == 200:
            checks["www_redirect"] = {
                "status": "warning",
                "detail": f"{test_url} returns 200 without redirecting — potential duplicate content",
                "status_code": 200,
            }
        else:
            checks["www_redirect"] = {
                "status": "info",
                "detail": f"{test_url} returned {r.status_code}",
                "status_code": r.status_code,
            }
    except Exception as e:
        checks["www_redirect"] = {"status": "error", "detail": str(e)}

    # HTTP → HTTPS
    if parsed.scheme == "https":
        http_url = f"http://{domain}"
        try:
            r = httpx.get(http_url, follow_redirects=False, timeout=8, headers=random_headers(self._ua))
            if r.status_code in (301, 302, 307, 308):
                checks["http_to_https"] = {
                    "status": "ok",
                    "redirects_to": r.headers.get("location", ""),
                    "status_code": r.status_code,
                }
            else:
                checks["http_to_https"] = {
                    "status": "warning",
                    "detail": f"http:// returns {r.status_code} instead of redirecting to HTTPS",
                    "status_code": r.status_code,
                }
        except Exception as e:
            checks["http_to_https"] = {"status": "error", "detail": str(e)}
    else:
        checks["http_to_https"] = {"status": "n/a", "detail": "Site is not HTTPS"}

    self.results["redirect_checks"] = checks
    logger.info("Redirect checks done www=%s http_to_https=%s",
                checks.get('www_redirect', {}).get('status'),
                checks.get('http_to_https', {}).get('status'))
    print(f"🔀 Redirect checks done: www={checks.get('www_redirect',{}).get('status')}  "
          f"http→https={checks.get('http_to_https',{}).get('status')}")


# ---------------------------------------------------------------------------
# Internal PageRank
# ---------------------------------------------------------------------------

def compute_pagerank(self):
    """Iterative PageRank on the internal link graph. Adds pagerank_score to each page."""
    graph = getattr(self, "_link_graph", {})
    pages = [p["url"] for p in self.pages_data]
    if not pages or not graph:
        for p in self.pages_data:
            p["pagerank_score"] = 0.0
        self.results["top_pagerank_pages"] = []
        return

    damping = 0.85
    n = len(pages)
    scores = {url: 1.0 / n for url in pages}
    page_set = set(pages)

    for _ in range(50):
        new_scores = {url: (1 - damping) / n for url in pages}
        for src, dests in graph.items():
            if src not in page_set:
                continue
            internal_dests = [d for d in dests if d in page_set]
            if not internal_dests:
                continue
            share = scores[src] / len(internal_dests)
            for dest in internal_dests:
                new_scores[dest] = new_scores.get(dest, 0) + damping * share
        scores = new_scores

    max_score = max(scores.values()) or 1.0
    for p in self.pages_data:
        raw = scores.get(p["url"], 0.0)
        p["pagerank_score"] = round(raw / max_score, 4)

    top = sorted(self.pages_data, key=lambda p: p["pagerank_score"], reverse=True)[:20]
    self.results["top_pagerank_pages"] = [
        {"url": p["url"], "pagerank_score": p["pagerank_score"], "title": p.get("title", "")}
        for p in top
    ]
    logger.info("PageRank computed pages=%d", len(pages))
