import os
from typing import Dict

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# Tried in order — first 200 response wins
_SITEMAP_PATHS = [
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/wp-sitemap.xml",
    "/news-sitemap.xml",
]

# Keys that must exist in every crawl_summary (used to build empty fallback)
SUMMARY_KEYS = [
    "total_pages", "status_2xx", "status_3xx", "status_4xx", "status_5xx",
    "redirect_chains", "missing_titles", "long_titles", "titles_too_short",
    "no_meta_desc", "meta_desc_too_long", "meta_desc_too_short",
    "no_h1", "multi_h1", "noindex_pages",
    "images_missing_alt_total", "pages_with_missing_alt",
    "pages_without_og", "pages_without_twitter",
    "pages_with_mixed_content", "pages_not_https",
    "slow_server_response", "avg_response_time_ms", "avg_page_size_kb",
    "pages_with_schema", "thin_content_pages", "orphan_pages_count",
    "urls_with_uppercase", "urls_too_long", "urls_with_underscores",
    "urls_with_special_chars", "trailing_slash_issues_count",
    "broken_external_links_count",
    "pages_with_hreflang", "canonical_issues_count",
]

PAGESPEED_API_KEY = os.getenv("PAGESPEED_API_KEY", "")


def random_headers(ua: str) -> Dict[str, str]:
    return {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }
