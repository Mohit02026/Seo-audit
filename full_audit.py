"""
full_audit.py — FullTechnicalAudit class.

Holds shared crawl state and wires together methods from:
  crawler.py    — HTTP fetch + page parsing
  analyzers.py  — robots, sitemap, crawl analysis, redirect checks, external links
  pagespeed.py  — Google PageSpeed Insights API
  reporters.py  — plain-text summary
"""
import random
import threading
from collections import deque
from typing import Any, Dict, List
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

# Module-level functions bound as methods below
import crawler as _crawler
import analyzers as _analyzers
import pagespeed as _pagespeed
import reporters as _reporters

from config import _USER_AGENTS, SUMMARY_KEYS


class FullTechnicalAudit:
    def __init__(
        self,
        site_url: str,
        max_pages: int = 50,
        pagespeed_key: str = None,
        threads: int = 8,
        js_render: bool = False,
        js_max_pages: int = 10,
        min_delay: float = 0.2,
        max_delay: float = 0.5,
        max_retries: int = 3,
    ):
        self.site_url = site_url.rstrip("/")
        self.parsed = urlparse(self.site_url)
        self.origin = f"{self.parsed.scheme}://{self.parsed.netloc}"
        self.domain = self.parsed.netloc
        self.max_pages = max_pages
        self.pagespeed_key = pagespeed_key
        self.concurrency = threads
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retries = max_retries

        self._ua = random.choice(_USER_AGENTS)

        # robots.txt
        self.robot_parser = RobotFileParser()
        self.robot_parser.set_url(f"{self.origin}/robots.txt")
        self._crawl_delay: float = 0.0
        try:
            self.robot_parser.read()
            self.robots_exists = True
            cd = self.robot_parser.crawl_delay("*")
            if cd:
                self._crawl_delay = float(cd)
        except Exception:
            self.robots_exists = False

        # Crawl state
        self.visited: set = set()
        self.to_visit: deque = deque([self.site_url])
        self.pages_data: List[Dict[str, Any]] = []
        self.results: Dict[str, Any] = {}
        self._lock = threading.Lock()

        # Link depth + orphan tracking
        self._url_depths: Dict[str, int] = {self.site_url: 0}
        self._link_targets: set = set()

        # Link graph for PageRank (source URL → list of internal destination URLs)
        self._link_graph: Dict[str, List[str]] = {}

        # External link health
        self._external_links_seen: set = set()

        # JS worker flags
        self.js_render = js_render
        self.js_max_pages = js_max_pages
        self.js_rendered_count = 0

    # ------------------------------------------------------------------
    # Bound methods from crawler.py
    # ------------------------------------------------------------------
    _effective_delay        = _crawler._effective_delay
    is_allowed              = _crawler.is_allowed
    _fetch                  = _crawler._fetch
    _crawl_page             = _crawler._crawl_page
    _run_crawl_async        = _crawler._run_crawl_async
    run_crawl               = _crawler.run_crawl

    # ------------------------------------------------------------------
    # Bound methods from analyzers.py
    # ------------------------------------------------------------------
    audit_robots_txt            = _analyzers.audit_robots_txt
    audit_sitemap               = _analyzers.audit_sitemap
    analyze_crawl               = _analyzers.analyze_crawl
    _check_external_links_async = _analyzers._check_external_links_async
    audit_broken_external_links = _analyzers.audit_broken_external_links
    audit_redirect_setup        = _analyzers.audit_redirect_setup
    compute_pagerank            = _analyzers.compute_pagerank

    # ------------------------------------------------------------------
    # Bound methods from pagespeed.py
    # ------------------------------------------------------------------
    _fetch_pagespeed_async  = _pagespeed._fetch_pagespeed_async
    _run_pagespeed_async    = _pagespeed._run_pagespeed_async
    run_pagespeed_sample    = _pagespeed.run_pagespeed_sample

    # ------------------------------------------------------------------
    # Bound methods from reporters.py
    # ------------------------------------------------------------------
    generate_summary        = _reporters.generate_summary

    # ------------------------------------------------------------------
    # Entrypoint
    # ------------------------------------------------------------------

    def run_full_audit(self) -> Dict[str, Any]:
        self.audit_robots_txt()

        if not self.results["robots_txt"].get("allows_homepage", True):
            empty = {k: 0 for k in SUMMARY_KEYS}
            self.results.update({
                "crawl_summary": empty,
                "broken_internal_links": [],
                "broken_external_links": [],
                "duplicate_titles": {},
                "duplicate_meta_descriptions": {},
                "pages_missing_alt": [],
                "orphan_pages": [],
                "trailing_slash_issues": [],
                "redirect_checks": {},
                "pagespeed_sample": [],
                "sitemap": {},
                "audit_status": "CRAWL_BLOCKED_BY_ROBOTS",
            })
            return self.results

        self.audit_sitemap()
        self.run_crawl()
        self.analyze_crawl()
        self.audit_redirect_setup()
        self.audit_broken_external_links()
        self.compute_pagerank()
        if self.pagespeed_key:
            self.run_pagespeed_sample()
        else:
            self.results["pagespeed_sample"] = []
        self.results["audit_status"] = "OK"
        return self.results
