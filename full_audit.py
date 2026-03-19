import requests
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import pandas as pd
from collections import deque, Counter
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Optional


class FullTechnicalAudit:
    def __init__(
        self,
        site_url: str,
        max_pages: int = 50,
        pagespeed_key: str = None,
        threads: int = 3,
    ):
        self.site_url = site_url.rstrip("/")
        self.parsed = urlparse(self.site_url)
        self.origin = f"{self.parsed.scheme}://{self.parsed.netloc}"
        self.domain = self.parsed.netloc
        self.max_pages = max_pages
        self.pagespeed_key = pagespeed_key
        self.threads = threads

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                )
            }
        )

        # robots.txt
        self.robot_parser = RobotFileParser()
        self.robot_parser.set_url(f"{self.origin}/robots.txt")
        try:
            self.robot_parser.read()
            self.robots_exists = True
        except Exception:
            self.robots_exists = False

        self.visited = set()
        self.to_visit = deque([self.site_url])
        self.pages_data: List[Dict[str, Any]] = []
        self.results: Dict[str, Any] = {}
        self.lock = threading.Lock()

    # ------------------- CORE CRAWLER -------------------

    def is_allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.netloc != self.domain:
            return False
        try:
            return self.robot_parser.can_fetch("*", url)
        except Exception:
            return True

    def crawl_page(self, url: str) -> Optional[Dict[str, Any]]:
        if url in self.visited:
            return None

        with self.lock:
            self.visited.add(url)

        if not self.is_allowed(url):
            return {"url": url, "skipped": "robots.txt"}

        try:
            resp = self.session.get(url, timeout=10, allow_redirects=True)
            final_url = resp.url
            status = resp.status_code
            redirect_chain = len(resp.history)

            soup = BeautifulSoup(resp.text, "html.parser")

            # Title
            title = soup.find("title")
            title_text = title.get_text().strip()[:150] if title else ""

            # Meta description
            meta_desc = soup.find("meta", attrs={"name": "description"})
            desc_text = meta_desc.get("content", "").strip()[:300] if meta_desc else ""

            # H1
            h1_tags = soup.find_all("h1")
            h1_texts = [h.get_text().strip() for h in h1_tags if h.get_text().strip()]
            h1_count = len(h1_texts)
            h1_sample = h1_texts[0][:150] if h1_texts else ""

            # Canonical
            canonical_tag = soup.find(
                "link", rel=lambda v: v and "canonical" in v.lower()
            )
            canonical_url = canonical_tag.get("href") if canonical_tag else ""

            # Meta robots
            robots_meta = soup.find("meta", attrs={"name": "robots"})
            robots_content = robots_meta.get("content", "").lower() if robots_meta else ""
            noindex = "noindex" in robots_content
            nofollow = "nofollow" in robots_content

            # X-Robots-Tag (HTTP header)
            x_robots = resp.headers.get("X-Robots-Tag", "").lower()
            if "noindex" in x_robots:
                noindex = True
            if "nofollow" in x_robots:
                nofollow = True

            # Word count (rough)
            text_content = soup.get_text(separator=" ")
            word_count = len(text_content.split())

            # Internal/external links
            internal_links = []
            external_links = []
            for a in soup.find_all("a", href=True):
                link = urljoin(url, a["href"])
                parsed_link = urlparse(link)
                if not parsed_link.scheme.startswith("http"):
                    continue
                if parsed_link.netloc == self.domain:
                    internal_links.append(link)
                else:
                    external_links.append(link)

            page_data = {
                "url": url,
                "final_url": final_url,
                "status_code": status,
                "redirect_chain": redirect_chain,
                "title": title_text,
                "title_length": len(title_text),
                "meta_description": desc_text,
                "has_meta_desc": bool(desc_text),
                "h1_count": h1_count,
                "h1_sample": h1_sample,
                "canonical": canonical_url,
                "meta_robots": robots_content,
                "noindex": noindex,
                "nofollow": nofollow,
                "word_count": word_count,
                "internal_links_count": len(internal_links),
                "external_links_count": len(external_links),
            }

            with self.lock:
                self.pages_data.append(page_data)
                # Enqueue next internal links
                for link in internal_links[:15]:
                    if (
                        len(self.visited) + len(self.to_visit) < self.max_pages
                        and link not in self.visited
                    ):
                        self.to_visit.append(link)

            return page_data

        except Exception as e:
            return {"url": url, "error": str(e)}

    def run_crawl(self):
        print(f"🐛 Crawling up to {self.max_pages} pages from {self.site_url}...")
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            while self.to_visit and len(self.visited) < self.max_pages:
                batch = [
                    self.to_visit.popleft()
                    for _ in range(min(5, len(self.to_visit)))
                ]
                futures = [executor.submit(self.crawl_page, u) for u in batch]
                for f in futures:
                    f.result()
                time.sleep(0.3)
        print(f"✅ Crawled {len(self.pages_data)} pages")

    # ------------------- ANALYSIS -------------------

    def audit_robots_txt(self):
        try:
            allows_homepage = self.robot_parser.can_fetch("*", self.site_url)
        except Exception:
            allows_homepage = True

        self.results["robots_txt"] = {
            "exists": self.robots_exists,
            "allows_homepage": allows_homepage,
        }

    def analyze_crawl(self):
        if not self.pages_data:
            self.results["crawl_summary"] = {
                "total_pages": 0,
                "status_2xx": 0,
                "status_3xx": 0,
                "status_4xx": 0,
                "status_5xx": 0,
                "redirect_chains": 0,
                "missing_titles": 0,
                "long_titles": 0,
                "no_meta_desc": 0,
                "no_h1": 0,
                "multi_h1": 0,
                "noindex_pages": 0,
            }
            self.results["broken_internal_links"] = []
            self.results["duplicate_titles"] = {}
            return

        df = pd.DataFrame(self.pages_data)

        # Basic status breakdown
        self.results["crawl_summary"] = {
            "total_pages": int(len(df)),
            "status_2xx": int(len(df[df["status_code"] < 300])),
            "status_3xx": int(
                len(df[(df["status_code"] >= 300) & (df["status_code"] < 400)])
            ),
            "status_4xx": int(
                len(df[(df["status_code"] >= 400) & (df["status_code"] < 500)])
            ),
            "status_5xx": int(len(df[df["status_code"] >= 500])),
            "redirect_chains": int(df["redirect_chain"].sum()),
            "missing_titles": int(len(df[df["title"] == ""])),
            "long_titles": int(len(df[df["title_length"] > 60])),
            "no_meta_desc": int(len(df[~df["has_meta_desc"]])),
            "no_h1": int(len(df[df["h1_count"] == 0])),
            "multi_h1": int(len(df[df["h1_count"] > 1])),
            "noindex_pages": int(len(df[df["noindex"]])),
        }

        # Duplicate titles
        title_counts = Counter(df["title"].tolist())
        duplicate_titles = {t: c for t, c in title_counts.items() if t and c > 1}
        self.results["duplicate_titles"] = dict(list(duplicate_titles.items())[:10])

        # Broken internal links: any URL we crawled that is 4xx/5xx
        broken = df[df["status_code"] >= 400][["url", "status_code"]]
        self.results["broken_internal_links"] = broken.to_dict(orient="records")

    # ------------------- PAGESPEED INSIGHTS (SAMPLE) -------------------

    def fetch_pagespeed(self, url: str) -> Optional[Dict[str, Any]]:
        if not self.pagespeed_key:
            return None

        api = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
        params = {
            "url": url,
            "strategy": "mobile",
            "key": self.pagespeed_key,
        }
        try:
            r = requests.get(api, params=params, timeout=60)
            if r.status_code != 200:
                return {"url": url, "error": f"status {r.status_code}"}
            data = r.json()

            # Prefer field data (CrUX)
            loading = data.get("loadingExperience", {}).get("metrics", {})
            lcp = loading.get("LARGEST_CONTENTFUL_PAINT_MS", {}).get("percentile")
            inp = loading.get("INTERACTION_TO_NEXT_PAINT_MS", {}).get("percentile")
            cls_raw = loading.get("CUMULATIVE_LAYOUT_SHIFT_SCORE", {}).get("percentile")
            cls = cls_raw / 100 if cls_raw is not None else None
            category = loading.get("EXPERIMENTAL_CWV_OVERALL", {}).get(
                "category", "Unknown"
            )

            # Fallback to Lighthouse lab metrics if needed
            audits = data.get("lighthouseResult", {}).get("audits", {})
            if lcp is None:
                lcp = audits.get("largest-contentful-paint", {}).get("numericValue")
            if inp is None:
                inp = audits.get("interaction-to-next-paint", {}).get("numericValue")
            if cls is None:
                cls = audits.get("cumulative-layout-shift", {}).get("numericValue")

            return {
                "url": url,
                "lcp_ms": lcp,
                "inp_ms": inp,
                "cls": cls,
                "cwv_category": category,
            }
        except Exception as e:
            return {"url": url, "error": str(e)}

    def run_pagespeed_sample(self):
        """Sample: homepage + first 2 other URLs"""
        if not self.pagespeed_key or not self.pages_data:
            self.results["pagespeed_sample"] = []
            return

        urls: List[str] = [self.site_url]
        for page in self.pages_data:
            if page["url"] != self.site_url and len(urls) < 3:
                urls.append(page["url"])

        cwv_results: List[Dict[str, Any]] = []
        for u in urls:
            res = self.fetch_pagespeed(u)
            if res:
                cwv_results.append(res)

        self.results["pagespeed_sample"] = cwv_results

    # ------------------- ENTRYPOINT -------------------

    def run_full_audit(self):
        self.audit_robots_txt()

        # If robots disallows homepage, do not crawl; mark status
        allows = self.results["robots_txt"].get("allows_homepage", True)
        if not allows:
            self.results["crawl_summary"] = {
                "total_pages": 0,
                "status_2xx": 0,
                "status_3xx": 0,
                "status_4xx": 0,
                "status_5xx": 0,
                "redirect_chains": 0,
                "missing_titles": 0,
                "long_titles": 0,
                "no_meta_desc": 0,
                "no_h1": 0,
                "multi_h1": 0,
                "noindex_pages": 0,
            }
            self.results["broken_internal_links"] = []
            self.results["duplicate_titles"] = {}
            self.results["pagespeed_sample"] = []
            self.results["audit_status"] = "CRAWL_BLOCKED_BY_ROBOTS"
            return self.results

        # Normal path
        self.run_crawl()
        self.analyze_crawl()
        if self.pagespeed_key:
            self.run_pagespeed_sample()
        else:
            self.results["pagespeed_sample"] = []
        self.results["audit_status"] = "OK"
        return self.results
    

    def generate_summary(self) -> str:
        """Generate a human-readable text summary of the audit."""
        summary = self.results.get("crawl_summary", {})
        robots_info = self.results.get("robots_txt", {})
        pagespeed = self.results.get("pagespeed_sample", [])
        audit_status = self.results.get("audit_status", "UNKNOWN")

        lines = []
        lines.append("=" * 60)
        lines.append("SEO TECHNICAL AUDIT REPORT")
        lines.append(f"URL: {self.site_url}")
        lines.append(f"Status: {audit_status}")
        lines.append("=" * 60)
        lines.append("")

        # Robots & Crawlability
        lines.append("ROBOTS & CRAWLABILITY")
        lines.append("-" * 60)
        lines.append(f"Robots.txt exists: {robots_info.get('exists', 'Unknown')}")
        lines.append(f"Homepage allowed: {robots_info.get('allows_homepage', 'Unknown')}")
        if audit_status == "CRAWL_BLOCKED_BY_ROBOTS":
            lines.append("")
            lines.append("Crawl blocked by robots.txt - cannot audit further.")
            return "\n".join(lines)
        lines.append("")

        # Crawl Summary
        lines.append("CRAWL SUMMARY")
        lines.append("-" * 60)
        lines.append(f"Pages crawled: {summary.get('total_pages', 0)}")
        lines.append(f"Status 2xx (OK): {summary.get('status_2xx', 0)}")
        lines.append(f"Status 3xx (Redirect): {summary.get('status_3xx', 0)}")
        lines.append(f"Status 4xx (Client Error): {summary.get('status_4xx', 0)}")
        lines.append(f"Status 5xx (Server Error): {summary.get('status_5xx', 0)}")
        lines.append(f"Redirect chains found: {summary.get('redirect_chains', 0)}")
        lines.append("")

        # On-Page SEO Issues
        lines.append("ON-PAGE SEO ISSUES")
        lines.append("-" * 60)
        lines.append(f"Pages missing title: {summary.get('missing_titles', 0)}")
        lines.append(f"Pages with long title (>60 chars): {summary.get('long_titles', 0)}")
        lines.append(f"Pages missing meta description: {summary.get('no_meta_desc', 0)}")
        lines.append(f"Pages with no H1: {summary.get('no_h1', 0)}")
        lines.append(f"Pages with multiple H1s: {summary.get('multi_h1', 0)}")
        lines.append(f"Pages with noindex tag: {summary.get('noindex_pages', 0)}")
        lines.append("")

        # Duplicate Titles
        duplicates = self.results.get("duplicate_titles", {})
        if duplicates:
            lines.append("DUPLICATE TITLES (Top 10)")
            lines.append("-" * 60)
            for title, count in list(duplicates.items())[:10]:
                lines.append(f"[{count}x] {title[:80]}")
            lines.append("")

        # Broken Links
        broken = self.results.get("broken_internal_links", [])
        if broken:
            lines.append("BROKEN INTERNAL LINKS (4xx/5xx)")
            lines.append("-" * 60)
            for item in broken[:10]:
                lines.append(f"{item.get('status_code', '?')} - {item.get('url', '')[:100]}")
            lines.append("")

        # Core Web Vitals
        if pagespeed:
            lines.append("CORE WEB VITALS (Lab Metrics - Mobile)")
            lines.append("-" * 60)
            for item in pagespeed:
                url = item.get("url", "Unknown")[:80]
                if "error" in item:
                    lines.append(f"{url}: ERROR - {item['error']}")
                else:
                    lcp = item.get("lcp_ms")
                    inp = item.get("inp_ms")
                    cls = item.get("cls")
                    if lcp is not None:
                        if lcp < 2500:
                            lcp_status = "Good"
                        elif lcp < 4000:
                            lcp_status = "Needs work"
                        else:
                            lcp_status = "Poor"
                    else:
                        lcp_status = "N/A"
                    if cls is not None:
                        if cls < 0.1:
                            cls_status = "Good"
                        elif cls < 0.25:
                            cls_status = "Needs work"
                        else:
                            cls_status = "Poor"
                    else:
                        cls_status = "N/A"

                    lines.append(f"URL: {url}")
                    lines.append(f"  LCP: {lcp:.0f} ms ({lcp_status})" if lcp is not None else "  LCP: N/A")
                    lines.append(f"  INP: {inp:.0f} ms" if inp is not None else "  INP: N/A")
                    lines.append(f"  CLS: {cls:.3f} ({cls_status})" if cls is not None else "  CLS: N/A")
            lines.append("")

        # Recommendations
        lines.append("RECOMMENDATIONS")
        lines.append("-" * 60)
        issues = []
        if summary.get("no_meta_desc", 0) > 5:
            issues.append("• Add missing meta descriptions to important pages.")
        if summary.get("no_h1", 0) > 0:
            issues.append("• Add H1 tags to pages without them.")
        if summary.get("multi_h1", 0) > 0:
            issues.append("• Reduce multiple H1s to a single main H1 per page.")
        if summary.get("long_titles", 0) > 5:
            issues.append("• Shorten long titles (>60 chars) to avoid truncation.")
        if duplicates:
            issues.append("• Make duplicate titles unique for each page.")
        if broken:
            issues.append("• Fix broken internal links returning 4xx/5xx.")
        if pagespeed:
            for item in pagespeed:
                if "error" not in item:
                    lcp = item.get("lcp_ms")
                    cls = item.get("cls")
                    if lcp is not None and lcp > 4000:
                        issues.append("• Improve LCP by optimizing images and reducing JavaScript.")
                    if cls is not None and cls > 0.25:
                        issues.append("• Reduce CLS by reserving space for images and avoiding layout shifts.")

        if issues:
            for issue in issues:
                lines.append(issue)
        else:
            lines.append("No major issues detected.")

        lines.append("")
        lines.append("=" * 60)

        return "\n".join(lines)

