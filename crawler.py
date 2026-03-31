"""
crawler.py — async HTTP crawling engine.
All functions take the FullTechnicalAudit instance as first arg so they can be
bound as methods in full_audit.py.
"""
import asyncio
import json as _json
import logging
import re
import random
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

import httpx
from bs4 import BeautifulSoup

from config import random_headers


# ---------------------------------------------------------------------------
# Utility — also used by analyzers.py
# ---------------------------------------------------------------------------

def extract_schema_types(soup: BeautifulSoup) -> List[str]:
    """Extract JSON-LD @type values from a parsed page."""
    types: List[str] = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            raw = script.string or ""
            data = _json.loads(raw)
            if isinstance(data, dict):
                t = data.get("@type", "")
                if t:
                    types.append(str(t))
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        t = item.get("@type", "")
                        if t:
                            types.append(str(t))
        except Exception:
            pass
    return types


# ---------------------------------------------------------------------------
# Methods (bound to FullTechnicalAudit in full_audit.py)
# ---------------------------------------------------------------------------

def _effective_delay(self) -> float:
    base = random.uniform(self.min_delay, self.max_delay)
    return max(base, self._crawl_delay)


def is_allowed(self, url: str) -> bool:
    parsed = urlparse(url)
    if parsed.netloc != self.domain:
        return False
    try:
        return self.robot_parser.can_fetch("*", url)
    except Exception:
        return True


async def _fetch(self, client: httpx.AsyncClient, url: str) -> Optional[httpx.Response]:
    headers = random_headers(self._ua)
    delay = 1.0
    for attempt in range(self.max_retries):
        try:
            resp = await client.get(url, headers=headers, follow_redirects=True, timeout=12)
            if resp.status_code in (429, 503):
                await asyncio.sleep(delay + random.uniform(0, 0.5))
                delay *= 2
                continue
            return resp
        except (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError):
            if attempt < self.max_retries - 1:
                await asyncio.sleep(delay)
                delay *= 2
            else:
                return None
        except Exception:
            logger.warning("Unexpected fetch error url=%s", url, exc_info=True)
            return None
    return None


async def _crawl_page(self, client: httpx.AsyncClient, url: str) -> Optional[Dict[str, Any]]:
    with self._lock:
        if url in self.visited:
            return None
        self.visited.add(url)

    if not self.is_allowed(url):
        return {"url": url, "skipped": "robots.txt"}

    t0 = time.monotonic()
    resp = await self._fetch(client, url)
    response_time_ms = round((time.monotonic() - t0) * 1000)

    if resp is None:
        return {"url": url, "error": "fetch_failed"}

    content_type = resp.headers.get("content-type", "")
    if "text/html" not in content_type:
        return None

    final_url = str(resp.url)
    status = resp.status_code
    redirect_chain = len(resp.history)
    page_size_kb = round(len(resp.content) / 1024, 1)

    try:
        soup = BeautifulSoup(resp.text, "lxml")
    except Exception:
        logger.warning("Parse failed url=%s", url, exc_info=True)
        return {"url": url, "final_url": final_url, "status_code": status, "error": "parse_failed"}

    # Title
    title_tag = soup.find("title")
    title_text = title_tag.get_text().strip()[:150] if title_tag else ""

    # Meta description
    meta_desc_tag = soup.find("meta", attrs={"name": "description"})
    desc_text = meta_desc_tag.get("content", "").strip()[:300] if meta_desc_tag else ""

    # Headings
    h1_tags = soup.find_all("h1")
    h1_texts = [h.get_text().strip() for h in h1_tags if h.get_text().strip()]
    h1_count = len(h1_texts)
    h1_sample = h1_texts[0][:150] if h1_texts else ""
    h2_count = len(soup.find_all("h2"))
    h3_count = len(soup.find_all("h3"))

    # Canonical
    canonical_tag = soup.find("link", rel=lambda v: v and "canonical" in v.lower())
    canonical_url = canonical_tag.get("href", "") if canonical_tag else ""

    # Meta robots + X-Robots-Tag
    robots_meta = soup.find("meta", attrs={"name": "robots"})
    robots_content = robots_meta.get("content", "").lower() if robots_meta else ""
    noindex = "noindex" in robots_content
    nofollow = "nofollow" in robots_content
    x_robots = resp.headers.get("x-robots-tag", "").lower()
    if "noindex" in x_robots:
        noindex = True
    if "nofollow" in x_robots:
        nofollow = True

    # Word count
    word_count = len(soup.get_text(separator=" ").split())

    # Images
    images = soup.find_all("img")
    images_total = len(images)
    images_missing_alt = sum(1 for img in images if not img.get("alt", "").strip())

    # Open Graph
    og_title_tag = soup.find("meta", property="og:title")
    og_title = og_title_tag.get("content", "").strip()[:150] if og_title_tag else ""
    og_desc_tag = soup.find("meta", property="og:description")
    og_description = og_desc_tag.get("content", "").strip()[:300] if og_desc_tag else ""
    og_image_tag = soup.find("meta", property="og:image")
    og_image = og_image_tag.get("content", "").strip() if og_image_tag else ""
    has_og = bool(og_title or og_description)

    # Twitter Card
    twitter_card_tag = soup.find("meta", attrs={"name": "twitter:card"})
    twitter_card = twitter_card_tag.get("content", "").strip() if twitter_card_tag else ""
    has_twitter_card = bool(twitter_card)

    # Mixed content
    mixed_content = False
    if url.startswith("https://"):
        for tag in soup.find_all(["img", "script", "iframe"]):
            if tag.get("src", "").startswith("http://"):
                mixed_content = True
                break
        if not mixed_content:
            for tag in soup.find_all("link"):
                if tag.get("href", "").startswith("http://"):
                    mixed_content = True
                    break

    # URL structure
    parsed_url = urlparse(url)
    url_length = len(url)
    url_depth = len([p for p in parsed_url.path.split("/") if p])
    has_query_params = bool(parsed_url.query)
    is_https = parsed_url.scheme == "https"

    # Hreflang
    hreflang_tags = [
        tag.get("hreflang", "")
        for tag in soup.find_all("link", rel="alternate")
        if tag.get("hreflang")
    ]
    has_hreflang = bool(hreflang_tags)
    hreflang_count = len(hreflang_tags)

    # Structured data
    schema_types = extract_schema_types(soup)
    has_microdata = bool(soup.find(attrs={"itemtype": True}))
    has_schema = bool(schema_types) or has_microdata

    # Pagination
    has_prev = bool(soup.find("link", rel=lambda v: v and "prev" in v.lower()))
    has_next = bool(soup.find("link", rel=lambda v: v and "next" in v.lower()))

    # Content quality
    is_thin_content = word_count < 300
    title_too_short = 0 < len(title_text) < 30
    meta_desc_too_long = len(desc_text) > 160
    meta_desc_too_short = 0 < len(desc_text) < 120

    # URL hygiene
    url_path_only = parsed_url.path
    url_has_uppercase = bool(re.search(r'[A-Z]', url_path_only))
    url_too_long = url_length > 115
    url_has_underscores = '_' in url_path_only
    url_has_special_chars = bool(re.search(r'[%\+]', url_path_only))

    # Links
    internal_links: List[str] = []
    external_links: List[str] = []
    for a in soup.find_all("a", href=True):
        link = urljoin(url, a["href"])
        pl = urlparse(link)
        if not pl.scheme.startswith("http"):
            continue
        clean = pl._replace(fragment="").geturl()
        if pl.netloc == self.domain:
            internal_links.append(clean)
        else:
            external_links.append(clean)

    page_data: Dict[str, Any] = {
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
        "used_js_render": False,
        "response_time_ms": response_time_ms,
        "page_size_kb": page_size_kb,
        "h2_count": h2_count,
        "h3_count": h3_count,
        "images_total": images_total,
        "images_missing_alt": images_missing_alt,
        "has_og": has_og,
        "og_title": og_title,
        "og_description": og_description,
        "og_image": og_image,
        "has_twitter_card": has_twitter_card,
        "twitter_card": twitter_card,
        "mixed_content": mixed_content,
        "url_length": url_length,
        "url_depth": url_depth,
        "has_query_params": has_query_params,
        "is_https": is_https,
        "has_hreflang": has_hreflang,
        "hreflang_count": hreflang_count,
        "link_depth": self._url_depths.get(url, 0),
        "has_schema": has_schema,
        "schema_types": schema_types,
        "has_pagination": has_prev or has_next,
        "is_thin_content": is_thin_content,
        "title_too_short": title_too_short,
        "meta_desc_too_long": meta_desc_too_long,
        "meta_desc_too_short": meta_desc_too_short,
        "url_has_uppercase": url_has_uppercase,
        "url_too_long": url_too_long,
        "url_has_underscores": url_has_underscores,
        "url_has_special_chars": url_has_special_chars,
    }

    with self._lock:
        self.pages_data.append(page_data)
        self._link_graph[url] = internal_links
        current_depth = self._url_depths.get(url, 0)
        for link in internal_links:
            self._link_targets.add(link)
            if (
                link not in self.visited
                and len(self.visited) + len(self.to_visit) < self.max_pages
            ):
                self.to_visit.append(link)
                if link not in self._url_depths:
                    self._url_depths[link] = current_depth + 1
        for ext in external_links:
            self._external_links_seen.add(ext)

    return page_data


async def _run_crawl_async(self):
    logger.info("Crawl started url=%s max_pages=%d concurrency=%d", self.site_url, self.max_pages, self.concurrency)
    print(f"🐛 Crawling up to {self.max_pages} pages from {self.site_url}  "
          f"[concurrency={self.concurrency}, delay={self.min_delay}–{self.max_delay}s]")

    semaphore = asyncio.Semaphore(self.concurrency)

    async def bounded_crawl(url: str):
        async with semaphore:
            result = await self._crawl_page(client, url)
            await asyncio.sleep(self._effective_delay())
            return result

    limits = httpx.Limits(
        max_connections=self.concurrency + 4,
        max_keepalive_connections=self.concurrency,
    )
    async with httpx.AsyncClient(http2=False, limits=limits, timeout=12) as client:
        tasks: set = set()
        while self.to_visit or tasks:
            while self.to_visit and len(tasks) < self.concurrency:
                url = self.to_visit.popleft()
                if url not in self.visited and len(self.visited) < self.max_pages:
                    task = asyncio.create_task(bounded_crawl(url))
                    tasks.add(task)
            if not tasks:
                break
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    logger.info("Crawl done url=%s pages=%d", self.site_url, len(self.pages_data))
    print(f"✅ Crawled {len(self.pages_data)} pages")


def run_crawl(self):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, self._run_crawl_async())
                future.result()
        else:
            loop.run_until_complete(self._run_crawl_async())
    except RuntimeError:
        asyncio.run(self._run_crawl_async())
