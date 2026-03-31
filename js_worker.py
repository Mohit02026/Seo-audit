import json
from typing import List, Dict, Any
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


def decide_js_needed(page: Dict[str, Any]) -> bool:
    if page.get("status_code") != 200:
        return False
    # wc = page.get("word_count", 0)
    # h1 = page.get("h1_count", 0)
    # internal = page.get("internal_links_count", 0)
    # return wc < 150 or h1 == 0 or internal == 0
    return True


def js_enrich_pages(
    pages: List[Dict[str, Any]],
    domain: str,
    max_js_pages: int = 3,
) -> List[Dict[str, Any]]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            used = 0
            for page in pages:
                if used >= max_js_pages:
                    break
                if not decide_js_needed(page):
                    continue

                url = page.get("final_url") or page.get("url")
                if not url:
                    continue

                try:
                    tab = browser.new_page()
                    tab.goto(url, wait_until="domcontentloaded", timeout=15000)
                    html = tab.content()
                    tab.close()
                except Exception:
                    continue

                rsoup = BeautifulSoup(html, "html.parser")

                r_title = rsoup.find("title")
                if r_title:
                    title_text = r_title.get_text().strip()[:150]
                    page["title"] = title_text
                    page["title_length"] = len(title_text)

                r_meta_desc = rsoup.find("meta", attrs={"name": "description"})
                if r_meta_desc:
                    desc = r_meta_desc.get("content", "").strip()[:300]
                    page["meta_description"] = desc
                    page["has_meta_desc"] = bool(desc)

                r_h1_tags = rsoup.find_all("h1")
                r_h1_texts = [
                    h.get_text().strip()
                    for h in r_h1_tags
                    if h.get_text().strip()
                ]
                if r_h1_texts:
                    page["h1_count"] = len(r_h1_texts)
                    page["h1_sample"] = r_h1_texts[0][:150]

                r_canonical = rsoup.find(
                    "link", rel=lambda v: v and "canonical" in v.lower()
                )
                if r_canonical:
                    page["canonical"] = r_canonical.get("href")

                r_text = rsoup.get_text(separator=" ")
                page["word_count"] = len(r_text.split())

                internal_links: List[str] = []
                external_links: List[str] = []
                for a in rsoup.find_all("a", href=True):
                    link = urljoin(url, a["href"])
                    parsed = urlparse(link)
                    if not parsed.scheme.startswith("http"):
                        continue
                    if parsed.netloc == domain:
                        internal_links.append(link)
                    else:
                        external_links.append(link)

                page["internal_links_count"] = len(internal_links)
                page["external_links_count"] = len(external_links)
                page["used_js_render"] = True
                used += 1

        finally:
            browser.close()

    return pages


if __name__ == "__main__":
    import sys

    raw = sys.stdin.read()
    data = json.loads(raw)
    pages = data["pages_data"]
    domain = data["domain"]
    max_js = data.get("max_js_pages", 10)
    enriched = js_enrich_pages(pages, domain, max_js_pages=max_js)
    print(json.dumps({"pages_data": enriched}, ensure_ascii=False))