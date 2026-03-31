"""
pagespeed.py — Google PageSpeed Insights API (parallel, mobile only).
Functions take the FullTechnicalAudit instance as first arg.
"""
import asyncio
import logging
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


async def _fetch_pagespeed_async(self, client: httpx.AsyncClient, url: str) -> Optional[Dict[str, Any]]:
    if not self.pagespeed_key:
        return None
    api = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
    params = {"url": url, "strategy": "mobile", "key": self.pagespeed_key}
    try:
        r = await client.get(api, params=params, timeout=60)
        if r.status_code != 200:
            return {"url": url, "error": f"status {r.status_code}"}
        data = r.json()
        loading = data.get("loadingExperience", {}).get("metrics", {})
        lcp = loading.get("LARGEST_CONTENTFUL_PAINT_MS", {}).get("percentile")
        inp = loading.get("INTERACTION_TO_NEXT_PAINT_MS", {}).get("percentile")
        cls_raw = loading.get("CUMULATIVE_LAYOUT_SHIFT_SCORE", {}).get("percentile")
        cls = cls_raw / 100 if cls_raw is not None else None
        category = loading.get("EXPERIMENTAL_CWV_OVERALL", {}).get("category", "Unknown")
        lighthouse = data.get("lighthouseResult", {})
        audits = lighthouse.get("audits", {})
        categories = lighthouse.get("categories", {})

        # Lab performance score (0–100)
        perf_score_raw = categories.get("performance", {}).get("score")
        performance_score = round(perf_score_raw * 100) if perf_score_raw is not None else None

        # Lab metrics (fallback when CrUX field data is unavailable)
        if lcp is None:
            lcp = audits.get("largest-contentful-paint", {}).get("numericValue")
        if inp is None:
            inp = audits.get("interaction-to-next-paint", {}).get("numericValue")
        if cls is None:
            cls = audits.get("cumulative-layout-shift", {}).get("numericValue")

        # Additional lab metrics
        fcp_ms = audits.get("first-contentful-paint", {}).get("numericValue")
        tbt_ms = audits.get("total-blocking-time", {}).get("numericValue")
        speed_index_ms = audits.get("speed-index", {}).get("numericValue")

        return {
            "url": url,
            "performance_score": performance_score,
            "lcp_ms": lcp,
            "inp_ms": inp,
            "cls": cls,
            "fcp_ms": fcp_ms,
            "tbt_ms": tbt_ms,
            "speed_index_ms": speed_index_ms,
            "cwv_category": category,
        }
    except httpx.TimeoutException:
        return {"url": url, "error": "pagespeed_timeout"}
    except httpx.ConnectError:
        return {"url": url, "error": "pagespeed_connect_error"}
    except Exception as e:
        logger.warning("PageSpeed unexpected error url=%s", url, exc_info=True)
        return {"url": url, "error": str(e)}


async def _run_pagespeed_async(self):
    if not self.pagespeed_key or not self.pages_data:
        self.results["pagespeed_sample"] = []
        return
    urls: List[str] = [self.site_url]
    for page in self.pages_data:
        if page.get("url") != self.site_url and len(urls) < 3:
            urls.append(page["url"])
    async with httpx.AsyncClient(http2=False, timeout=65) as client:
        results = await asyncio.gather(*[self._fetch_pagespeed_async(client, u) for u in urls])
    self.results["pagespeed_sample"] = [r for r in results if r is not None]


def run_pagespeed_sample(self):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(asyncio.run, self._run_pagespeed_async()).result()
        else:
            loop.run_until_complete(self._run_pagespeed_async())
    except RuntimeError:
        asyncio.run(self._run_pagespeed_async())
