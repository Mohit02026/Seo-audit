"""
diff.py — Compare two completed audit jobs and surface changes.
"""
from typing import Any, Dict, List

from db import db_get_job


def compute_diff(job_id_a: str, job_id_b: str) -> Dict[str, Any]:
    job_a = db_get_job(job_id_a)
    job_b = db_get_job(job_id_b)

    if not job_a or not job_b:
        return {"error": "One or both job IDs not found"}
    if job_a["status"] != "completed" or job_b["status"] != "completed":
        return {"error": "Both jobs must be completed"}

    result_a = job_a.get("result_json") or {}
    result_b = job_b.get("result_json") or {}
    pages_a = {p["url"]: p for p in (job_a.get("pages_data_json") or [])}
    pages_b = {p["url"]: p for p in (job_b.get("pages_data_json") or [])}

    # Summary delta
    summary_a = result_a.get("crawl_summary", {})
    summary_b = result_b.get("crawl_summary", {})
    summary_delta = {
        k: summary_b.get(k, 0) - summary_a.get(k, 0)
        for k in set(list(summary_a.keys()) + list(summary_b.keys()))
        if isinstance(summary_a.get(k, 0), (int, float))
    }

    # New / removed pages
    urls_a = set(pages_a.keys())
    urls_b = set(pages_b.keys())
    new_pages = sorted(urls_b - urls_a)
    removed_pages = sorted(urls_a - urls_b)

    # Status changes
    status_changes: List[Dict] = []
    for url in urls_a & urls_b:
        sc_a = pages_a[url].get("status_code")
        sc_b = pages_b[url].get("status_code")
        if sc_a != sc_b:
            status_changes.append({"url": url, "before": sc_a, "after": sc_b})

    # Title changes
    title_changes: List[Dict] = []
    for url in urls_a & urls_b:
        t_a = pages_a[url].get("title", "")
        t_b = pages_b[url].get("title", "")
        if t_a != t_b:
            title_changes.append({"url": url, "before": t_a, "after": t_b})

    # PageSpeed score delta
    ps_a = {r["url"]: r.get("performance_score") for r in result_a.get("pagespeed_sample", []) if "url" in r}
    ps_b = {r["url"]: r.get("performance_score") for r in result_b.get("pagespeed_sample", []) if "url" in r}
    score_delta: List[Dict] = []
    for url in set(list(ps_a.keys()) + list(ps_b.keys())):
        s_a = ps_a.get(url)
        s_b = ps_b.get(url)
        if s_a is not None and s_b is not None and s_a != s_b:
            score_delta.append({"url": url, "before": s_a, "after": s_b, "delta": s_b - s_a})

    score_delta.sort(key=lambda x: abs(x["delta"]), reverse=True)

    return {
        "job_a": {"job_id": job_id_a, "url": job_a["url"], "created_at": job_a["created_at"]},
        "job_b": {"job_id": job_id_b, "url": job_b["url"], "created_at": job_b["created_at"]},
        "summary_delta": summary_delta,
        "new_pages": new_pages,
        "removed_pages": removed_pages,
        "status_changes": status_changes,
        "title_changes": title_changes,
        "score_delta": score_delta,
    }
