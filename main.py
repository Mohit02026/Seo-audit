from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
import uvicorn
from datetime import datetime
from full_audit import FullTechnicalAudit

app = FastAPI(title="🔥 SEO Audit API - Local Test")

PAGESPEED_API_KEY = "AIzaSyCJGzOGMdzwyg7BAxLpL6mJa8OrRk4jE2I"  # your key

# simple in-memory job storage
jobs = {}


class AuditRequest(BaseModel):
    url: str
    lead_name: str = "Test Lead"
    max_pages: Optional[int] = 200  # higher for real audits


async def run_audit_background(job_id: str, request: AuditRequest):
    jobs[job_id]["status"] = "running"
    try:
        audit = FullTechnicalAudit(
            request.url,
            request.max_pages,
            pagespeed_key=PAGESPEED_API_KEY,
        )
        result = audit.run_full_audit()
        jobs[job_id]["result"] = result
        jobs[job_id]["status"] = "done"
    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)


@app.post("/api/audit")
async def queue_audit(request: AuditRequest, background_tasks: BackgroundTasks):
    import uuid

    job_id = str(uuid.uuid4())[:8]

    jobs[job_id] = {
        "status": "queued",
        "lead": request.lead_name,
        "url": request.url,
        "created_at": datetime.now().isoformat(),
    }

    background_tasks.add_task(run_audit_background, job_id, request)

    return {
        "job_id": job_id,
        "status": "queued",
        "poll": f"http://localhost:8000/api/status/{job_id}",
    }


@app.get("/api/status/{job_id}")
async def get_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return job


@app.get("/test-simple/{url:path}")
async def test_simple(url: str):
    """
    Quick test endpoint – 1 URL directly.
    """
    audit = FullTechnicalAudit(
        url,
        max_pages=200,  # good depth for SMB sites
        pagespeed_key=PAGESPEED_API_KEY,
    )
    results = audit.run_full_audit()
    summary = results.get("crawl_summary", {})
    robots_info = results.get("robots_txt", {})
    robots_ok = robots_info.get("allows_homepage", False)
    audit_status = results.get("audit_status", "UNKNOWN")

    robots_notes = (
        "Crawl blocked by robots.txt (homepage disallowed)"
        if audit_status == "CRAWL_BLOCKED_BY_ROBOTS"
        else "Crawl allowed by robots.txt"
    )

    return {
        "url": url,
        "audit_status": audit_status,
        "robots_ok": robots_ok,
        "robots_notes": robots_notes,
        "pages_crawled": summary.get("total_pages", 0),
        "ok_pages": summary.get("status_2xx", 0),
        "errors": summary.get("status_4xx", 0) + summary.get("status_5xx", 0),
        "missing_titles": summary.get("missing_titles", 0),
        "long_titles": summary.get("long_titles", 0),
        "no_meta_desc": summary.get("no_meta_desc", 0),
        "no_h1": summary.get("no_h1", 0),
        "multi_h1": summary.get("multi_h1", 0),
        "noindex_pages": summary.get("noindex_pages", 0),
        "duplicate_titles_sample": results.get("duplicate_titles", {}),
        "broken_internal_links_sample": results.get("broken_internal_links", [])[:5],
        "pagespeed_sample": results.get("pagespeed_sample", []),
    }


@app.get("/")
async def home():
    return {
        "message": "SEO Audit API Ready!",
        "try": "GET /test-simple/https://example.com",
    }

@app.get("/summary/{url:path}")
async def get_summary(url: str):
    """
    Get a text summary of the audit for a URL.
    """
    audit = FullTechnicalAudit(
        url,
        max_pages=200,
        pagespeed_key=PAGESPEED_API_KEY,
    )
    results = audit.run_full_audit()
    summary_text = audit.generate_summary()
    
    return {
        "url": url,
        "summary": summary_text,
        "content_type": "text/plain",
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
