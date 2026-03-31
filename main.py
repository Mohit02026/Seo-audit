"""
main.py — FastAPI application. Routes only.
Heavy logic lives in: full_audit.py, db.py, reporters.py
"""
import csv
import io
import json
import sys
import subprocess
import uuid
from typing import Optional, List
from urllib.parse import urlparse

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import StreamingResponse, PlainTextResponse
from pydantic import BaseModel, field_validator

from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

from config import PAGESPEED_API_KEY
from db import init_db, db_create_job, db_update_job, db_get_job, db_list_jobs, db_purge_old_jobs
from full_audit import FullTechnicalAudit
from reporters import generate_client_report, generate_internal_report

app = FastAPI()
init_db()


# ─────────────────────────────────────────────
# Request model
# ─────────────────────────────────────────────

class AuditRequest(BaseModel):
    url: str
    lead_name: str = "Test Lead"
    max_pages: Optional[int] = 200
    js_render: bool = True

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        parsed = urlparse(v)
        if parsed.scheme not in ("http", "https"):
            raise ValueError("URL must start with http:// or https://")
        if not parsed.netloc:
            raise ValueError("Invalid URL — missing domain")
        return v


# ─────────────────────────────────────────────
# Background audit task
# ─────────────────────────────────────────────

async def run_audit_background(job_id: str, request: AuditRequest):
    db_update_job(job_id, status="running")
    try:
        audit = FullTechnicalAudit(
            request.url,
            request.max_pages or 200,
            pagespeed_key=PAGESPEED_API_KEY,
            threads=8,
            js_render=False,
            js_max_pages=10,
        )
        audit.run_full_audit()

        # Optional JS enrichment via subprocess worker
        if request.js_render:
            try:
                # Smart ordering: put low word-count pages first so the worker
                # focuses on pages most likely to be JS-rendered content.
                sorted_pages = sorted(
                    audit.pages_data,
                    key=lambda p: p.get("word_count", 9999)
                )
                js_candidate_count = sum(
                    1 for p in sorted_pages if p.get("word_count", 9999) < 200
                )
                max_js = max(3, min(8, js_candidate_count))

                payload = {
                    "pages_data": sorted_pages,
                    "domain": urlparse(request.url).netloc,
                    "max_js_pages": max_js,
                }
                proc = subprocess.Popen(
                    [sys.executable, "js_worker.py"],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                stdout, stderr = proc.communicate(json.dumps(payload), timeout=180)
                print("JS worker return code:", proc.returncode)
                if stderr:
                    print("JS worker stderr:", stderr)

                if proc.returncode == 0:
                    data = json.loads(stdout)
                    audit.pages_data = data["pages_data"]
                    audit.results = {}
                    audit.audit_robots_txt()
                    audit.analyze_crawl()
                    audit.audit_redirect_setup()
                    audit.audit_broken_external_links()
                    if PAGESPEED_API_KEY:
                        audit.run_pagespeed_sample()
                    else:
                        audit.results["pagespeed_sample"] = []
                    audit.results["audit_status"] = "OK"
                else:
                    print("JS worker error:", stderr)
            except Exception as e:
                print("JS enrichment failed:", e)

        result = audit.results
        summary = audit.generate_summary()
        js_used = any(p.get("used_js_render") for p in audit.pages_data)
        result["js_enrichment_used"] = js_used

        db_update_job(
            job_id,
            status="completed",
            result_json=json.dumps(result, ensure_ascii=False),
            summary=summary,
            pages_data_json=json.dumps(audit.pages_data, ensure_ascii=False),
            js_enrichment_used=1 if js_used else 0,
        )

    except Exception as e:
        db_update_job(job_id, status="error", error=str(e))


# ─────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────

@app.get("/test-simple")
def test_simple(url: str):
    audit = FullTechnicalAudit(url, max_pages=50, pagespeed_key=PAGESPEED_API_KEY, threads=8)
    return audit.run_full_audit()


@app.get("/summary")
def get_summary(url: str):
    audit = FullTechnicalAudit(url, max_pages=50, pagespeed_key=PAGESPEED_API_KEY, threads=8)
    result = audit.run_full_audit()
    return {"result": result, "summary": audit.generate_summary()}


@app.get("/debug-pages")
def debug_pages(url: str):
    audit = FullTechnicalAudit(url, max_pages=50, pagespeed_key=PAGESPEED_API_KEY, threads=8)
    audit.run_full_audit()
    return {"pages_data": audit.pages_data}


@app.post("/api/audit")
def start_audit(request: AuditRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    db_create_job(job_id, request.url, request.lead_name)
    background_tasks.add_task(run_audit_background, job_id, request)
    return {"job_id": job_id, "status": "queued"}


@app.get("/api/status/{job_id}")
def get_status(job_id: str):
    job = db_get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "url": job["url"],
        "lead_name": job["lead_name"],
        "created_at": job["created_at"],
        "updated_at": job["updated_at"],
        "js_enrichment_used": bool(job["js_enrichment_used"]),
        "error": job["error"],
        "result": job["result_json"],
        "summary": job["summary"],
    }


@app.get("/api/jobs")
def list_jobs(limit: int = 50):
    return db_list_jobs(limit)


@app.get("/api/export/{job_id}/csv")
def export_csv(job_id: str):
    job = db_get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    pages = job.get("pages_data_json") or []
    if not pages:
        raise HTTPException(status_code=404, detail="No pages data for this job")

    all_keys: List[str] = []
    seen: set = set()
    for p in pages:
        for k in p.keys():
            if k not in seen:
                all_keys.append(k)
                seen.add(k)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=all_keys, extrasaction="ignore")
    writer.writeheader()
    for p in pages:
        flat = {k: (", ".join(v) if isinstance(v, list) else v) for k, v in p.items()}
        writer.writerow(flat)

    output.seek(0)
    filename = f"audit_{job_id[:8]}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/api/export/{job_id}/json")
def export_json(job_id: str):
    job = db_get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    payload = {
        "job_id": job["job_id"],
        "url": job["url"],
        "lead_name": job["lead_name"],
        "created_at": job["created_at"],
        "js_enrichment_used": bool(job["js_enrichment_used"]),
        "result": job["result_json"],
        "summary": job["summary"],
        "pages_data": job["pages_data_json"],
    }
    filename = f"audit_{job_id[:8]}.json"
    return StreamingResponse(
        iter([json.dumps(payload, indent=2, ensure_ascii=False)]),
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/api/report/{job_id}/client", response_class=PlainTextResponse)
def report_client(job_id: str):
    job = db_get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"Job status is '{job['status']}', not completed")
    return PlainTextResponse(generate_client_report(job), media_type="text/markdown")


@app.get("/api/report/{job_id}/internal", response_class=PlainTextResponse)
def report_internal(job_id: str):
    job = db_get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"Job status is '{job['status']}', not completed")
    return PlainTextResponse(generate_internal_report(job), media_type="text/markdown")


@app.delete("/api/jobs/purge")
def purge_old_jobs(days: int = 30):
    count = db_purge_old_jobs(days)
    return {"deleted": count, "older_than_days": days}
