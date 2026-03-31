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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List
from urllib.parse import urlparse

from fastapi import FastAPI, BackgroundTasks, HTTPException, Depends
from fastapi.responses import StreamingResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, field_validator

from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

from config import PAGESPEED_API_KEY
from db import (
    init_db, db_create_job, db_update_job, db_get_job, db_list_jobs,
    db_purge_old_jobs, db_get_user_by_email, db_create_user,
    db_list_users, db_get_user_by_id,
    db_list_gsc_tokens, db_delete_gsc_token,
    db_create_schedule, db_list_schedules, db_get_schedule,
    db_update_schedule, db_delete_schedule,
)
from full_audit import FullTechnicalAudit
from reporters import generate_client_report, generate_internal_report
from auth import (
    hash_password, verify_password, create_access_token,
    get_current_user, require_admin, ensure_admin_exists,
)
from diff import compute_diff
from pdf_export import generate_pdf
import gsc as _gsc
from scheduler import start_scheduler, stop_scheduler

app = FastAPI(title="SEO Crawler API")
init_db()
ensure_admin_exists()


@app.on_event("startup")
async def startup():
    start_scheduler()


@app.on_event("shutdown")
async def shutdown():
    stop_scheduler()


# Serve React frontend (built to static/)
_static = Path(__file__).parent / "static"
if _static.exists():
    app.mount("/app", StaticFiles(directory=str(_static), html=True), name="frontend")


# ─────────────────────────────────────────────
# Request models
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


class LoginRequest(BaseModel):
    email: str
    password: str


class CreateUserRequest(BaseModel):
    email: str
    password: str
    role: str = "client"
    lead_name: str = ""


class ScheduleRequest(BaseModel):
    url: str
    lead_name: str = ""
    frequency: str = "weekly"
    max_pages: int = 200
    js_render: bool = True

    @field_validator("frequency")
    @classmethod
    def validate_frequency(cls, v: str) -> str:
        if v not in ("weekly", "monthly"):
            raise ValueError("frequency must be 'weekly' or 'monthly'")
        return v


# ─────────────────────────────────────────────
# Auth endpoints
# ─────────────────────────────────────────────

@app.post("/auth/login")
def login(req: LoginRequest):
    user = db_get_user_by_email(req.email)
    if not user or not verify_password(req.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    token = create_access_token(user["user_id"], user["role"], user.get("lead_name", ""))
    return {"token": token, "role": user["role"], "email": user["email"]}


@app.get("/auth/me")
def me(user: dict = Depends(get_current_user)):
    return {"user_id": user["user_id"], "email": user["email"],
            "role": user["role"], "lead_name": user.get("lead_name", "")}


@app.get("/auth/users")
def list_users(user: dict = Depends(require_admin)):
    return db_list_users()


@app.post("/auth/users")
def create_user(req: CreateUserRequest, user: dict = Depends(require_admin)):
    existing = db_get_user_by_email(req.email)
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    db_create_user(
        user_id=str(uuid.uuid4()),
        email=req.email,
        password_hash=hash_password(req.password),
        role=req.role,
        lead_name=req.lead_name,
    )
    return {"status": "created"}


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

        if request.js_render:
            try:
                sorted_pages = sorted(audit.pages_data, key=lambda p: p.get("word_count", 9999))
                js_candidate_count = sum(1 for p in sorted_pages if p.get("word_count", 9999) < 200)
                max_js = max(3, min(8, js_candidate_count))
                payload = {
                    "pages_data": sorted_pages,
                    "domain": urlparse(request.url).netloc,
                    "max_js_pages": max_js,
                }
                proc = subprocess.Popen(
                    [sys.executable, "js_worker.py"],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE, text=True,
                )
                stdout, stderr = proc.communicate(json.dumps(payload), timeout=180)
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
                    audit.compute_pagerank()
                    if PAGESPEED_API_KEY:
                        audit.run_pagespeed_sample()
                    else:
                        audit.results["pagespeed_sample"] = []
                    audit.results["audit_status"] = "OK"
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
# Audit routes
# ─────────────────────────────────────────────

@app.post("/api/audit")
def start_audit(request: AuditRequest, background_tasks: BackgroundTasks,
                user: dict = Depends(get_current_user)):
    job_id = str(uuid.uuid4())
    db_create_job(job_id, request.url, request.lead_name)
    background_tasks.add_task(run_audit_background, job_id, request)
    return {"job_id": job_id, "status": "queued"}


@app.get("/api/status/{job_id}")
def get_status(job_id: str, user: dict = Depends(get_current_user)):
    job = db_get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if user["role"] == "client" and job.get("lead_name") != user.get("lead_name"):
        raise HTTPException(status_code=403, detail="Access denied")
    return {
        "job_id": job["job_id"], "status": job["status"], "url": job["url"],
        "lead_name": job["lead_name"], "created_at": job["created_at"],
        "updated_at": job["updated_at"],
        "js_enrichment_used": bool(job["js_enrichment_used"]),
        "error": job["error"], "result": job["result_json"], "summary": job["summary"],
    }


@app.get("/api/jobs")
def list_jobs(limit: int = 50, user: dict = Depends(get_current_user)):
    lead_name = user.get("lead_name") if user["role"] == "client" else None
    return db_list_jobs(limit, lead_name=lead_name)


@app.delete("/api/jobs/purge")
def purge_old_jobs(days: int = 30, user: dict = Depends(require_admin)):
    count = db_purge_old_jobs(days)
    return {"deleted": count, "older_than_days": days}


# ─────────────────────────────────────────────
# Diff
# ─────────────────────────────────────────────

@app.get("/api/diff/{job_id_a}/{job_id_b}")
def diff_audits(job_id_a: str, job_id_b: str, user: dict = Depends(get_current_user)):
    return compute_diff(job_id_a, job_id_b)


# ─────────────────────────────────────────────
# Exports
# ─────────────────────────────────────────────

@app.get("/api/export/{job_id}/csv")
def export_csv(job_id: str, user: dict = Depends(get_current_user)):
    job = db_get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    pages = job.get("pages_data_json") or []
    if not pages:
        raise HTTPException(status_code=404, detail="No pages data")
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
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": f"attachment; filename={filename}"})


@app.get("/api/export/{job_id}/json")
def export_json(job_id: str, user: dict = Depends(get_current_user)):
    job = db_get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    payload = {
        "job_id": job["job_id"], "url": job["url"], "lead_name": job["lead_name"],
        "created_at": job["created_at"], "js_enrichment_used": bool(job["js_enrichment_used"]),
        "result": job["result_json"], "summary": job["summary"],
        "pages_data": job["pages_data_json"],
    }
    filename = f"audit_{job_id[:8]}.json"
    return StreamingResponse(iter([json.dumps(payload, indent=2, ensure_ascii=False)]),
                             media_type="application/json",
                             headers={"Content-Disposition": f"attachment; filename={filename}"})


@app.get("/api/export/{job_id}/pdf")
def export_pdf(job_id: str, user: dict = Depends(get_current_user)):
    job = db_get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail="Job not completed")
    report_md = generate_client_report(job)
    title = f"SEO Audit — {job.get('lead_name', job.get('url', ''))}"
    pdf_bytes = generate_pdf(report_md, title=title)
    filename = f"audit_{job_id[:8]}.pdf"
    return StreamingResponse(iter([pdf_bytes]), media_type="application/pdf",
                             headers={"Content-Disposition": f"attachment; filename={filename}"})


# ─────────────────────────────────────────────
# Reports (Markdown)
# ─────────────────────────────────────────────

@app.get("/api/report/{job_id}/client", response_class=PlainTextResponse)
def report_client(job_id: str, user: dict = Depends(get_current_user)):
    job = db_get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"Job status is '{job['status']}'")
    return PlainTextResponse(generate_client_report(job), media_type="text/markdown")


@app.get("/api/report/{job_id}/internal", response_class=PlainTextResponse)
def report_internal(job_id: str, user: dict = Depends(require_admin)):
    job = db_get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"Job status is '{job['status']}'")
    return PlainTextResponse(generate_internal_report(job), media_type="text/markdown")


# ─────────────────────────────────────────────
# GSC OAuth
# ─────────────────────────────────────────────

@app.get("/api/gsc/auth")
def gsc_auth(user: dict = Depends(require_admin)):
    return RedirectResponse(_gsc.get_auth_url())


@app.get("/api/gsc/callback")
def gsc_callback(code: str, state: Optional[str] = None):
    creds = _gsc.exchange_code(code)
    if not creds:
        raise HTTPException(status_code=400, detail="OAuth exchange failed")
    # Store under the domain from token_uri (we'll ask user to specify domain in UI)
    # For now store under a placeholder — UI will confirm the domain
    from urllib.parse import urlparse as _up
    domain = "pending"
    _gsc.save_token_for_domain(domain, creds)
    return RedirectResponse("/app/settings/gsc?connected=1")


@app.post("/api/gsc/save/{domain}")
def gsc_save_domain(domain: str, user: dict = Depends(require_admin)):
    """Rename a pending GSC token to its actual domain."""
    from db import db_get_gsc_token, db_save_gsc_token, db_delete_gsc_token
    token = db_get_gsc_token("pending")
    if not token:
        raise HTTPException(status_code=404, detail="No pending token found")
    db_save_gsc_token(domain, token)
    db_delete_gsc_token("pending")
    return {"status": "saved", "domain": domain}


@app.get("/api/gsc/status")
def gsc_status(user: dict = Depends(require_admin)):
    return _gsc.list_connected_domains()


@app.get("/api/gsc/data/{job_id}")
def gsc_data(job_id: str, user: dict = Depends(get_current_user)):
    job = db_get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return _gsc.get_gsc_data(job["url"])


@app.delete("/api/gsc/{domain}")
def gsc_revoke(domain: str, user: dict = Depends(require_admin)):
    _gsc.revoke_domain(domain)
    return {"status": "revoked", "domain": domain}


# ─────────────────────────────────────────────
# Schedules
# ─────────────────────────────────────────────

@app.post("/api/schedules")
def create_schedule(req: ScheduleRequest, user: dict = Depends(require_admin)):
    schedule_id = str(uuid.uuid4())
    next_run = (datetime.utcnow() + timedelta(hours=1)).isoformat()
    db_create_schedule(
        schedule_id=schedule_id,
        url=req.url,
        lead_name=req.lead_name,
        frequency=req.frequency,
        max_pages=req.max_pages,
        js_render=req.js_render,
        next_run_at=next_run,
    )
    return {"schedule_id": schedule_id, "status": "created", "next_run_at": next_run}


@app.get("/api/schedules")
def list_schedules(user: dict = Depends(require_admin)):
    return db_list_schedules()


@app.patch("/api/schedules/{schedule_id}")
def update_schedule(schedule_id: str, body: dict, user: dict = Depends(require_admin)):
    allowed = {"active", "frequency", "max_pages", "js_render"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail="No valid fields to update")
    db_update_schedule(schedule_id, **updates)
    return {"status": "updated"}


@app.delete("/api/schedules/{schedule_id}")
def delete_schedule(schedule_id: str, user: dict = Depends(require_admin)):
    db_delete_schedule(schedule_id)
    return {"status": "deleted"}


# ─────────────────────────────────────────────
# Debug / legacy routes (no auth — internal use)
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
