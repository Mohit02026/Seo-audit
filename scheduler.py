"""
scheduler.py — APScheduler-based recurring audit runner.
"""
import logging
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from db import db_get_due_schedules, db_update_schedule

logger = logging.getLogger(__name__)
_scheduler = AsyncIOScheduler()


def _next_run(frequency: str) -> str:
    if frequency == "weekly":
        return (datetime.utcnow() + timedelta(weeks=1)).isoformat()
    return (datetime.utcnow() + timedelta(days=30)).isoformat()


async def _run_due_schedules():
    """Called every hour by APScheduler — fires audits for due schedules."""
    from main import run_audit_background  # local import avoids circular dep

    due = db_get_due_schedules()
    for schedule in due:
        job_id = str(uuid.uuid4())
        logger.info("Scheduled audit firing schedule_id=%s url=%s", schedule["schedule_id"], schedule["url"])

        # Import here to avoid circular at module load
        from db import db_create_job
        from pydantic import BaseModel

        class _Req:
            url = schedule["url"]
            lead_name = schedule["lead_name"]
            max_pages = schedule["max_pages"]
            js_render = bool(schedule["js_render"])

        db_create_job(job_id, schedule["url"], schedule["lead_name"])
        import asyncio
        asyncio.create_task(run_audit_background(job_id, _Req()))

        db_update_schedule(
            schedule["schedule_id"],
            last_run_at=datetime.utcnow().isoformat(),
            next_run_at=_next_run(schedule["frequency"]),
        )


def start_scheduler():
    _scheduler.add_job(_run_due_schedules, "interval", hours=1, id="schedule_runner")
    _scheduler.start()
    logger.info("APScheduler started")


def stop_scheduler():
    _scheduler.shutdown(wait=False)
