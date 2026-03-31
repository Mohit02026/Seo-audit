import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

DB_PATH = "audits.db"


def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id          TEXT PRIMARY KEY,
            status          TEXT,
            url             TEXT,
            lead_name       TEXT,
            created_at      TEXT,
            updated_at      TEXT,
            result_json     TEXT,
            summary         TEXT,
            pages_data_json TEXT,
            js_enrichment_used INTEGER,
            error           TEXT
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at)")
    con.commit()
    con.close()


def db_create_job(job_id: str, url: str, lead_name: str):
    now = datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT INTO jobs (job_id, status, url, lead_name, created_at, updated_at) VALUES (?,?,?,?,?,?)",
        (job_id, "queued", url, lead_name, now, now),
    )
    con.commit()
    con.close()


def db_update_job(job_id: str, **kwargs):
    kwargs["updated_at"] = datetime.utcnow().isoformat()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    vals = list(kwargs.values()) + [job_id]
    con = sqlite3.connect(DB_PATH)
    con.execute(f"UPDATE jobs SET {sets} WHERE job_id = ?", vals)
    con.commit()
    con.close()


def db_get_job(job_id: str) -> Optional[Dict[str, Any]]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    row = con.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
    con.close()
    if not row:
        return None
    d = dict(row)
    for field in ("result_json", "pages_data_json"):
        if d.get(field):
            try:
                d[field] = json.loads(d[field])
            except Exception:
                pass
    return d


def db_purge_old_jobs(days: int = 30) -> int:
    """Delete jobs older than `days` days. Returns count deleted."""
    con = sqlite3.connect(DB_PATH)
    cur = con.execute(
        "DELETE FROM jobs WHERE created_at < datetime('now', ?)",
        (f"-{days} days",),
    )
    count = cur.rowcount
    con.commit()
    con.close()
    return count


def db_list_jobs(limit: int = 50) -> List[Dict[str, Any]]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT job_id, status, url, lead_name, created_at, updated_at, js_enrichment_used, error "
        "FROM jobs ORDER BY created_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    con.close()
    return [dict(r) for r in rows]
