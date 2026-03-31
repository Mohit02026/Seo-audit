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

    con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id       TEXT PRIMARY KEY,
            email         TEXT UNIQUE,
            password_hash TEXT,
            role          TEXT,
            lead_name     TEXT,
            created_at    TEXT
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS gsc_tokens (
            domain     TEXT PRIMARY KEY,
            token_json TEXT,
            updated_at TEXT
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS schedules (
            schedule_id TEXT PRIMARY KEY,
            url         TEXT,
            lead_name   TEXT,
            frequency   TEXT,
            max_pages   INTEGER,
            js_render   INTEGER,
            last_run_at TEXT,
            next_run_at TEXT,
            created_at  TEXT,
            active      INTEGER DEFAULT 1
        )
    """)

    con.commit()
    con.close()


# ── Jobs ────────────────────────────────────────────────────────────────────

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
    con = sqlite3.connect(DB_PATH)
    cur = con.execute(
        "DELETE FROM jobs WHERE created_at < datetime('now', ?)",
        (f"-{days} days",),
    )
    count = cur.rowcount
    con.commit()
    con.close()
    return count


def db_list_jobs(limit: int = 50, lead_name: Optional[str] = None) -> List[Dict[str, Any]]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    if lead_name:
        rows = con.execute(
            "SELECT job_id, status, url, lead_name, created_at, updated_at, js_enrichment_used, error "
            "FROM jobs WHERE lead_name = ? ORDER BY created_at DESC LIMIT ?",
            (lead_name, limit),
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT job_id, status, url, lead_name, created_at, updated_at, js_enrichment_used, error "
            "FROM jobs ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    con.close()
    return [dict(r) for r in rows]


# ── Users ────────────────────────────────────────────────────────────────────

def db_create_user(user_id: str, email: str, password_hash: str, role: str, lead_name: str = ""):
    now = datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT INTO users (user_id, email, password_hash, role, lead_name, created_at) VALUES (?,?,?,?,?,?)",
        (user_id, email, password_hash, role, lead_name, now),
    )
    con.commit()
    con.close()


def db_get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    row = con.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    con.close()
    return dict(row) if row else None


def db_get_user_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    row = con.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
    con.close()
    return dict(row) if row else None


def db_list_users() -> List[Dict[str, Any]]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT user_id, email, role, lead_name, created_at FROM users ORDER BY created_at DESC").fetchall()
    con.close()
    return [dict(r) for r in rows]


# ── GSC Tokens ───────────────────────────────────────────────────────────────

def db_save_gsc_token(domain: str, token_json: str):
    now = datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT INTO gsc_tokens (domain, token_json, updated_at) VALUES (?,?,?) "
        "ON CONFLICT(domain) DO UPDATE SET token_json=excluded.token_json, updated_at=excluded.updated_at",
        (domain, token_json, now),
    )
    con.commit()
    con.close()


def db_get_gsc_token(domain: str) -> Optional[str]:
    con = sqlite3.connect(DB_PATH)
    row = con.execute("SELECT token_json FROM gsc_tokens WHERE domain = ?", (domain,)).fetchone()
    con.close()
    return row[0] if row else None


def db_delete_gsc_token(domain: str):
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM gsc_tokens WHERE domain = ?", (domain,))
    con.commit()
    con.close()


def db_list_gsc_tokens() -> List[Dict[str, Any]]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT domain, updated_at FROM gsc_tokens ORDER BY updated_at DESC").fetchall()
    con.close()
    return [dict(r) for r in rows]


# ── Schedules ────────────────────────────────────────────────────────────────

def db_create_schedule(schedule_id: str, url: str, lead_name: str, frequency: str,
                        max_pages: int, js_render: bool, next_run_at: str):
    now = datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT INTO schedules (schedule_id, url, lead_name, frequency, max_pages, js_render, "
        "next_run_at, created_at, active) VALUES (?,?,?,?,?,?,?,?,1)",
        (schedule_id, url, lead_name, frequency, max_pages, 1 if js_render else 0, next_run_at, now),
    )
    con.commit()
    con.close()


def db_list_schedules() -> List[Dict[str, Any]]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM schedules ORDER BY created_at DESC").fetchall()
    con.close()
    return [dict(r) for r in rows]


def db_get_schedule(schedule_id: str) -> Optional[Dict[str, Any]]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    row = con.execute("SELECT * FROM schedules WHERE schedule_id = ?", (schedule_id,)).fetchone()
    con.close()
    return dict(row) if row else None


def db_update_schedule(schedule_id: str, **kwargs):
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    vals = list(kwargs.values()) + [schedule_id]
    con = sqlite3.connect(DB_PATH)
    con.execute(f"UPDATE schedules SET {sets} WHERE schedule_id = ?", vals)
    con.commit()
    con.close()


def db_delete_schedule(schedule_id: str):
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM schedules WHERE schedule_id = ?", (schedule_id,))
    con.commit()
    con.close()


def db_get_due_schedules() -> List[Dict[str, Any]]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT * FROM schedules WHERE active = 1 AND next_run_at <= ?",
        (datetime.utcnow().isoformat(),),
    ).fetchall()
    con.close()
    return [dict(r) for r in rows]
