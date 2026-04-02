"""
gsc.py — Google Search Console OAuth2 flow + data pull.
"""
import json
import logging
import os
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

from db import db_save_gsc_token, db_get_gsc_token, db_delete_gsc_token, db_list_gsc_tokens

logger = logging.getLogger(__name__)

# Holds in-flight OAuth flows keyed by state so the code_verifier
# generated during authorization_url() is available at callback time.
_pending_flows: dict = {}

SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
CLIENT_CONFIG = {
    "web": {
        "client_id": os.getenv("GOOGLE_CLIENT_ID", ""),
        "client_secret": os.getenv("GOOGLE_CLIENT_SECRET", ""),
        "redirect_uris": [os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/api/gsc/callback")],
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
}
REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/api/gsc/callback")


def get_auth_url() -> str:
    flow = Flow.from_client_config(CLIENT_CONFIG, scopes=SCOPES, redirect_uri=REDIRECT_URI)
    auth_url, state = flow.authorization_url(access_type="offline", prompt="consent")
    _pending_flows[state] = flow
    return auth_url


def exchange_code(code: str, state: Optional[str] = None) -> Optional[Credentials]:
    try:
        # Reuse the original flow so the PKCE code_verifier is preserved
        if state and state in _pending_flows:
            flow = _pending_flows.pop(state)
        else:
            flow = Flow.from_client_config(CLIENT_CONFIG, scopes=SCOPES, redirect_uri=REDIRECT_URI)
        flow.fetch_token(code=code)
        return flow.credentials
    except Exception:
        logger.exception("GSC token exchange failed")
        return None


def _load_credentials(domain: str) -> Optional[Credentials]:
    token_json = db_get_gsc_token(domain)
    if not token_json:
        return None
    info = json.loads(token_json)
    creds = Credentials(
        token=info.get("token"),
        refresh_token=info.get("refresh_token"),
        token_uri=info.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=os.getenv("GOOGLE_CLIENT_ID", ""),
        client_secret=os.getenv("GOOGLE_CLIENT_SECRET", ""),
        scopes=SCOPES,
    )
    return creds


def _save_credentials(domain: str, creds: Credentials):
    info = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "scopes": list(creds.scopes or []),
    }
    db_save_gsc_token(domain, json.dumps(info))


def save_token_for_domain(domain: str, creds: Credentials):
    _save_credentials(domain, creds)
    logger.info("GSC token saved domain=%s", domain)


def get_gsc_data(site_url: str) -> List[Dict[str, Any]]:
    domain = urlparse(site_url).netloc
    creds = _load_credentials(domain)
    if not creds:
        return []
    try:
        service = build("searchconsole", "v1", credentials=creds)
        # Use sc-domain: prefix if no protocol variant found
        sc_property = site_url if site_url.startswith("http") else f"sc-domain:{domain}"
        body = {
            "startDate": _days_ago(90),
            "endDate": _days_ago(1),
            "dimensions": ["page"],
            "rowLimit": 50,
            "orderBy": [{"fieldName": "clicks", "sortOrder": "DESCENDING"}],
        }
        resp = service.searchanalytics().query(siteUrl=sc_property, body=body).execute()
        rows = resp.get("rows", [])
        result = []
        for row in rows:
            result.append({
                "page": row["keys"][0],
                "clicks": row.get("clicks", 0),
                "impressions": row.get("impressions", 0),
                "ctr": round(row.get("ctr", 0) * 100, 2),
                "position": round(row.get("position", 0), 1),
            })
        # Refresh token if it was refreshed during request
        if creds.token:
            _save_credentials(domain, creds)
        return result
    except Exception:
        logger.exception("GSC data pull failed domain=%s", domain)
        return []


def list_connected_domains() -> List[Dict[str, Any]]:
    return db_list_gsc_tokens()


def revoke_domain(domain: str):
    db_delete_gsc_token(domain)
    logger.info("GSC token revoked domain=%s", domain)


def _days_ago(n: int) -> str:
    from datetime import datetime, timedelta
    return (datetime.utcnow() - timedelta(days=n)).strftime("%Y-%m-%d")
