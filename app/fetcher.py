# app/fetcher.py
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any

import httpx

from .settings import settings
from .status import status


def _theta_url() -> str:
    return settings.THETA_BASE_URL.rstrip("/") + "/" + settings.THETA_PROBE_PATH.lstrip("/")


def _theta_tz() -> ZoneInfo:
    # Server-URL-Beispiel nutzt "YYYY-MM-DD HH:MM:SS" ohne TZ.
    # Wir formatieren daher in der konfigurierten TZ (Default Berlin).
    try:
        return ZoneInfo(settings.THETA_TIMEZONE)
    except Exception:
        return ZoneInfo("Europe/Berlin")


def _format_theta_dt(dt_utc: datetime) -> str:
    tz = _theta_tz()
    return dt_utc.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")


def _build_auth() -> tuple[httpx.Auth | None, dict[str, str]]:
    """
    httpx:
    - Basic Auth via httpx.BasicAuth (sauber)
    - Bearer via Header
    """
    headers: dict[str, str] = {"Accept": "application/json"}
    auth_type = (settings.AUTH_TYPE or "none").lower().strip()

    if auth_type == "basic":
        if not settings.AUTH_USERNAME or not settings.AUTH_PASSWORD:
            # besser sofort failen als 401 spammen
            raise RuntimeError("AUTH_TYPE=basic aber AUTH_USERNAME/AUTH_PASSWORD fehlen in .env")
        return httpx.BasicAuth(settings.AUTH_USERNAME, settings.AUTH_PASSWORD), headers

    if auth_type == "bearer":
        if not settings.AUTH_BEARER_TOKEN:
            raise RuntimeError("AUTH_TYPE=bearer aber AUTH_BEARER_TOKEN fehlt in .env")
        headers["Authorization"] = f"Bearer {settings.AUTH_BEARER_TOKEN}"
        return None, headers

    return None, headers


async def fetch_theta(start_utc: datetime, end_utc: datetime) -> list[dict[str, Any]]:
    """
    Holt Rohdaten aus Theta für [start_utc, end_utc].
    Erwartet JSON response (Liste oder dict mit list unter key).
    """
    url = _theta_url()
    params = {
        "start_date": _format_theta_dt(start_utc),
        "end_date": _format_theta_dt(end_utc),
    }

    auth, headers = _build_auth()

    timeout = httpx.Timeout(settings.HTTP_TIMEOUT_SEC)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        r = await client.get(url, params=params, headers=headers, auth=auth)

    if r.status_code == 401:
        raise RuntimeError("HTTP 401 from theta: Authorization header missing or invalid.")
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} from theta: {r.text[:500]}")

    data = r.json()

    # flexibel: manche APIs liefern {"items":[...]} oder direkt [...]
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("items", "data", "results"):
            v = data.get(k)
            if isinstance(v, list):
                return v
    # fallback: nichts verwertbares
    return []


async def fetch_loop(queue: asyncio.Queue):
    """
    Endlosschleife:
    - status.fetch_enabled steuert
    - cursor läuft UTC (datetime)
    - fragt nie in die Zukunft
    """
    # Cursor initial: wenn noch nie gesetzt, "jetzt - window" als Start
    cursor = datetime.now(timezone.utc) - timedelta(seconds=settings.DEFAULT_WINDOW_SEC)

    # status defaults
    status.fetch_enabled = getattr(status, "fetch_enabled", False)
    status.fetch_window_sec = getattr(status, "fetch_window_sec", settings.DEFAULT_WINDOW_SEC)
    status.fetch_poll_sec = getattr(status, "fetch_poll_sec", settings.DEFAULT_POLL_SEC)
    status.fetch_cursor_utc = getattr(status, "fetch_cursor_utc", cursor.isoformat())

    while True:
        try:
            if not getattr(status, "fetch_enabled", False):
                await asyncio.sleep(0.2)
                continue

            window_sec = int(getattr(status, "fetch_window_sec", settings.DEFAULT_WINDOW_SEC) or settings.DEFAULT_WINDOW_SEC)
            poll_sec = int(getattr(status, "fetch_poll_sec", settings.DEFAULT_POLL_SEC) or settings.DEFAULT_POLL_SEC)

            now_utc = datetime.now(timezone.utc)
            end_utc = min(cursor + timedelta(seconds=window_sec), now_utc)

            # wenn wir "caught up" sind: warten
            if end_utc <= cursor:
                await asyncio.sleep(poll_sec)
                continue

            # sichtbarer Fortschritt im UI
            status.fetch_cursor_utc = cursor.isoformat()

            items = await fetch_theta(cursor, end_utc)

            # queue füllen
            for it in items:
                await queue.put(it)

            status.last_fetch_at = datetime.now(timezone.utc).timestamp()
            status.fetched_total += len(items)

            # Cursor vorziehen
            cursor = end_utc
            status.fetch_cursor_utc = cursor.isoformat()

            # wenn wir am "now" sind -> poll
            if cursor >= now_utc:
                await asyncio.sleep(poll_sec)

        except Exception as e:
            status.fetch_errors += 1
            status.error_logs.append(f"FETCH ERROR: {repr(e)}")
            # backoff kurz
            await asyncio.sleep(2.0)
