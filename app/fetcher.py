# app/fetcher.py
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any
from urllib.parse import quote

import httpx

from .settings import settings
from .status import status


def _theta_url() -> str:
    return settings.THETA_BASE_URL.rstrip("/") + "/" + settings.THETA_PROBE_PATH.lstrip("/")


def _theta_tz() -> ZoneInfo:
    try:
        return ZoneInfo(settings.THETA_TIMEZONE)
    except Exception:
        return ZoneInfo("Europe/Berlin")


def _format_theta_dt(dt_utc: datetime) -> str:
    """
    Theta erwartet einen String ohne TZ:
    "YYYY-MM-DD HH:MM:SS"
    Wir formatieren in settings.THETA_TIMEZONE.
    """
    tz = _theta_tz()
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return dt_utc.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")


def _build_auth() -> tuple[httpx.Auth | None, dict[str, str]]:
    headers: dict[str, str] = {"Accept": "application/json"}
    auth_type = (settings.AUTH_TYPE or "none").lower().strip()

    if auth_type == "basic":
        if not settings.AUTH_USERNAME or not settings.AUTH_PASSWORD:
            raise RuntimeError("AUTH_TYPE=basic aber AUTH_USERNAME/AUTH_PASSWORD fehlen in .env")
        return httpx.BasicAuth(settings.AUTH_USERNAME, settings.AUTH_PASSWORD), headers

    if auth_type == "bearer":
        if not settings.AUTH_BEARER_TOKEN:
            raise RuntimeError("AUTH_TYPE=bearer aber AUTH_BEARER_TOKEN fehlt in .env")
        headers["Authorization"] = f"Bearer {settings.AUTH_BEARER_TOKEN}"
        return None, headers

    return None, headers


def _parse_cursor_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _build_theta_request_url(start_utc: datetime, end_utc: datetime) -> str:
    """
    CRITICAL FIX:
    Wir bauen den Querystring selbst und encoden mit quote(),
    damit Leerzeichen IMMER als %20 rausgehen (nicht als '+').
    """
    base = _theta_url()
    start_s = _format_theta_dt(start_utc)
    end_s = _format_theta_dt(end_utc)

    # quote encodiert Space als %20
    # safe=":-_" lässt Doppelpunkte/Minus/Underscore stehen (lesbar)
    start_q = quote(start_s, safe=":-_")
    end_q = quote(end_s, safe=":-_")

    return f"{base}?start_date={start_q}&end_date={end_q}"


async def fetch_theta(start_utc: datetime, end_utc: datetime) -> list[dict[str, Any]]:
    url = _build_theta_request_url(start_utc, end_utc)

    auth, headers = _build_auth()
    timeout = httpx.Timeout(settings.HTTP_TIMEOUT_SEC)

    # Debug: was fragen wir wirklich ab?
    status.error_logs.appendleft(f"FETCH REQ: {url}")

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        r = await client.get(url, headers=headers, auth=auth)

    if r.status_code == 401:
        raise RuntimeError("HTTP 401 from theta: Authorization header missing or invalid.")
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} from theta: {r.text[:500]}")

    data = r.json()

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("items", "data", "results"):
            v = data.get(k)
            if isinstance(v, list):
                return v
    return []


async def fetch_loop(queue: asyncio.Queue):
    status.fetch_enabled = getattr(status, "fetch_enabled", False)
    status.fetch_window_sec = getattr(status, "fetch_window_sec", settings.DEFAULT_WINDOW_SEC)
    status.fetch_poll_sec = getattr(status, "fetch_poll_sec", settings.DEFAULT_POLL_SEC)

    cursor: datetime | None = None
    last_status_cursor_iso: str | None = None
    last_enabled: bool = bool(getattr(status, "fetch_enabled", False))

    while True:
        try:
            enabled = bool(getattr(status, "fetch_enabled", False))
            if not enabled:
                last_enabled = False
                await asyncio.sleep(0.2)
                continue

            status_cursor_iso = getattr(status, "fetch_cursor_utc", None)

            if cursor is None:
                parsed = _parse_cursor_iso(status_cursor_iso)
                if parsed is None:
                    parsed = datetime.now(timezone.utc) - timedelta(seconds=settings.DEFAULT_WINDOW_SEC)
                cursor = parsed
                last_status_cursor_iso = status_cursor_iso

            if (not last_enabled) or (status_cursor_iso and status_cursor_iso != last_status_cursor_iso):
                parsed = _parse_cursor_iso(status_cursor_iso)
                if parsed is not None:
                    cursor = parsed
                else:
                    cursor = datetime.now(timezone.utc) - timedelta(seconds=settings.DEFAULT_WINDOW_SEC)

                last_status_cursor_iso = status_cursor_iso
                last_enabled = True

            window_sec = int(getattr(status, "fetch_window_sec", settings.DEFAULT_WINDOW_SEC) or settings.DEFAULT_WINDOW_SEC)
            poll_sec = int(getattr(status, "fetch_poll_sec", settings.DEFAULT_POLL_SEC) or settings.DEFAULT_POLL_SEC)

            now_utc = datetime.now(timezone.utc)

            if cursor > now_utc:
                cursor = now_utc

            end_utc = min(cursor + timedelta(seconds=window_sec), now_utc)

            if end_utc <= cursor:
                status.fetch_cursor_utc = cursor.isoformat()
                await asyncio.sleep(poll_sec)
                continue

            status.fetch_cursor_utc = cursor.isoformat()

            items = await fetch_theta(cursor, end_utc)

            for it in items:
                await queue.put(it)

            status.last_fetch_at = datetime.now(timezone.utc).timestamp()
            status.fetched_total += len(items)

            cursor = end_utc
            status.fetch_cursor_utc = cursor.isoformat()

            if cursor >= now_utc:
                await asyncio.sleep(poll_sec)

        except Exception as e:
            status.fetch_errors += 1
            status.error_logs.appendleft(f"FETCH ERROR: {repr(e)}")
            await asyncio.sleep(2.0)
