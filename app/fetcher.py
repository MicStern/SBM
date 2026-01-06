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


# === KONFIG ===
THETA_TZ = ZoneInfo(settings.THETA_TIMEZONE)  # z.B. "Europe/Berlin"


def _theta_url() -> str:
    return settings.THETA_BASE_URL.rstrip("/") + "/" + settings.THETA_PROBE_PATH.lstrip("/")


def _format_theta_dt(dt_utc: datetime) -> str:
    """
    Theta erwartet: 'YYYY-MM-DD HH:MM:SS' (ohne TZ)
    Wir geben EXAKT die lokale Theta-Zeit zurück.
    """
    return dt_utc.astimezone(THETA_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _build_auth() -> tuple[httpx.Auth | None, dict[str, str]]:
    headers = {"Accept": "application/json"}
    auth_type = (settings.AUTH_TYPE or "none").lower()

    if auth_type == "basic":
        return httpx.BasicAuth(settings.AUTH_USERNAME, settings.AUTH_PASSWORD), headers

    if auth_type == "bearer":
        headers["Authorization"] = f"Bearer {settings.AUTH_BEARER_TOKEN}"
        return None, headers

    return None, headers


def _parse_cursor_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _build_theta_request_url(start_utc: datetime, end_utc: datetime) -> str:
    """
    Wichtig: Leerzeichen IMMER als %20 encoden (nicht '+')
    """
    base = _theta_url()

    start_s = quote(_format_theta_dt(start_utc), safe=":-_")
    end_s = quote(_format_theta_dt(end_utc), safe=":-_")

    return f"{base}?start_date={start_s}&end_date={end_s}"


async def fetch_theta(start_utc: datetime, end_utc: datetime) -> list[dict[str, Any]]:
    url = _build_theta_request_url(start_utc, end_utc)
    auth, headers = _build_auth()

    status.error_logs.appendleft(f"FETCH REQ: {url}")

    async with httpx.AsyncClient(timeout=settings.HTTP_TIMEOUT_SEC) as client:
        r = await client.get(url, headers=headers, auth=auth)

    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")

    data = r.json()

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("items", "data", "results"):
            if isinstance(data.get(k), list):
                return data[k]
    return []


async def fetch_loop(queue: asyncio.Queue):
    cursor: datetime | None = None
    last_status_cursor: str | None = None

    while True:
        try:
            if not status.fetch_enabled:
                await asyncio.sleep(0.2)
                continue

            # Cursor initialisieren oder aus UI übernehmen
            status_cursor = status.fetch_cursor_utc

            if cursor is None or status_cursor != last_status_cursor:
                parsed = _parse_cursor_iso(status_cursor)
                cursor = parsed if parsed else datetime.now(timezone.utc)
                last_status_cursor = status_cursor

            window_sec = int(status.fetch_window_sec)
            poll_sec = int(status.fetch_poll_sec)

            # === DAS IST DER KERN ===
            # "Jetzt" in DERSELBEN Zeitzone wie Theta
            now_theta = datetime.now(THETA_TZ).replace(tzinfo=None)

            # Cursor ebenfalls nach Theta-Zeit
            cursor_theta = cursor.astimezone(THETA_TZ).replace(tzinfo=None)

            end_theta = min(
                cursor_theta + timedelta(seconds=window_sec),
                now_theta
            )

            # Wenn wir real angekommen sind → warten
            if end_theta <= cursor_theta:
                await asyncio.sleep(poll_sec)
                continue

            # Zurück nach UTC
            end_utc = end_theta.replace(tzinfo=THETA_TZ).astimezone(timezone.utc)

            status.error_logs.appendleft(
                f"FETCH RANGE: {cursor_theta} -> {end_theta}"
            )

            items = await fetch_theta(cursor, end_utc)

            for it in items:
                await queue.put(it)

            cursor = end_utc
            status.fetch_cursor_utc = cursor.isoformat()
            status.fetched_total += len(items)
            status.last_fetch_at = datetime.now(timezone.utc).timestamp()

            if cursor_theta >= now_theta:
                await asyncio.sleep(poll_sec)

        except Exception as e:
            status.fetch_errors += 1
            status.error_logs.appendleft(f"FETCH ERROR: {repr(e)}")
            await asyncio.sleep(2)
