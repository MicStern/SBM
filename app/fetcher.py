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

THETA_TZ = ZoneInfo(settings.THETA_TIMEZONE)


def _theta_base_url() -> str:
    return settings.THETA_BASE_URL.rstrip("/") + "/" + settings.THETA_PROBE_PATH.lstrip("/")


def _format_theta_dt(dt_utc: datetime) -> str:
    """
    Theta erwartet: "YYYY-MM-DD HH:MM:SS" OHNE TZ.
    Wir formatieren in THETA_TIMEZONE (z.B. Europe/Berlin).
    """
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return dt_utc.astimezone(THETA_TZ).strftime("%Y-%m-%d %H:%M:%S")


def build_theta_url(start_utc: datetime, end_utc: datetime) -> str:
    """
    Exakt gleicher URL-Build für:
    - kontinuierlich
    - einmalig

    Wichtig:
    - Leerzeichen als %20 (nicht '+'), damit es sich wie Postman verhält.
    """
    base = _theta_base_url()
    start_s = quote(_format_theta_dt(start_utc), safe=":-_")
    end_s = quote(_format_theta_dt(end_utc), safe=":-_")
    return f"{base}?start_date={start_s}&end_date={end_s}"


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


async def fetch_theta(start_utc: datetime, end_utc: datetime) -> list[dict[str, Any]]:
    url = build_theta_url(start_utc, end_utc)
    auth, headers = _build_auth()

    # Log die echte URL, die auch Postman-mäßig aussieht
    status.error_logs.appendleft(f"FETCH REQ: {url}")

    timeout = httpx.Timeout(settings.HTTP_TIMEOUT_SEC)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        r = await client.get(url, headers=headers, auth=auth)

    if r.status_code == 401:
        raise RuntimeError("HTTP 401 from theta (auth wrong/missing).")
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
    """
    Kontinuierlicher Fetch:
    - Cursor läuft in UTC
    - 'now' wird in der Theta-Zeitzone bestimmt (damit 15:00 auch 15:00 ist)
    - end_date wird niemals "weiter als jetzt" in Theta-Zeit gesetzt
    - minimaler Overlap (1s), damit du keine Boundary-Daten verlierst
    """
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

            # Cursor initialisieren
            if cursor is None:
                parsed = _parse_cursor_iso(status_cursor_iso)
                if parsed is None:
                    parsed = datetime.now(timezone.utc) - timedelta(seconds=settings.DEFAULT_WINDOW_SEC)
                cursor = parsed
                last_status_cursor_iso = status_cursor_iso

            # Cursor aus UI übernehmen (Start/Override)
            if (not last_enabled) or (status_cursor_iso and status_cursor_iso != last_status_cursor_iso):
                parsed = _parse_cursor_iso(status_cursor_iso)
                cursor = parsed if parsed is not None else datetime.now(timezone.utc)
                last_status_cursor_iso = status_cursor_iso
                last_enabled = True
                status.error_logs.appendleft(f"FETCH CURSOR OVERRIDE: {cursor.isoformat()}")

            window_sec = int(getattr(status, "fetch_window_sec", settings.DEFAULT_WINDOW_SEC) or settings.DEFAULT_WINDOW_SEC)
            poll_sec = int(getattr(status, "fetch_poll_sec", settings.DEFAULT_POLL_SEC) or settings.DEFAULT_POLL_SEC)

            # "Jetzt" in Theta-Zeit (naiv), damit 17:31 Berlin auch 17:31 ist
            now_theta = datetime.now(THETA_TZ).replace(tzinfo=None)

            # Cursor in Theta-Zeit (naiv)
            cursor_theta = cursor.astimezone(THETA_TZ).replace(tzinfo=None)

            # Ende in Theta-Zeit
            end_theta = min(cursor_theta + timedelta(seconds=window_sec), now_theta)

            # caught up
            if end_theta <= cursor_theta:
                await asyncio.sleep(poll_sec)
                continue

            # Minimaler Overlap, damit boundary nicht verloren geht
            overlap = int(getattr(settings, "FETCH_OVERLAP_SEC", 1) or 0)
            if overlap > 0 and cursor_theta > datetime.min:
                cursor_theta_for_req = cursor_theta - timedelta(seconds=overlap)
            else:
                cursor_theta_for_req = cursor_theta

            start_utc = cursor_theta_for_req.replace(tzinfo=THETA_TZ).astimezone(timezone.utc)
            end_utc = end_theta.replace(tzinfo=THETA_TZ).astimezone(timezone.utc)

            status.error_logs.appendleft(f"FETCH RANGE (theta): {cursor_theta_for_req} -> {end_theta}")

            items = await fetch_theta(start_utc, end_utc)

            for it in items:
                await queue.put(it)

            status.last_fetch_at = datetime.now(timezone.utc).timestamp()
            status.fetched_total += len(items)

            # Cursor NUR vorziehen bis end_theta (ohne overlap)
            cursor = end_utc
            status.fetch_cursor_utc = cursor.isoformat()

            # wenn wir "am now" sind -> poll
            if end_theta >= now_theta:
                await asyncio.sleep(poll_sec)

        except Exception as e:
            status.fetch_errors += 1
            status.error_logs.appendleft(f"FETCH ERROR: {repr(e)}")
            await asyncio.sleep(2.0)
