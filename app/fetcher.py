# app/fetcher.py
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import httpx

from .settings import settings
from .status import status


def _iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _parse_iso_utc(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _auth_headers() -> dict[str, str]:
    """
    AUTH_TYPE:
      - "basic": nutzt httpx BasicAuth (nicht header hier)
      - "bearer": Authorization: Bearer <token>
      - "none": nichts
    """
    auth_type = (getattr(settings, "AUTH_TYPE", "none") or "none").lower().strip()
    if auth_type == "bearer":
        token = (getattr(settings, "AUTH_TOKEN", "") or "").strip()
        if token:
            return {"Authorization": f"Bearer {token}"}
    return {}


def _basic_auth() -> httpx.BasicAuth | None:
    auth_type = (getattr(settings, "AUTH_TYPE", "none") or "none").lower().strip()
    if auth_type != "basic":
        return None
    user = (getattr(settings, "AUTH_USERNAME", "") or "").strip()
    pwd = (getattr(settings, "AUTH_PASSWORD", "") or "").strip()
    if not user or not pwd:
        return None
    return httpx.BasicAuth(user, pwd)


async def fetch_theta(start_utc: datetime, end_utc: datetime) -> list[dict]:
    """
    Ruft Theta so ab, wie du es willst:

    https://theta-v2-server.5micron.net/basic-api/probes/hsl
      ?start_date=YYYY-MM-DD HH:MM:SS
      &end_date=YYYY-MM-DD HH:MM:SS
    """
    base = (getattr(settings, "THETA_BASE_URL", "https://theta-v2-server.5micron.net") or "").rstrip("/")
    path = (getattr(settings, "THETA_PATH", "/basic-api/probes/hsl") or "").strip()
    if not path.startswith("/"):
        path = "/" + path

    # Theta erwartet "YYYY-MM-DD HH:MM:SS" (mit SPACE)
    start_str = start_utc.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_utc.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    url = f"{base}{path}"
    params = {"start_date": start_str, "end_date": end_str}

    timeout = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)

    headers = _auth_headers()
    auth = _basic_auth()

    async with httpx.AsyncClient(timeout=timeout, headers=headers, auth=auth) as client:
        r = await client.get(url, params=params)
        if r.status_code == 401:
            raise RuntimeError("HTTP 401 from theta: Authorization header missing or invalid.")
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code} from theta: {r.text[:300]}")
        data = r.json()

    # ✅ Hier musst du ggf. anpassen, je nachdem wie Theta liefert.
    # Wir nehmen an: data ist Liste oder {"items":[...]}.
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if "items" in data and isinstance(data["items"], list):
            return data["items"]
        if "data" in data and isinstance(data["data"], list):
            return data["data"]

    # fallback: unbekanntes format
    return []


async def fetch_loop(queue: asyncio.Queue):
    """
    Background loop:
    - liest status.fetch_* (UI)
    - holt in Windows Daten von Theta
    - put() in queue
    - updated status.fetch_cursor_utc laufend => UI zeigt echten Fortschritt
    """

    # Defaults
    status.fetch_enabled = getattr(status, "fetch_enabled", False)
    status.fetch_window_sec = getattr(status, "fetch_window_sec", getattr(settings, "DEFAULT_WINDOW_SEC", 1800))
    status.fetch_poll_sec = getattr(status, "fetch_poll_sec", getattr(settings, "DEFAULT_POLL_SEC", 30))
    status.fetch_cursor_utc = getattr(status, "fetch_cursor_utc", None)
    status.fetch_started_at = getattr(status, "fetch_started_at", None)

    # interner cursor (falls UI noch keinen gesetzt hat)
    cursor = datetime.now(timezone.utc) - timedelta(seconds=int(getattr(settings, "DEFAULT_WINDOW_SEC", 1800)))
    if status.fetch_cursor_utc:
        desired = _parse_iso_utc(status.fetch_cursor_utc)
        if desired:
            cursor = desired
    status.fetch_cursor_utc = _iso_utc(cursor)

    while True:
        try:
            if not getattr(status, "fetch_enabled", False):
                await asyncio.sleep(0.2)
                continue

            # ✅ Cursor override: UI gewinnt (damit Startzeit wirklich gilt)
            desired = _parse_iso_utc(getattr(status, "fetch_cursor_utc", None))
            if desired and abs((desired - cursor).total_seconds()) > 0.5:
                cursor = desired
                status.error_logs.append(f"FETCH CURSOR OVERRIDE: cursor={_iso_utc(cursor)}")

            window_sec = int(getattr(status, "fetch_window_sec", getattr(settings, "DEFAULT_WINDOW_SEC", 1800)) or 1800)
            poll_sec = int(getattr(status, "fetch_poll_sec", getattr(settings, "DEFAULT_POLL_SEC", 30)) or 30)

            now_utc = datetime.now(timezone.utc)
            end_utc = min(cursor + timedelta(seconds=window_sec), now_utc)

            # nie in die Zukunft
            if end_utc <= cursor:
                await asyncio.sleep(poll_sec)
                continue

            # Fortschritt für UI
            status.fetch_cursor_utc = _iso_utc(cursor)

            items = await fetch_theta(cursor, end_utc)

            for it in items:
                await queue.put(it)

            status.last_fetch_at = datetime.now(timezone.utc).timestamp()
            status.fetched_total += len(items)

            # cursor nachziehen
            cursor = end_utc
            status.fetch_cursor_utc = _iso_utc(cursor)

            # wenn catchup fertig -> poll
            if cursor >= now_utc:
                await asyncio.sleep(poll_sec)

        except Exception as e:
            status.fetch_errors += 1
            status.error_logs.append(f"FETCH ERROR: {repr(e)}")
            await asyncio.sleep(2.0)
