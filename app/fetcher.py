import asyncio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import List, Optional
from urllib.parse import urlencode

import httpx

from .settings import settings
from .status import status

BERLIN = ZoneInfo("Europe/Berlin")


def _build_auth_headers() -> dict:
    # aktuell nicht benötigt (Basic Auth läuft über httpx auth=...)
    if getattr(settings, "AUTH_TYPE", "none").lower() == "bearer" and getattr(
        settings, "AUTH_BEARER_TOKEN", None
    ):
        return {"Authorization": f"Bearer {settings.AUTH_BEARER_TOKEN}"}
    return {}


def _build_auth_tuple():
    if (
        getattr(settings, "AUTH_TYPE", "none").lower() == "basic"
        and getattr(settings, "AUTH_USERNAME", None)
        and getattr(settings, "AUTH_PASSWORD", None)
    ):
        return (settings.AUTH_USERNAME, settings.AUTH_PASSWORD)
    return None


def _fmt_api_dt(dt_local_berlin: datetime) -> str:
    """
    API erwartet: YYYY-MM-DD HH:MM:SS (mit Space).
    Wir senden Berlin-Zeit (lokal).
    """
    if dt_local_berlin.tzinfo is None:
        dt_local_berlin = dt_local_berlin.replace(tzinfo=BERLIN)
    dt_local_berlin = dt_local_berlin.astimezone(BERLIN)
    return dt_local_berlin.strftime("%Y-%m-%d %H:%M:%S")


def _build_url(start_local: datetime, end_local: datetime) -> str:
    """
    Muss so aussehen:
    https://theta-v2-server.5micron.net/basic-api/probes/hsl?&start_date=2025-12-10%2008:00:00&end_date=2025-12-24%2008:00:00
    """
    base = str(settings.API_BASE_URL).rstrip("?")
    params = {
        "start_date": _fmt_api_dt(start_local),
        "end_date": _fmt_api_dt(end_local),
    }
    # urlencode macht aus Space automatisch %20 -> passt zu deinem Beispiel
    return f"{base}?&{urlencode(params)}"


def _parse_start_cursor_from_status() -> Optional[datetime]:
    """
    Optional: wenn du irgendwo (z.B. status.fetch_cursor_iso) den Cursor speicherst,
    kannst du hier lesen. Wenn nicht vorhanden -> None.
    """
    v = getattr(status, "fetch_cursor_iso", None)
    if not v:
        return None
    try:
        # erwartet ISO-Format
        dt = datetime.fromisoformat(v)
        # wenn ohne tzinfo -> Berlin interpretieren
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=BERLIN)
        return dt.astimezone(BERLIN)
    except Exception:
        return None


def _now_berlin() -> datetime:
    return datetime.now(tz=BERLIN)


async def fetch_once(client: httpx.AsyncClient, start_local: datetime, end_local: datetime) -> List[dict]:
    url = _build_url(start_local, end_local)

    try:
        resp = await client.get(url)
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        text = ""
        try:
            text = e.response.text[:300]
        except Exception:
            pass
        await status.inc("fetch_errors")
        await status.log_error(f"FETCH HTTP ERROR: {e.response.status_code} url={url} body={text}")
        return []
    except Exception as e:
        await status.inc("fetch_errors")
        await status.log_error(f"FETCH RAW ERROR: {repr(e)} url={url}")
        return []

    raw = resp.json()
    if not isinstance(raw, list):
        await status.inc("fetch_errors")
        await status.log_error(f"FETCH ERROR: expected list got {type(raw)} url={url}")
        return []

    # raw ist bereits die finale JSON-Struktur (list of objects)
    return raw


async def fetch_loop(queue: asyncio.Queue):
    """
    Poll-Loop:
    - Startet erst, wenn ein Cursor gesetzt ist (über UI / Status).
      Wenn du das schon implementiert hast (POST /fetch/start), schreibst du dort
      status.fetch_cursor_iso = "...".
    - Wenn Cursor in der Vergangenheit: schnell "aufholen" (ohne in die Zukunft zu fetchen).
    - Sobald Cursor nahe an 'now' ist: normaler Poll mit sleeps.
    """
    headers = _build_auth_headers()
    auth = _build_auth_tuple()

    window_sec = int(getattr(settings, "API_WINDOW_SEC", 300))           # z.B. 300s
    poll_sec = float(getattr(settings, "API_POLL_INTERVAL_SEC", 30.0))  # z.B. 30s

    async with httpx.AsyncClient(timeout=30, headers=headers, auth=auth) as client:
        while True:
            try:
                cursor = _parse_start_cursor_from_status()

                # noch keine Startzeit gewählt -> warten
                if cursor is None:
                    await asyncio.sleep(1.0)
                    continue

                now = _now_berlin()

                # end = cursor + window, aber niemals > now
                end = cursor + timedelta(seconds=window_sec)
                if end > now:
                    end = now

                # Wenn cursor >= now: nichts zu holen -> schlafen bis nächste Poll-Iteration
                if cursor >= now:
                    await asyncio.sleep(poll_sec)
                    continue

                # Fetch
                items = await fetch_once(client, cursor, end)

                await status.inc("fetched_total", len(items))
                await status.set_time("last_fetch_at")

                for it in items:
                    await queue.put(it)

                # Cursor fortschreiben:
                # - wir schieben cursor auf "end" weiter (nicht auf now)
                new_cursor = end

                # Cursor speichern (wenn status das Feld hat)
                if hasattr(status, "fetch_cursor_iso"):
                    status.fetch_cursor_iso = new_cursor.isoformat()

                # Catch-up Logik:
                # Wenn wir deutlich hinterher sind, NICHT poll_sec schlafen,
                # sondern kurz schlafen (z.B. 0.1s) um schnell aufzuholen.
                behind = (_now_berlin() - new_cursor).total_seconds()

                if behind > window_sec:
                    await asyncio.sleep(0.1)
                else:
                    await asyncio.sleep(poll_sec)

            except Exception as e:
                await status.inc("fetch_errors")
                await status.log_error(f"FETCH LOOP ERROR: {repr(e)}")
                await asyncio.sleep(1.0)
