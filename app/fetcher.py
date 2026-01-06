import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlencode
from typing import List, Optional

import httpx

from .settings import settings
from .status import status

UTC = ZoneInfo("UTC")
BERLIN = ZoneInfo("Europe/Berlin")


def _build_auth_headers() -> dict:
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


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


def _fmt_api_dt_from_utc(dt_utc: datetime) -> str:
    """
    API erwartet "YYYY-MM-DD HH:MM:SS" in Berlin-Lokalzeit.
    """
    dt_local = dt_utc.astimezone(BERLIN)
    return dt_local.strftime("%Y-%m-%d %H:%M:%S")


def _build_url(start_utc: datetime, end_utc: datetime) -> str:
    base = str(settings.API_BASE_URL).rstrip("?")
    params = {
        "start_date": _fmt_api_dt_from_utc(start_utc),
        "end_date": _fmt_api_dt_from_utc(end_utc),
    }
    return f"{base}?&{urlencode(params)}"


def _cursor_from_status() -> Optional[datetime]:
    if not status.fetch_enabled:
        return None
    v = status.fetch_cursor_iso
    if not v:
        return None
    try:
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return None


async def fetch_once(client: httpx.AsyncClient, start_utc: datetime, end_utc: datetime) -> List[dict]:
    url = _build_url(start_utc, end_utc)
    try:
        resp = await client.get(url)
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        txt = ""
        try:
            txt = e.response.text[:300]
        except Exception:
            pass
        await status.inc("fetch_errors")
        await status.log_error(f"FETCH HTTP ERROR: {e.response.status_code} url={url} body={txt}")
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

    return raw


async def fetch_loop(queue: asyncio.Queue):
    headers = _build_auth_headers()
    auth = _build_auth_tuple()

    window_sec = int(getattr(settings, "API_WINDOW_SEC", 300))
    poll_sec = float(getattr(settings, "API_POLL_INTERVAL_SEC", 30.0))

    async with httpx.AsyncClient(timeout=30, headers=headers, auth=auth) as client:
        while True:
            try:
                cursor = _cursor_from_status()
                if cursor is None:
                    await asyncio.sleep(1.0)
                    continue

                now = _now_utc()

                if cursor >= now:
                    await asyncio.sleep(poll_sec)
                    continue

                end = cursor + timedelta(seconds=window_sec)
                if end > now:
                    end = now

                items = await fetch_once(client, cursor, end)
                await status.inc("fetched_total", len(items))
                await status.set_time("last_fetch_at")

                for it in items:
                    await queue.put(it)

                # Cursor fortschreiben
                status.fetch_cursor_iso = end.isoformat()

                # Catch-up: wenn wir noch deutlich hinterher sind -> fast weiterlaufen
                behind = (_now_utc() - end).total_seconds()
                if behind > window_sec:
                    await asyncio.sleep(0.1)
                else:
                    await asyncio.sleep(poll_sec)

            except Exception as e:
                await status.inc("fetch_errors")
                await status.log_error(f"FETCH LOOP ERROR: {repr(e)}")
                await asyncio.sleep(1.0)
