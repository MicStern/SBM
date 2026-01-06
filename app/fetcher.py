import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx

from .settings import settings
from .status import status


# -----------------------------
# Helpers
# -----------------------------

def _build_auth_headers() -> dict:
    if settings.AUTH_TYPE.lower() == "bearer" and settings.AUTH_BEARER_TOKEN:
        return {"Authorization": f"Bearer {settings.AUTH_BEARER_TOKEN}"}
    return {}


def _build_auth_tuple():
    if (
        settings.AUTH_TYPE.lower() == "basic"
        and settings.AUTH_USERNAME
        and settings.AUTH_PASSWORD
    ):
        return (settings.AUTH_USERNAME, settings.AUTH_PASSWORD)
    return None


def _to_utc(dt: datetime) -> datetime:
    # Intern IMMER UTC-naiv vermeiden -> timezone-aware UTC
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fmt_api(dt_utc: datetime) -> str:
    # API erwartet: "YYYY-MM-DD HH:MM:SS" (ohne TZ), wir schicken UTC-Zeit als naive string
    dt_utc = _to_utc(dt_utc)
    return dt_utc.strftime("%Y-%m-%d %H:%M:%S")


def _parse_local_datetime(dt_str: str) -> datetime:
    """
    Erwartet input aus HTML datetime-local: "YYYY-MM-DDTHH:MM"
    Wir interpretieren das als Europe/Berlin local time, wandeln zu UTC.
    Ohne externe libs machen wir es pragmatisch:
    - Wir nehmen an, dass dein Server in Europe/Berlin läuft (was bei dir typischerweise so ist).
    - Dann interpretieren wir es als lokale Zeit und hängen die lokale tz an.
    """
    # datetime-local liefert z.B. 2026-01-06T10:30
    dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M")
    # "lokal" -> wir nehmen System-Local TZ Offset via astimezone Trick
    # (kein pytz/zoneinfo nötig)
    local_now = datetime.now().astimezone()
    local_tz = local_now.tzinfo
    dt_local = dt.replace(tzinfo=local_tz)
    return dt_local.astimezone(timezone.utc)


# -----------------------------
# Fetch runtime config
# -----------------------------

@dataclass
class FetchRuntimeConfig:
    enabled: bool = False
    cursor_utc: Optional[datetime] = None

    # "window" = wie groß ist ein Request-Fenster (Sekunden)
    window_sec: int = 300

    # "poll" = wie oft im Normalbetrieb abfragen (Sekunden)
    poll_sec: int = 30

    # Catchup: wenn Cursor weit hinter "jetzt" liegt, dann schneller loopen
    catchup_poll_sec: int = 1

    # Wie weit dürfen wir bis "jetzt" abfragen. (0 = bis jetzt)
    # Wir lassen 2 Sekunden Puffer, um nicht in die Zukunft zu rutschen.
    now_safety_lag_sec: int = 2


_fetch_cfg = FetchRuntimeConfig()
_fetch_lock = asyncio.Lock()


async def get_fetch_config() -> FetchRuntimeConfig:
    async with _fetch_lock:
        return FetchRuntimeConfig(**_fetch_cfg.__dict__)


async def start_fetch(cursor_utc: datetime, window_sec: int, poll_sec: int):
    cursor_utc = _to_utc(cursor_utc)
    window_sec = int(window_sec)
    poll_sec = int(poll_sec)

    if window_sec < 1:
        window_sec = 1
    if poll_sec < 1:
        poll_sec = 1

    async with _fetch_lock:
        _fetch_cfg.enabled = True
        _fetch_cfg.cursor_utc = cursor_utc
        _fetch_cfg.window_sec = window_sec
        _fetch_cfg.poll_sec = poll_sec

    await status.log_error(
        f"FETCH START: cursor={cursor_utc.isoformat()} window={window_sec}s poll={poll_sec}s"
    )


async def stop_fetch():
    async with _fetch_lock:
        _fetch_cfg.enabled = False
    await status.log_error("FETCH STOP")


def build_url(start_utc: datetime, end_utc: datetime) -> str:
    # URL muss genau so aussehen wie du es willst:
    # https://theta-v2-server.5micron.net/basic-api/probes/hsl?&start_date=...&end_date=...
    base = str(settings.API_BASE_URL).rstrip("?")
    start_str = _fmt_api(start_utc).replace(" ", "%20")
    end_str = _fmt_api(end_utc).replace(" ", "%20")
    return f"{base}?&start_date={start_str}&end_date={end_str}"


async def fetch_once(client: httpx.AsyncClient, start_utc: datetime, end_utc: datetime) -> List[Dict[str, Any]]:
    url = build_url(start_utc, end_utc)

    try:
        resp = await client.get(url)
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        text = ""
        try:
            text = e.response.text[:200]
        except Exception:
            pass
        await status.inc("fetch_errors")
        await status.log_error(f"FETCH HTTP ERROR: {e.response.status_code} {text} url={url}")
        return []
    except Exception as e:
        await status.inc("fetch_errors")
        await status.log_error(f"FETCH RAW ERROR: {repr(e)} url={url}")
        return []

    raw = resp.json()
    if not isinstance(raw, list):
        await status.inc("fetch_errors")
        await status.log_error(f"FETCH ERROR: unexpected response type {type(raw)} url={url}")
        return []

    # raw ist bereits Liste von dicts im finalen Format
    return raw


async def fetch_loop(queue: asyncio.Queue):
    headers = _build_auth_headers()
    auth = _build_auth_tuple()

    async with httpx.AsyncClient(timeout=30, headers=headers, auth=auth) as client:
        while True:
            cfg = await get_fetch_config()
            if not cfg.enabled or cfg.cursor_utc is None:
                await asyncio.sleep(0.5)
                continue

            cursor = _to_utc(cfg.cursor_utc)

            # wir fragen nie in die Zukunft:
            now_utc = datetime.now(timezone.utc) - timedelta(seconds=cfg.now_safety_lag_sec)

            # Endpunkt des Fensters:
            end_utc = cursor + timedelta(seconds=cfg.window_sec)
            if end_utc > now_utc:
                end_utc = now_utc

            # Wenn cursor schon >= now_utc -> warten bis neue Zeit vergangen ist
            if cursor >= now_utc:
                await asyncio.sleep(cfg.poll_sec)
                continue

            # Fetch durchführen
            items = await fetch_once(client, cursor, end_utc)
            await status.inc("fetched_total", len(items))
            await status.set_time("last_fetch_at")

            for it in items:
                await queue.put(it)

            # Cursor nach vorne schieben
            async with _fetch_lock:
                # Falls zwischenzeitlich gestoppt wurde
                if _fetch_cfg.enabled:
                    _fetch_cfg.cursor_utc = end_utc

            # Catchup-Mode: Wenn wir weit hinterher sind, schneller loopen
            # (z.B. Startdatum vor Tagen)
            after_cfg = await get_fetch_config()
            now_utc2 = datetime.now(timezone.utc) - timedelta(seconds=after_cfg.now_safety_lag_sec)
            behind = (now_utc2 - _to_utc(after_cfg.cursor_utc)).total_seconds() if after_cfg.cursor_utc else 0

            if behind > after_cfg.window_sec * 2:
                await asyncio.sleep(after_cfg.catchup_poll_sec)
            else:
                await asyncio.sleep(after_cfg.poll_sec)
