import asyncio
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import aiohttp

from .settings import settings
from .status import status


def _fmt_api_dt(dt_utc: datetime) -> str:
    """
    API erwartet: YYYY-MM-DD HH:MM:SS (ohne T/Z), in URL encoded.
    Wir senden UTC-Zeit im Format, wie du es als Test-URL wolltest.
    """
    dt_utc = dt_utc.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%d %H:%M:%S")


def build_theta_url(start_utc: datetime, end_utc: datetime) -> str:
    start_s = quote(_fmt_api_dt(start_utc))
    end_s = quote(_fmt_api_dt(end_utc))
    # exakt wie von dir gefordert:
    # https://theta-v2-server.5micron.net/basic-api/probes/hsl?&start_date=...&end_date=...
    return f"{settings.THETA_BASE_URL}?&start_date={start_s}&end_date={end_s}"


async def fetch_window(start_utc: datetime, end_utc: datetime):
    """
    Holt JSON von Theta API.
    Erwartung: Liste von Messpunkten (JSON array) oder dict mit Feld das Liste enthält.
    """
    url = build_theta_url(start_utc, end_utc)

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                txt = await resp.text()
                raise RuntimeError(f"HTTP {resp.status} from theta: {txt[:300]}")
            return await resp.json()


async def fetch_loop(queue: asyncio.Queue):
    """
    Läuft dauerhaft.
    - Wenn fetch_enabled False -> schlafen.
    - Wenn cursor in Vergangenheit -> Catch-up: schnell durchlaufen bis jetzt.
    - Niemals in die Zukunft abfragen: window_end = min(cursor+window, now_utc).
    - Wenn cursor >= now -> warten poll_sec, dann erneut probieren.
    """
    while True:
        try:
            if not status.fetch_enabled:
                await asyncio.sleep(0.5)
                continue

            if not status.fetch_cursor_utc:
                # Safety: ohne Cursor nix zu tun
                await asyncio.sleep(0.5)
                continue

            cursor_utc = datetime.fromisoformat(status.fetch_cursor_utc)
            if cursor_utc.tzinfo is None:
                cursor_utc = cursor_utc.replace(tzinfo=timezone.utc)

            now_utc = datetime.now(timezone.utc)

            # Wenn Cursor schon "in der Zukunft" liegt: warten bis Zeit vorbei ist
            if cursor_utc >= now_utc:
                await asyncio.sleep(max(1, int(status.fetch_poll_sec or 30)))
                continue

            window_sec = int(status.fetch_window_sec or settings.DEFAULT_WINDOW_SEC)
            poll_sec = int(status.fetch_poll_sec or settings.DEFAULT_POLL_SEC)

            window_end = cursor_utc + timedelta(seconds=window_sec)

            # nie in die Zukunft
            if window_end > now_utc:
                window_end = now_utc

            # Fetch
            data = await fetch_window(cursor_utc, window_end)

            # Status updaten
            status.last_fetch_at = datetime.now(timezone.utc).timestamp()

            # items in queue
            # Unterstützt:
            # - list[dict]
            # - dict mit "data"/"items"/"results"
            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                for k in ("data", "items", "results"):
                    if k in data and isinstance(data[k], list):
                        items = data[k]
                        break
                if not items:
                    # Wenn dict ein einzelnes item ist
                    items = [data]
            else:
                items = []

            for item in items:
                await queue.put(item)
                status.fetched_total += 1

            # Cursor nach vorne schieben
            status.fetch_cursor_utc = window_end.astimezone(timezone.utc).isoformat()

            # Catch-up: wenn wir noch hinterherhinken, nicht poll-warten -> sofort weiter
            # Wenn wir jetzt "nahe dran" sind (cursor >= now) greift oben das poll-warten.
            await asyncio.sleep(0)

        except Exception as e:
            status.fetch_errors += 1
            status.error_logs.appendleft(f"FETCH ERROR: {repr(e)}")
            # kleine Backoff Pause
            await asyncio.sleep(2)
