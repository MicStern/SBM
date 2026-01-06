import asyncio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx

from .settings import settings
from .status import status


UTC = timezone.utc
BERLIN = ZoneInfo("Europe/Berlin")


def build_theta_url(cursor_utc: datetime, window_sec: int) -> str:
    """
    Baue hier deinen Theta-Request zusammen.
    cursor_utc ist aware UTC datetime.
    window_sec ist die Fenstergröße in Sekunden.
    """
    end_utc = cursor_utc + timedelta(seconds=window_sec)

    # Beispiel (bitte an deinen Theta-Endpunkt anpassen):
    # /measurements?start=...&end=...
    start_iso = cursor_utc.isoformat()
    end_iso = end_utc.isoformat()

    return f"{settings.THETA_BASE_URL}/measurements?start={start_iso}&end={end_iso}"


def _get_httpx_auth():
    if settings.AUTH_TYPE.lower() == "basic":
        if not settings.AUTH_USERNAME or not settings.AUTH_PASSWORD:
            raise RuntimeError(
                "AUTH_TYPE=basic aber AUTH_USERNAME/AUTH_PASSWORD fehlen in ENV"
            )
        return httpx.BasicAuth(settings.AUTH_USERNAME, settings.AUTH_PASSWORD)
    return None


async def fetch_once(cursor_utc: datetime, window_sec: int) -> list[dict]:
    """
    Holt ein Fenster ab cursor_utc. Gibt eine Liste von JSON-Items zurück.
    """
    url = build_theta_url(cursor_utc, window_sec)
    auth = _get_httpx_auth()

    async with httpx.AsyncClient(timeout=settings.THETA_TIMEOUT_SEC, auth=auth) as client:
        resp = await client.get(url)

        if resp.status_code == 401:
            raise RuntimeError(
                "HTTP 401 from theta: Authorization header missing or invalid."
            )

        resp.raise_for_status()

        data = resp.json()

        # Falls Theta direkt { items: [...] } liefert, hier anpassen:
        if isinstance(data, dict) and "items" in data:
            return data["items"]

        if isinstance(data, list):
            return data

        # fallback
        return []


async def fetch_loop(queue: asyncio.Queue):
    """
    Läuft permanent im Hintergrund:
    - Wenn disabled: schläft kurz
    - Wenn enabled: holt Fenster ab cursor_utc (UTC)
    - Schiebt Items in queue
    - cursor_utc wird weitergeschoben (catchup bis jetzt)
    """
    while True:
        try:
            enabled = getattr(status, "fetch_enabled", False)
            if not enabled:
                await asyncio.sleep(0.5)
                continue

            cursor_utc = getattr(status, "fetch_cursor_utc", None)
            window_sec = int(getattr(status, "fetch_window_sec", 300) or 300)
            poll_sec = int(getattr(status, "fetch_poll_sec", 30) or 30)

            if cursor_utc is None:
                # Sicherheitsfallback: jetzt - window
                cursor_utc = datetime.now(UTC) - timedelta(seconds=window_sec)
                status.fetch_cursor_utc = cursor_utc

            # niemals in die Zukunft fetchen
            now_utc = datetime.now(UTC)
            if cursor_utc >= now_utc:
                await asyncio.sleep(poll_sec)
                continue

            # Fenster begrenzen, sodass end nicht > now
            end_utc = cursor_utc + timedelta(seconds=window_sec)
            if end_utc > now_utc:
                # verkürzen: nur bis jetzt
                effective_window = int((now_utc - cursor_utc).total_seconds())
                effective_window = max(effective_window, 1)
            else:
                effective_window = window_sec

            # Status-Update (für UI)
            status.last_fetch_at = datetime.now(UTC).timestamp()

            # Fetch
            items = await fetch_once(cursor_utc, effective_window)

            # Push in Queue
            for item in items:
                await queue.put(item)
                status.fetched_total += 1

            # Cursor weiterschieben
            status.fetch_cursor_utc = cursor_utc + timedelta(seconds=effective_window)

            # Wenn wir noch hinterherhängen → sofort nächstes Fenster (kein sleep)
            # Wenn wir "nahe an jetzt" sind → poll warten
            if status.fetch_cursor_utc < (datetime.now(UTC) - timedelta(seconds=window_sec)):
                # Catchup Mode: kein sleep
                continue

            await asyncio.sleep(poll_sec)

        except Exception as e:
            status.fetch_errors += 1
            status.error_logs.append(f"FETCH ERROR: {repr(e)}")
            await asyncio.sleep(2)
