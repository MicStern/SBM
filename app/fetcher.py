import asyncio
import base64
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from .settings import settings
from .status import status


UTC = timezone.utc


def normalize_cursor(value: Any) -> datetime:
    """
    Cursor-Normalisierung:
    akzeptiert:
    - datetime (naiv -> UTC)
    - ISO-String (z.B. "2026-01-05T10:00:00+00:00" oder "...Z")
    - epoch seconds (int/float oder str)
    """
    if value is None:
        return datetime.now(UTC) - timedelta(seconds=settings.DEFAULT_WINDOW_SEC)

    # datetime
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

    # epoch seconds
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)

    # string -> ISO oder epoch
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return datetime.now(UTC) - timedelta(seconds=settings.DEFAULT_WINDOW_SEC)

        # epoch as string?
        try:
            return datetime.fromtimestamp(float(s), tz=UTC)
        except Exception:
            pass

        # ISO string
        # handle trailing Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
            dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
            return dt
        except Exception:
            # fallback: now - default window
            return datetime.now(UTC) - timedelta(seconds=settings.DEFAULT_WINDOW_SEC)

    # fallback
    return datetime.now(UTC) - timedelta(seconds=settings.DEFAULT_WINDOW_SEC)


def build_auth_headers() -> dict[str, str]:
    """
    Unterstützt:
    - AUTH_TYPE=basic + AUTH_USERNAME/PASSWORD
    - AUTH_TYPE=none
    """
    auth_type = (settings.AUTH_TYPE or "none").strip().lower()

    if auth_type == "basic":
        user = settings.AUTH_USERNAME or ""
        pwd = settings.AUTH_PASSWORD or ""
        token = base64.b64encode(f"{user}:{pwd}".encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {token}"}

    return {}


async def fetch_window(cursor_utc: datetime, window_sec: int) -> list[dict]:
    """
    Ruft ein Zeitfenster bei Theta ab.
    Annahme: Theta akzeptiert query params:
      - cursor (ISO8601)
      - window_sec (int)
    Passe URL/Params an deinen Theta-Endpoint an, falls nötig.
    """
    cursor_utc = normalize_cursor(cursor_utc)
    window_sec = int(window_sec)

    params = {
        "cursor": cursor_utc.isoformat(),
        "window_sec": window_sec,
    }

    headers = build_auth_headers()

    async with httpx.AsyncClient(timeout=settings.THETA_TIMEOUT_SEC) as client:
        resp = await client.get(f"{settings.THETA_BASE_URL}/measurements", params=params, headers=headers)

    if resp.status_code == 401:
        raise RuntimeError("HTTP 401 from theta: Authorization header missing or invalid.")
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code} from theta: {resp.text[:300]}")

    data = resp.json()

    # Erwartung: {"items":[...]} oder direkt Liste
    if isinstance(data, dict) and "items" in data:
        items = data["items"]
    else:
        items = data

    if not isinstance(items, list):
        raise RuntimeError(f"Unexpected theta response shape: {type(items)}")

    # Items müssen dicts sein
    return [x for x in items if isinstance(x, dict)]


async def fetch_loop(queue: asyncio.Queue):
    """
    Background loop:
    - nutzt status.fetch_* als Steuerung / Anzeige
    - cursor läuft immer nur bis 'jetzt' (UTC), nie in die Zukunft
    """
    # Defaults
    status.fetch_enabled = getattr(status, "fetch_enabled", False)
    status.fetch_window_sec = getattr(status, "fetch_window_sec", settings.DEFAULT_WINDOW_SEC)
    status.fetch_poll_sec = getattr(status, "fetch_poll_sec", settings.DEFAULT_POLL_SEC)

    # cursor kann (je nach vorherigen Änderungen) str/float/datetime sein -> normalisieren
    cursor_dt = normalize_cursor(getattr(status, "fetch_cursor_utc", None))
    status.fetch_cursor_utc = cursor_dt.isoformat()

    while True:
        try:
            if not getattr(status, "fetch_enabled", False):
                await asyncio.sleep(0.5)
                continue

            window_sec = int(getattr(status, "fetch_window_sec", settings.DEFAULT_WINDOW_SEC) or settings.DEFAULT_WINDOW_SEC)
            poll_sec = int(getattr(status, "fetch_poll_sec", settings.DEFAULT_POLL_SEC) or settings.DEFAULT_POLL_SEC)

            # cursor aus status wieder aufnehmen (kann string sein)
            cursor_dt = normalize_cursor(getattr(status, "fetch_cursor_utc", cursor_dt))
            now_dt = datetime.now(UTC)

            # Nie in die Zukunft fetchen
            if cursor_dt >= now_dt:
                await asyncio.sleep(max(1, poll_sec))
                continue

            status.last_fetch_at = datetime.now(UTC).timestamp()
            status.fetch_cursor_utc = cursor_dt.isoformat()

            items = await fetch_window(cursor_dt, window_sec)

            # Enqueue items
            for item in items:
                await queue.put(item)

            status.fetched_total += len(items)

            # Cursor fortschieben: standardmäßig um window_sec
            next_cursor = cursor_dt + timedelta(seconds=window_sec)

            # Wenn Theta weniger Daten liefert, kann man optional konservativer sein.
            # Hier: trotzdem window-basiert fortschieben, aber niemals über "now".
            if next_cursor > now_dt:
                next_cursor = now_dt

            cursor_dt = next_cursor
            status.fetch_cursor_utc = cursor_dt.isoformat()

            await asyncio.sleep(max(1, poll_sec))

        except Exception as e:
            status.fetch_errors += 1
            status.error_logs.append(f"FETCH ERROR: {repr(e)}")
            # kleines Backoff
            await asyncio.sleep(2.0)
