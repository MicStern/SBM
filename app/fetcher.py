import asyncio
from datetime import datetime, timedelta
from typing import List

import httpx

from .settings import settings
from .schema import IncomingItem
from .status import status


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


def _make_time_window() -> tuple[str, str]:
    """
    Erzeugt ein Zeitfenster:
    end_date = jetzt
    start_date = end_date - 2 * Poll-Intervall

    Format: 'YYYY-MM-DD HH:MM:SS'
    """
    now = datetime.now()
    window = timedelta(seconds=settings.API_POLL_INTERVAL_SEC * 2)
    end_dt = now
    start_dt = now - window

    fmt = "%Y-%m-%d %H:%M:%S"
    start_str = start_dt.strftime(fmt)
    end_str = end_dt.strftime(fmt)
    return start_str, end_str


async def fetch_once(client: httpx.AsyncClient) -> List[IncomingItem]:
    start_str, end_str = _make_time_window()

    params = {
        "start_date": start_str,
        "end_date": end_str,
    }

    r = await client.get(str(settings.API_BASE_URL), params=params)
    r.raise_for_status()
    raw = r.json()

    if not isinstance(raw, list):
        # Falls der Server etwas anderes zurückliefert, lieber nichts tun,
        # statt das System zu zerschießen.
        return []

    # JSON -> Pydantic
    return [IncomingItem(**it) for it in raw]


async def fetch_loop(queue: asyncio.Queue):
    headers = _build_auth_headers()
    auth = _build_auth_tuple()

    async with httpx.AsyncClient(timeout=30, headers=headers, auth=auth) as client:
        while True:
            try:
                items = await fetch_once(client)
                await status.inc("fetched_total", len(items))
                await status.set_time("last_fetch_at")

                for it in items:
                    # Direkt in Queue legen
                    await queue.put(it.model_dump())

            except Exception as e:
                await status.inc("fetch_errors")
                await status.log_error(f"FETCH ERROR: {e}")

            await asyncio.sleep(settings.API_POLL_INTERVAL_SEC)
