import asyncio
from typing import List

import httpx

from .settings import settings
from .schema import IncomingItem
from .status import status


def _build_auth_headers() -> dict:
    """
    Optional: Bearer Token aus ENV (falls später benötigt).
    Für Basic Auth nicht nötig.
    """
    if settings.AUTH_TYPE.lower() == "bearer" and settings.AUTH_BEARER_TOKEN:
        return {"Authorization": f"Bearer {settings.AUTH_BEARER_TOKEN}"}
    return {}


def _build_auth_tuple():
    """
    Basic Auth aus ENV:
    AUTH_TYPE=basic
    AUTH_USERNAME=...
    AUTH_PASSWORD=...
    """
    if (
        settings.AUTH_TYPE.lower() == "basic"
        and settings.AUTH_USERNAME
        and settings.AUTH_PASSWORD
    ):
        return (settings.AUTH_USERNAME, settings.AUTH_PASSWORD)
    return None


async def fetch_once(client: httpx.AsyncClient) -> List[IncomingItem]:
    """
    Holt eine Liste deiner JSON-Objekte von der API.
    Erwartetes Format: Liste von Objekten mit 'info' + 'data'.

    Testweise OHNE Query-Parameter: nur die Basis-URL.
    """
    try:
        resp = await client.get(str(settings.API_BASE_URL))
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        # HTTP-Fehler (401, 403, 500, ...)
        text = ""
        try:
            text = e.response.text[:200]
        except Exception:
            pass
        await status.log_error(
            f"FETCH HTTP ERROR: {e.response.status_code} {text}"
        )
        return []
    except Exception as e:
        # Netzwerkfehler etc.
        await status.log_error(f"FETCH RAW ERROR: {repr(e)}")
        return []

    raw = resp.json()

    if not isinstance(raw, list):
        await status.log_error(
            f"FETCH ERROR: unexpected response type {type(raw)} (expected list)"
        )
        return []

    try:
        items = [IncomingItem(**it) for it in raw]
    except Exception as e:
        await status.log_error(f"FETCH PARSE ERROR: {repr(e)}")
        return []

    return items


async def fetch_loop(queue: asyncio.Queue):
    """
    Endlosschleife: pollt deine API in Intervallen und legt Items in die Queue.
    """
    headers = _build_auth_headers()
    auth = _build_auth_tuple()

    async with httpx.AsyncClient(timeout=30, headers=headers, auth=auth) as client:
        while True:
            try:
                items = await fetch_once(client)
                await status.inc("fetched_total", len(items))
                await status.set_time("last_fetch_at")

                for it in items:
                    await queue.put(it.model_dump())

            except Exception as e:
                # Catch-all, falls im fetch_once was Unerwartetes durchkommt
                await status.inc("fetch_errors")
                await status.log_error(f"FETCH LOOP ERROR: {repr(e)}")

            await asyncio.sleep(settings.API_POLL_INTERVAL_SEC)
