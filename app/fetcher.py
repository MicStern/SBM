import asyncio
from datetime import datetime, timedelta
from typing import List
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import select

from .settings import settings
from .schema import IncomingItem
from .status import status
from .db import SessionLocal
from .models import FetchConfig


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


async def _get_or_create_fetch_config() -> FetchConfig:
    async with SessionLocal() as session:
        cfg = (
            await session.execute(select(FetchConfig).where(FetchConfig.id == 1))
        ).scalar_one_or_none()
        if cfg:
            return cfg

        cfg = FetchConfig(
            id=1,
            enabled=False,
            cursor=None,
            window_seconds=settings.DEFAULT_WINDOW_SECONDS,
            poll_seconds=settings.DEFAULT_POLL_SECONDS,
        )
        session.add(cfg)
        await session.commit()
        return cfg


async def _set_fetch_config(**kwargs) -> None:
    async with SessionLocal() as session:
        cfg = (
            await session.execute(select(FetchConfig).where(FetchConfig.id == 1))
        ).scalar_one_or_none()

        if not cfg:
            cfg = FetchConfig(id=1)
            session.add(cfg)
            await session.flush()

        for k, v in kwargs.items():
            setattr(cfg, k, v)

        await session.commit()


def _fmt_api(dt: datetime, tz: ZoneInfo) -> str:
    """
    API erwartet: YYYY-MM-DD HH:MM:SS (ohne TZ in URL)
    Wir rechnen intern tz-aware (Europe/Berlin), formatieren aber ohne TZ.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")


async def fetch_once(
    client: httpx.AsyncClient,
    start_dt: datetime,
    end_dt: datetime,
    tz: ZoneInfo,
) -> List[IncomingItem]:
    params = {
        "start_date": _fmt_api(start_dt, tz),
        "end_date": _fmt_api(end_dt, tz),
    }
    url = f"{str(settings.API_BASE_URL)}?{urlencode(params)}"

    try:
        resp = await client.get(url)
        if hasattr(status, "set"):
            await status.set("last_fetch_http", {"status": resp.status_code, "url": url})
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        text = ""
        try:
            text = e.response.text[:500]
        except Exception:
            pass
        await status.log_error(f"FETCH HTTP ERROR: {e.response.status_code} url={url} body={text}")
        await status.inc("fetch_errors")
        return []
    except Exception as e:
        await status.log_error(f"FETCH RAW ERROR: {repr(e)} url={url}")
        await status.inc("fetch_errors")
        return []

    try:
        raw = resp.json()
    except Exception as e:
        await status.log_error(f"FETCH JSON ERROR: {repr(e)} url={url}")
        await status.inc("fetch_errors")
        return []

    if not isinstance(raw, list):
        await status.log_error(f"FETCH ERROR: unexpected response type {type(raw)} url={url}")
        await status.inc("fetch_errors")
        return []

    try:
        items = [IncomingItem(**it) for it in raw]
    except Exception as e:
        await status.log_error(f"FETCH PARSE ERROR: {repr(e)} url={url}")
        await status.inc("fetch_errors")
        return []

    return items


async def fetch_loop(queue: asyncio.Queue):
    """
    - Start erst wenn enabled=True und cursor gesetzt
    - Catch-up: wenn cursor in Vergangenheit -> schnell durchlaufen bis jetzt
    - Niemals in Zukunft abfragen: end=min(cursor+window, now)
    - Wenn cursor > now -> warten bis Zeit erreicht ist (kein 1h TZ-Bug)
    """
    tz = ZoneInfo(settings.TIMEZONE)

    headers = _build_auth_headers()
    auth = _build_auth_tuple()

    async with httpx.AsyncClient(timeout=60, headers=headers, auth=auth) as client:
        while True:
            try:
                cfg = await _get_or_create_fetch_config()

                if not cfg.enabled or cfg.cursor is None:
                    await asyncio.sleep(1.0)
                    continue

                cursor = cfg.cursor
                if cursor.tzinfo is None:
                    cursor = cursor.replace(tzinfo=tz)
                cursor = cursor.astimezone(tz)

                window = int(cfg.window_seconds)
                poll = float(cfg.poll_seconds)

                now = datetime.now(tz)

                # cursor liegt in Zukunft -> warten
                if now <= cursor:
                    wait_s = (cursor - now).total_seconds()
                    wait_s = max(0.5, min(wait_s, 60.0))
                    if hasattr(status, "set"):
                        await status.set("fetch_mode", {"mode": "waiting", "wait_seconds": round(wait_s, 2)})
                    await asyncio.sleep(wait_s)
                    continue

                proposed_end = cursor + timedelta(seconds=window)
                end_dt = proposed_end if proposed_end <= now else now

                if end_dt <= cursor:
                    await asyncio.sleep(0.5)
                    continue

                if hasattr(status, "set"):
                    await status.set(
                        "last_fetch_window",
                        {
                            "start": _fmt_api(cursor, tz),
                            "end": _fmt_api(end_dt, tz),
                            "window_seconds": window,
                            "poll_seconds": poll,
                        },
                    )

                items = await fetch_once(client, cursor, end_dt, tz)
                await status.inc("fetched_total", len(items))
                await status.set_time("last_fetch_at")

                for it in items:
                    await queue.put(it.model_dump())

                # cursor fortschreiben
                await _set_fetch_config(cursor=end_dt)

                behind = (datetime.now(tz) - end_dt).total_seconds()
                if behind > 2.0:
                    if hasattr(status, "set"):
                        await status.set("fetch_mode", {"mode": "catch_up", "behind_seconds": round(behind, 2)})
                    await asyncio.sleep(0.1)
                else:
                    if hasattr(status, "set"):
                        await status.set("fetch_mode", {"mode": "live", "behind_seconds": round(behind, 2)})
                    await asyncio.sleep(poll)

            except Exception as e:
                await status.inc("fetch_errors")
                await status.log_error(f"FETCH LOOP ERROR: {repr(e)}")
                await asyncio.sleep(5.0)
