import asyncio
from datetime import datetime, timedelta, timezone
from typing import List
from urllib.parse import urlencode

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
        cfg = (await session.execute(select(FetchConfig).where(FetchConfig.id == 1))).scalar_one_or_none()
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
        cfg = (await session.execute(select(FetchConfig).where(FetchConfig.id == 1))).scalar_one_or_none()
        if not cfg:
            cfg = FetchConfig(id=1)
            session.add(cfg)
            await session.flush()

        for k, v in kwargs.items():
            setattr(cfg, k, v)

        await session.commit()


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def fetch_once(client: httpx.AsyncClient, start_dt: datetime, end_dt: datetime) -> List[IncomingItem]:
    start_dt = _ensure_utc(start_dt)
    end_dt = _ensure_utc(end_dt)

    params = {
        "start_date": start_dt.isoformat(timespec="seconds"),
        "end_date": end_dt.isoformat(timespec="seconds"),
    }
    url = f"{str(settings.API_BASE_URL)}?{urlencode(params)}"

    try:
        resp = await client.get(url)
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        text = ""
        try:
            text = e.response.text[:300]
        except Exception:
            pass
        await status.log_error(f"FETCH HTTP ERROR: {e.response.status_code} {text}")
        await status.inc("fetch_errors")
        return []
    except Exception as e:
        await status.log_error(f"FETCH RAW ERROR: {repr(e)}")
        await status.inc("fetch_errors")
        return []

    raw = resp.json()
    if not isinstance(raw, list):
        await status.log_error(f"FETCH ERROR: unexpected response type {type(raw)} (expected list)")
        await status.inc("fetch_errors")
        return []

    try:
        items = [IncomingItem(**it) for it in raw]
    except Exception as e:
        await status.log_error(f"FETCH PARSE ERROR: {repr(e)}")
        await status.inc("fetch_errors")
        return []

    return items


async def fetch_loop(queue: asyncio.Queue):
    headers = _build_auth_headers()
    auth = _build_auth_tuple()

    async with httpx.AsyncClient(timeout=60, headers=headers, auth=auth) as client:
        while True:
            try:
                cfg = await _get_or_create_fetch_config()

                if not cfg.enabled or cfg.cursor is None:
                    await asyncio.sleep(1.0)
                    continue

                cursor = _ensure_utc(cfg.cursor)
                end_dt = cursor + timedelta(seconds=int(cfg.window_seconds))

                # optional debug
                if hasattr(status, "set"):
                    await status.set("last_fetch_window", {
                        "start": cursor.isoformat(),
                        "end": end_dt.isoformat(),
                        "window_seconds": int(cfg.window_seconds),
                    })

                items = await fetch_once(client, cursor, end_dt)

                await status.inc("fetched_total", len(items))
                await status.set_time("last_fetch_at")

                for it in items:
                    await queue.put(it.model_dump())

                await _set_fetch_config(cursor=end_dt)

                await asyncio.sleep(float(cfg.poll_seconds))

            except Exception as e:
                await status.inc("fetch_errors")
                await status.log_error(f"FETCH LOOP ERROR: {repr(e)}")
                await asyncio.sleep(5.0)
