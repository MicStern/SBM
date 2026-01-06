# app/main.py
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .db import SessionLocal, engine
from .models import Base, Measurement, MeasurementGroup
from .settings import settings
from .status import status
from .storage import save_item
from .fetcher import fetch_loop


app = FastAPI()
templates = Jinja2Templates(directory="app/templates")

BERLIN_TZ = ZoneInfo("Europe/Berlin")


def ts_berlin(value):
    """
    Jinja Filter:
    - epoch seconds (float/int) => Berlin time
    - datetime (aware/naiv) => Berlin time
    - string => string (unverändert)
    """
    if value is None:
        return "—"
    try:
        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(float(value), tz=timezone.utc).astimezone(BERLIN_TZ)
            return dt.strftime("%Y-%m-%d %H:%M:%S %Z")

        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

        return str(value)
    except Exception:
        return str(value)


templates.env.filters["ts_berlin"] = ts_berlin

queue: asyncio.Queue | None = None


async def saver_loop(q: asyncio.Queue):
    while True:
        item = await q.get()
        try:
            await save_item(item)
        except Exception as e:
            status.save_errors += 1
            status.error_logs.append(f"SAVE ERROR: {repr(e)}")
        finally:
            q.task_done()


@app.on_event("startup")
async def startup():
    global queue
    queue = asyncio.Queue(maxsize=getattr(settings, "QUEUE_MAXSIZE", 10000))

    # Tabellen anlegen (Prod: Alembic)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # status defaults
    if not hasattr(status, "started_at") or status.started_at is None:
        status.started_at = datetime.now(timezone.utc).timestamp()
    if not hasattr(status, "last_fetch_at"):
        status.last_fetch_at = None
    if not hasattr(status, "last_save_at"):
        status.last_save_at = None
    if not hasattr(status, "fetched_total"):
        status.fetched_total = 0
    if not hasattr(status, "saved_total"):
        status.saved_total = 0
    if not hasattr(status, "fetch_errors"):
        status.fetch_errors = 0
    if not hasattr(status, "save_errors"):
        status.save_errors = 0
    if not hasattr(status, "error_logs"):
        status.error_logs = []

    # Fetcher state
    status.fetch_enabled = getattr(status, "fetch_enabled", False)
    status.fetch_started_at = getattr(status, "fetch_started_at", None)
    status.fetch_cursor_utc = getattr(status, "fetch_cursor_utc", None)
    status.fetch_window_sec = getattr(status, "fetch_window_sec", getattr(settings, "DEFAULT_WINDOW_SEC", 1800))
    status.fetch_poll_sec = getattr(status, "fetch_poll_sec", getattr(settings, "DEFAULT_POLL_SEC", 30))

    # Fetch Loop
    asyncio.create_task(fetch_loop(queue))

    # Saver Workers
    for _ in range(int(getattr(settings, "SAVE_CONCURRENCY", 2))):
        asyncio.create_task(saver_loop(queue))


@app.get("/healthz")
async def healthz():
    return PlainTextResponse("ok")


@app.get("/status.json")
async def status_json():
    return JSONResponse(
        {
            "started_at": status.started_at,
            "last_fetch_at": status.last_fetch_at,
            "last_save_at": status.last_save_at,
            "fetched_total": status.fetched_total,
            "saved_total": status.saved_total,
            "fetch_errors": status.fetch_errors,
            "save_errors": status.save_errors,
            "queue_size": queue.qsize() if queue else 0,
            "fetch_enabled": getattr(status, "fetch_enabled", False),
            "fetch_started_at": getattr(status, "fetch_started_at", None),
            "fetch_cursor_utc": getattr(status, "fetch_cursor_utc", None),
            "fetch_window_sec": getattr(status, "fetch_window_sec", None),
            "fetch_poll_sec": getattr(status, "fetch_poll_sec", None),
            "error_logs": list(status.error_logs),
        }
    )


@app.post("/fetch/start")
async def fetch_start(
    start_dt: str = Form(...),  # kommt als "YYYY-MM-DDTHH:MM"
    window_sec: int = Form(default=None),
    poll_sec: int = Form(default=None),
):
    """
    Startzeit ist im UI Berlin-lokal (datetime-local ohne TZ).
    Wir interpretieren es als Europe/Berlin und speichern Cursor in UTC ISO.
    """
    try:
        local = datetime.fromisoformat(start_dt)  # naive
    except Exception:
        # Fallback: wenn browser komisch sendet
        local = datetime.strptime(start_dt, "%Y-%m-%dT%H:%M")

    local = local.replace(tzinfo=BERLIN_TZ)
    start_utc = local.astimezone(timezone.utc)

    status.fetch_enabled = True
    status.fetch_started_at = datetime.now(timezone.utc).timestamp()

    if window_sec is None:
        window_sec = int(getattr(settings, "DEFAULT_WINDOW_SEC", 1800))
    if poll_sec is None:
        poll_sec = int(getattr(settings, "DEFAULT_POLL_SEC", 30))

    status.fetch_window_sec = int(window_sec)
    status.fetch_poll_sec = int(poll_sec)

    # ✅ DAS ist der Cursor, den der fetch_loop aktiv übernimmt
    status.fetch_cursor_utc = start_utc.isoformat()

    status.error_logs.append(
        f"FETCH START: cursor={status.fetch_cursor_utc} window={status.fetch_window_sec}s poll={status.fetch_poll_sec}s"
    )

    return RedirectResponse(url="/", status_code=303)


@app.post("/fetch/stop")
async def fetch_stop():
    status.fetch_enabled = False
    status.error_logs.append("FETCH STOP")
    return RedirectResponse(url="/", status_code=303)


@app.post("/label")
async def set_label(label_uid: str = Form(...), label: str = Form("")):
    """
    Label setzen:
    - measurement_groups.label
    - alle measurements.label mit dieser label_uid
    """
    label_val = label.strip() or None

    async with SessionLocal() as session:
        # Group upsert
        grp_stmt = (
            pg_insert(MeasurementGroup)
            .values(label_uid=label_uid, label=label_val, measurement_ids=[])
            .on_conflict_do_update(
                index_elements=[MeasurementGroup.label_uid],
                set_={"label": label_val},
            )
        )
        await session.execute(grp_stmt)

        # Measurements updaten
        await session.execute(
            update(Measurement)
            .where(Measurement.label_uid == label_uid)
            .values(label=label_val)
        )

        await session.commit()

    return RedirectResponse(url="/", status_code=303)


@app.get("/group_frames")
async def group_frames(label_uid: str):
    """
    Für Schwerpunkt-Visualizer:
    liefert Frames für eine Gruppe (label_uid) sortiert nach timestamp_sensor.
    """
    async with SessionLocal() as session:
        q = (
            select(Measurement)
            .where(Measurement.label_uid == label_uid)
            .order_by(Measurement.timestamp_sensor.asc())
        )
        rows = (await session.execute(q)).scalars().all()

    frames = []
    for m in rows:
        # weight fields: passe ggf. an deine echten Spaltennamen an
        frames.append(
            {
                "timestamp": (
                    m.timestamp_sensor.astimezone(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
                    if getattr(m, "timestamp_sensor", None)
                    else getattr(m, "timestamp_sensor_iso", None)
                ),
                "weightA": float(getattr(m, "weight_a", 0.0) or 0.0),
                "weightB": float(getattr(m, "weight_b", 0.0) or 0.0),
                "weightC": float(getattr(m, "weight_c", 0.0) or 0.0),
                "weightD": float(getattr(m, "weight_d", 0.0) or 0.0),
            }
        )

    return JSONResponse({"label_uid": label_uid, "frames": frames})


@app.get("/", response_class=HTMLResponse)
async def status_page(request: Request):
    """
    Statusseite:
    - Runtime
    - Gruppenübersicht
    - Fetch-Status (fetch.enabled / fetch.cursor_utc / fetch.window_sec / fetch.poll_sec)
    """

    fetch = {
        "enabled": getattr(status, "fetch_enabled", False),
        "cursor_utc": getattr(status, "fetch_cursor_utc", None),
        "window_sec": getattr(status, "fetch_window_sec", None),
        "poll_sec": getattr(status, "fetch_poll_sec", None),
    }

    async with SessionLocal() as session:
        agg = (
            select(
                Measurement.label_uid.label("label_uid"),
                func.count(Measurement.id).label("count"),
                func.max(Measurement.timestamp_sensor).label("last_seen"),
            )
            .group_by(Measurement.label_uid)
            .subquery()
        )

        q = (
            select(
                MeasurementGroup.label_uid,
                func.coalesce(agg.c.count, 0).label("count"),
                agg.c.last_seen,
                MeasurementGroup.label,
            )
            .join(agg, agg.c.label_uid == MeasurementGroup.label_uid, isouter=True)
            .order_by(agg.c.last_seen.desc().nullslast())
        )

        rows = (await session.execute(q)).all()

    groups = []
    for uid, cnt, last_seen, label in rows:
        groups.append(
            {
                "label_uid": uid,
                "count": int(cnt or 0),
                "last_seen": (
                    last_seen.astimezone(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
                    if last_seen
                    else None
                ),
                "label": label,
            }
        )

    return templates.TemplateResponse(
        "status.html",
        {
            "request": request,
            "s": status,
            "queue_size": queue.qsize() if queue else 0,
            "groups": groups,
            "fetch": fetch,
        },
    )
