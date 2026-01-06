import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, Form
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
)
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .db import SessionLocal, engine
from .fetcher import fetch_loop  # <- keine FetchConfig-Imports!
from .models import Base, Measurement, MeasurementGroup
from .settings import settings
from .status import status
from .storage import save_item


app = FastAPI()
templates = Jinja2Templates(directory="app/templates")

# -------- Zeitformat Berlin (für Template) --------
BERLIN_TZ = ZoneInfo("Europe/Berlin")

def ts_berlin(value):
    """
    Jinja Filter:
    - status.* timestamps sind floats (unix seconds)
    - optional: datetime (aware/naiv) wird auch unterstützt
    """
    if value is None:
        return "—"

    try:
        # 1) epoch seconds (float/int)
        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(float(value), tz=timezone.utc).astimezone(BERLIN_TZ)
            return dt.strftime("%Y-%m-%d %H:%M:%S %Z")

        # 2) datetime-Objekt
        if isinstance(value, datetime):
            # Wenn naiv, als UTC interpretieren (weil deine status times UTC epoch sind)
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

        # 3) string -> einfach so lassen (oder du kannst hier optional parse einbauen)
        return str(value)

    except Exception:
        return str(value)

templates.env.filters["ts_berlin"] = ts_berlin

# -------- Queue / Background workers --------
queue: asyncio.Queue | None = None


async def saver_loop(q: asyncio.Queue):
    while True:
        item = await q.get()
        try:
            await save_item(item)
        finally:
            q.task_done()


@app.on_event("startup")
async def startup():
    global queue
    queue = asyncio.Queue(maxsize=settings.QUEUE_MAXSIZE)

    # Tabellen erstellen (Prod: Alembic)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Fetch Loop
    asyncio.create_task(fetch_loop(queue))

    # Saver Workers
    for _ in range(settings.SAVE_CONCURRENCY):
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
            "error_logs": list(status.error_logs),
        }
    )


@app.post("/label")
async def set_label(label_uid: str = Form(...), label: str = Form("")):
    """
    Label setzen:
    - measurement_groups.label
    - alle measurements.label mit dieser label_uid
    """
    label_val = label.strip() or None

    async with SessionLocal() as session:
        # Group upsert (falls group noch nicht existiert)
        grp_stmt = (
            pg_insert(MeasurementGroup)
            .values(label_uid=label_uid, label=label_val, measurement_ids=[])
            .on_conflict_do_update(
                index_elements=[MeasurementGroup.label_uid],
                set_={"label": label_val},
            )
        )
        await session.execute(grp_stmt)

        # Measurements der Gruppe updaten
        await session.execute(
            update(Measurement)
            .where(Measurement.label_uid == label_uid)
            .values(label=label_val)
        )

        await session.commit()

    return RedirectResponse(url="/", status_code=303)


@app.get("/group_weights")
async def group_weights(label_uid: str):
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
        frames.append(
            {
                "timestamp": (
                    m.timestamp_sensor.astimezone(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S")
                    if m.timestamp_sensor
                    else (m.timestamp_sensor_iso or None)
                ),
                "weightA": float(m.weight_a or 0.0),
                "weightB": float(m.weight_b or 0.0),
                "weightC": float(m.weight_c or 0.0),
                "weightD": float(m.weight_d or 0.0),
            }
        )

    return JSONResponse({"label_uid": label_uid, "frames": frames})


@app.get("/", response_class=HTMLResponse)
async def status_page(request: Request):
    """
    Statusseite:
    - Runtime
    - Gruppenübersicht
    - Fetch-Status (für status.html: fetch.enabled / fetch.cursor_utc / fetch.window_sec / fetch.poll_sec)
    """
    # fetch-state so bereitstellen, wie status.html es erwartet
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
            "fetch": fetch,  # ✅ DAS fehlte
        },
    )
