import asyncio
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .analysis import analysis_loop
from .db import SessionLocal, engine
from .fetcher import fetch_loop, start_fetch, stop_fetch, get_fetch_config, _parse_local_datetime
from .models import Base, Measurement, MeasurementGroup
from .settings import settings
from .status import status
from .storage import save_item

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")

queue: asyncio.Queue | None = None
analysis_result: dict = {}


async def saver_loop(queue: asyncio.Queue):
    while True:
        item = await queue.get()
        try:
            await save_item(item)
        finally:
            queue.task_done()


@app.on_event("startup")
async def startup():
    global queue
    queue = asyncio.Queue(maxsize=settings.QUEUE_MAXSIZE)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    asyncio.create_task(fetch_loop(queue))

    for _ in range(settings.SAVE_CONCURRENCY):
        asyncio.create_task(saver_loop(queue))

    # Analyse optional (wenn du es behalten willst)
    if hasattr(settings, "ANALYSIS_INTERVAL_SEC"):
        asyncio.create_task(analysis_loop(analysis_result))


@app.get("/healthz")
async def healthz():
    return PlainTextResponse("ok")


@app.get("/status.json")
async def status_json():
    cfg = await get_fetch_config()
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
            "analysis": analysis_result,
            "error_logs": list(status.error_logs),
            "fetch": {
                "enabled": cfg.enabled,
                "cursor_utc": cfg.cursor_utc.isoformat() if cfg.cursor_utc else None,
                "window_sec": cfg.window_sec,
                "poll_sec": cfg.poll_sec,
            },
        }
    )


@app.post("/fetch/start")
async def fetch_start(
    start_dt: str = Form(...),      # datetime-local "YYYY-MM-DDTHH:MM"
    window_sec: int = Form(300),
    poll_sec: int = Form(30),
):
    cursor_utc = _parse_local_datetime(start_dt)
    await start_fetch(cursor_utc=cursor_utc, window_sec=window_sec, poll_sec=poll_sec)
    return RedirectResponse(url="/", status_code=303)


@app.post("/fetch/stop")
async def fetch_stop():
    await stop_fetch()
    return RedirectResponse(url="/", status_code=303)


@app.post("/label")
async def set_label(label_uid: str = Form(...), label: str = Form("")):
    """
    Label wird in measurement_groups.label und in allen measurements.label gespiegelt.
    """
    label_val = label.strip() or None

    async with SessionLocal() as session:
        # group upsert
        stmt = pg_insert(MeasurementGroup).values(label_uid=label_uid, label=label_val)
        stmt = stmt.on_conflict_do_update(
            index_elements=[MeasurementGroup.label_uid],
            set_={"label": label_val},
        )
        await session.execute(stmt)

        # measurements update (alle in dieser gruppe)
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
    Frames für Visualizer: timestamp + weight_a/b/c/d
    Sortierung: timestamp_sensor asc
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
                "timestamp": m.timestamp_sensor.isoformat() if m.timestamp_sensor else None,
                "weightA": float(m.weight_a or 0),
                "weightB": float(m.weight_b or 0),
                "weightC": float(m.weight_c or 0),
                "weightD": float(m.weight_d or 0),
            }
        )

    return JSONResponse({"label_uid": label_uid, "frames": frames})


@app.get("/", response_class=HTMLResponse)
async def status_page(request: Request):
    cfg = await get_fetch_config()

    async with SessionLocal() as session:
        # Gruppiert nach label_uid
        stats_subq = (
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
                func.coalesce(stats_subq.c.count, 0).label("count"),
                stats_subq.c.last_seen,
                MeasurementGroup.label,
            )
            .join(stats_subq, stats_subq.c.label_uid == MeasurementGroup.label_uid, isouter=True)
            .order_by(stats_subq.c.last_seen.desc().nullslast())
        )
        rows = (await session.execute(q)).all()

    groups = []
    for label_uid, cnt, last_seen, label in rows:
        groups.append(
            {
                "label_uid": label_uid,
                "count": cnt,
                "last_seen": last_seen.isoformat() if last_seen else None,
                "label": label,
            }
        )

    return templates.TemplateResponse(
        "status.html",
        {
            "request": request,
            "s": status,
            "queue_size": queue.qsize() if queue else 0,
            "analysis": analysis_result,
            "groups": groups,
            "fetch": {
                "enabled": cfg.enabled,
                "cursor_utc": cfg.cursor_utc.isoformat() if cfg.cursor_utc else None,
                "window_sec": cfg.window_sec,
                "poll_sec": cfg.poll_sec,
            },
        },
    )
