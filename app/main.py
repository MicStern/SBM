import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func

from .analysis import analysis_loop
from .db import SessionLocal, engine
from .fetcher import fetch_loop
from .models import Base, Measurement, MeasurementGroup, FetchConfig
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


async def get_or_create_fetch_config() -> FetchConfig:
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


async def update_fetch_config(**kwargs) -> None:
    async with SessionLocal() as session:
        cfg = (await session.execute(select(FetchConfig).where(FetchConfig.id == 1))).scalar_one_or_none()
        if not cfg:
            cfg = FetchConfig(id=1)
            session.add(cfg)
            await session.flush()
        for k, v in kwargs.items():
            setattr(cfg, k, v)
        await session.commit()


@app.on_event("startup")
async def startup():
    global queue
    queue = asyncio.Queue(maxsize=settings.QUEUE_MAXSIZE)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    asyncio.create_task(fetch_loop(queue))

    for _ in range(settings.SAVE_CONCURRENCY):
        asyncio.create_task(saver_loop(queue))

    asyncio.create_task(analysis_loop(analysis_result))


@app.get("/healthz")
async def healthz():
    return PlainTextResponse("ok")


@app.get("/status.json")
async def status_json():
    cfg = await get_or_create_fetch_config()
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
            "debug": getattr(status, "debug", {}),
            "fetch_config": {
                "enabled": cfg.enabled,
                "cursor": cfg.cursor.isoformat() if cfg.cursor else None,
                "window_seconds": cfg.window_seconds,
                "poll_seconds": cfg.poll_seconds,
            },
        }
    )


@app.post("/fetch/start")
async def fetch_start(
    start_dt: str = Form(...),
    window_seconds: int = Form(300),
    poll_seconds: int = Form(30),
):
    tz = ZoneInfo(settings.TIMEZONE)

    # datetime-local => naive, wir interpretieren als Europe/Berlin
    dt = datetime.fromisoformat(start_dt).replace(tzinfo=tz)

    await update_fetch_config(
        enabled=True,
        cursor=dt,
        window_seconds=int(window_seconds),
        poll_seconds=int(poll_seconds),
    )

    await status.log_error(
        f"FETCH START: cursor={dt.isoformat()} window={window_seconds}s poll={poll_seconds}s"
    )
    return RedirectResponse(url="/", status_code=303)


@app.post("/fetch/stop")
async def fetch_stop():
    await update_fetch_config(enabled=False)
    await status.log_error("FETCH STOP")
    return RedirectResponse(url="/", status_code=303)


@app.post("/fetch/reset")
async def fetch_reset():
    await update_fetch_config(enabled=False, cursor=None)
    await status.log_error("FETCH RESET")
    return RedirectResponse(url="/", status_code=303)


@app.get("/groups.json")
async def groups_json():
    async with SessionLocal() as session:
        q = (
            select(
                MeasurementGroup.label_uid,
                func.cardinality(MeasurementGroup.measurement_ids).label("count"),
            )
            .order_by(func.cardinality(MeasurementGroup.measurement_ids).desc())
        )
        rows = (await session.execute(q)).all()

    return JSONResponse(
        [{"label_uid": uid, "count": cnt or 0} for (uid, cnt) in rows]
    )


@app.get("/packet_weights")
async def packet_weights(packet_id: str):
    """
    Visualizer Frames: alle Messungen der Gruppe (label_uid),
    sortiert nach timestamp_sensor.
    """
    async with SessionLocal() as session:
        q = (
            select(Measurement)
            .where(Measurement.label_uid == packet_id)
            .order_by(Measurement.timestamp_sensor.asc().nulls_last(), Measurement.id.asc())
        )
        rows = (await session.execute(q)).scalars().all()

    frames = []
    for m in rows:
        ts = m.timestamp_sensor.strftime("%Y-%m-%d %H:%M:%S") if m.timestamp_sensor else None
        frames.append(
            {
                "timestamp": ts,
                "weightA": float(m.weight_a or 0.0),
                "weightB": float(m.weight_b or 0.0),
                "weightC": float(m.weight_c or 0.0),
                "weightD": float(m.weight_d or 0.0),
            }
        )

    return JSONResponse({"packet_id": packet_id, "frames": frames})


@app.get("/", response_class=HTMLResponse)
async def status_page(request: Request):
    cfg = await get_or_create_fetch_config()

    async with SessionLocal() as session:
        # Gruppenliste + Count
        q_groups = (
            select(
                MeasurementGroup.label_uid,
                func.cardinality(MeasurementGroup.measurement_ids).label("count"),
            )
            .order_by(func.cardinality(MeasurementGroup.measurement_ids).desc())
            .limit(200)
        )
        groups_rows = (await session.execute(q_groups)).all()

        # optional: label + last_seen aus measurements aggregiert (praktisch fürs UI)
        group_uids = [uid for (uid, _) in groups_rows]
        labels_map = {}
        last_seen_map = {}

        if group_uids:
            q_meta = (
                select(
                    Measurement.label_uid,
                    func.max(Measurement.timestamp_sensor).label("last_seen"),
                    func.max(Measurement.label).label("label"),
                )
                .where(Measurement.label_uid.in_(group_uids))
                .group_by(Measurement.label_uid)
            )
            meta_rows = (await session.execute(q_meta)).all()
            for uid, last_seen, lbl in meta_rows:
                last_seen_map[uid] = last_seen.strftime("%Y-%m-%d %H:%M:%S") if last_seen else None
                labels_map[uid] = lbl

    groups = []
    for uid, cnt in groups_rows:
        groups.append(
            {
                "packet_id": uid,
                "count": int(cnt or 0),
                "last_seen": last_seen_map.get(uid),
                "label": labels_map.get(uid),
            }
        )

    status_json_debug = json.dumps(getattr(status, "debug", {}), indent=2, ensure_ascii=False)

    return templates.TemplateResponse(
        "status.html",
        {
            "request": request,
            "s": status,
            "queue_size": queue.qsize() if queue else 0,
            "analysis": analysis_result,
            "packets": groups,  # UI nutzt packets-Name weiter
            "fetch_config": cfg,
            "status_json_debug": status_json_debug,
        },
    )
