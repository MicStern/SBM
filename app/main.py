import asyncio
from datetime import datetime

from fastapi import FastAPI, Request, Form
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
)
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .analysis import analysis_loop
from .db import SessionLocal, engine
from .fetcher import fetch_loop
from .models import Base, Record, PacketLabel, FetchConfig
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


async def update_fetch_config(**kwargs) -> None:
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
            "fetch_config": {
                "enabled": cfg.enabled,
                "cursor": cfg.cursor.isoformat() if cfg.cursor else None,
                "window_seconds": cfg.window_seconds,
                "poll_seconds": cfg.poll_seconds,
            },
            # optional debug keys, wenn status.set() existiert:
            "debug": getattr(status, "debug", None),
        }
    )


@app.post("/fetch/start")
async def fetch_start(
    start_dt: str = Form(...),
    window_seconds: int = Form(300),
    poll_seconds: int = Form(30),
):
    """
    Startzeit setzen und Fetch aktivieren.
    datetime-local liefert keine TZ -> wir lassen es bewusst NAIV,
    weil die API genau 'YYYY-MM-DD HH:MM:SS' ohne TZ erwartet.
    """
    dt = datetime.fromisoformat(start_dt)
    dt = dt.replace(tzinfo=None)

    await update_fetch_config(
        enabled=True,
        cursor=dt,
        window_seconds=int(window_seconds),
        poll_seconds=int(poll_seconds),
    )

    await status.log_error(
        f"FETCH START: cursor={dt.strftime('%Y-%m-%d %H:%M:%S')} window={window_seconds}s poll={poll_seconds}s"
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


@app.get("/packets.json")
async def packets_json():
    async with SessionLocal() as session:
        q = (
            select(
                Record.packet_id,
                func.count(Record.id).label("count"),
                func.max(Record.created_at).label("last_seen"),
                PacketLabel.label,
            )
            .join(PacketLabel, PacketLabel.packet_id == Record.packet_id, isouter=True)
            .group_by(Record.packet_id, PacketLabel.label)
            .order_by(func.max(Record.created_at).desc())
        )
        rows = (await session.execute(q)).all()

    result = []
    for pid, cnt, last_seen, label in rows:
        if pid is None:
            continue
        result.append(
            {
                "packet_id": pid,
                "count": cnt,
                "last_seen": last_seen.isoformat() if last_seen else None,
                "label": label,
            }
        )
    return JSONResponse(result)


@app.post("/label")
async def set_label(packet_id: str = Form(...), label: str = Form("")):
    label_val = label.strip() or None
    async with SessionLocal() as session:
        stmt = pg_insert(PacketLabel).values(packet_id=packet_id, label=label_val)
        stmt = stmt.on_conflict_do_update(
            index_elements=[PacketLabel.packet_id],
            set_={"label": label_val},
        )
        await session.execute(stmt)
        await session.commit()
    return RedirectResponse(url="/", status_code=303)


@app.get("/packet_weights")
async def packet_weights(packet_id: str):
    async with SessionLocal() as session:
        q = (
            select(Record)
            .where(Record.packet_id == packet_id)
            .order_by(Record.created_at.asc())
        )
        records = (await session.execute(q)).scalars().all()

    def get_weight(payload: dict, key: str) -> float:
        v = payload.get(key)
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    frames = []
    for rec in records:
        payload = rec.payload or {}
        ts = (
            payload.get("timestamp_sensor_iso")
            or payload.get("dateTime")
            or (rec.created_at.isoformat() if rec.created_at else None)
        )

        frames.append(
            {
                "timestamp": ts,
                "weightA": get_weight(payload, "weight_a"),
                "weightB": get_weight(payload, "weight_b"),
                "weightC": get_weight(payload, "weight_c"),
                "weightD": get_weight(payload, "weight_d"),
            }
        )

    return JSONResponse({"packet_id": packet_id, "frames": frames})


@app.get("/", response_class=HTMLResponse)
async def status_page(request: Request):
    cfg = await get_or_create_fetch_config()

    async with SessionLocal() as session:
        q = (
            select(
                Record.packet_id,
                func.count(Record.id).label("count"),
                func.max(Record.created_at).label("last_seen"),
                PacketLabel.label,
            )
            .join(PacketLabel, PacketLabel.packet_id == Record.packet_id, isouter=True)
            .group_by(Record.packet_id, PacketLabel.label)
            .order_by(func.max(Record.created_at).desc())
        )
        rows = (await session.execute(q)).all()

    packets = []
    for pid, cnt, last_seen, label in rows:
        if pid is None:
            continue
        packets.append(
            {
                "packet_id": pid,
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
            "packets": packets,
            "fetch_config": cfg,
        },
    )
