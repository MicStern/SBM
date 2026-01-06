import asyncio

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func, update

from .analysis import analysis_loop
from .db import SessionLocal, engine
from .fetcher import fetch_loop
from .models import Base, Measurement
from .settings import settings
from .status import status
from .storage import save_item

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")

queue: asyncio.Queue | None = None
analysis_result: dict = {}


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
        }
    )


@app.get("/groups.json")
async def groups_json():
    """
    Gruppiert nach label_uid (Messgruppen-ID).
    """
    async with SessionLocal() as session:
        q = (
            select(
                Measurement.label_uid,
                func.count(Measurement.id).label("count"),
                func.max(Measurement.timestamp_sensor).label("last_seen"),
                func.max(Measurement.label).label("label"),
            )
            .group_by(Measurement.label_uid)
            .order_by(func.max(Measurement.timestamp_sensor).desc())
        )
        rows = (await session.execute(q)).all()

    out = []
    for uid, cnt, last_seen, label in rows:
        out.append(
            {
                "label_uid": uid,
                "count": int(cnt),
                "last_seen": last_seen.isoformat() if last_seen else None,
                "label": label,
            }
        )
    return JSONResponse(out)


@app.post("/label")
async def set_label(label_uid: str = Form(...), label: str = Form("")):
    """
    Label für eine ganze Gruppe setzen:
    UPDATE measurements SET label=... WHERE label_uid=...
    """
    label_val = (label or "").strip() or None

    async with SessionLocal() as session:
        stmt = (
            update(Measurement)
            .where(Measurement.label_uid == label_uid)
            .values(label=label_val)
        )
        await session.execute(stmt)
        await session.commit()

    return RedirectResponse(url="/", status_code=303)


@app.get("/group_weights")
async def group_weights(label_uid: str):
    """
    Liefert alle Frames (Sekunden) für eine label_uid,
    sortiert nach timestamp_sensor, mit weight_a/b/c/d.
    """
    async with SessionLocal() as session:
        q = (
            select(Measurement)
            .where(Measurement.label_uid == label_uid)
            .order_by(Measurement.timestamp_sensor.asc(), Measurement.id.asc())
        )
        rows = (await session.execute(q)).scalars().all()

    frames = []
    for m in rows:
        frames.append(
            {
                "timestamp": m.timestamp_sensor.isoformat() if m.timestamp_sensor else None,
                "weight_a": float(m.weight_a or 0.0),
                "weight_b": float(m.weight_b or 0.0),
                "weight_c": float(m.weight_c or 0.0),
                "weight_d": float(m.weight_d or 0.0),
            }
        )

    return JSONResponse({"label_uid": label_uid, "frames": frames})


@app.get("/", response_class=HTMLResponse)
async def status_page(request: Request):
    """
    HTML-Statusseite: Runtime + Gruppenliste + Logs + Schwerpunkt-Visualizer
    """
    async with SessionLocal() as session:
        q = (
            select(
                Measurement.label_uid,
                func.count(Measurement.id).label("count"),
                func.max(Measurement.timestamp_sensor).label("last_seen"),
                func.max(Measurement.label).label("label"),
            )
            .group_by(Measurement.label_uid)
            .order_by(func.max(Measurement.timestamp_sensor).desc())
        )
        rows = (await session.execute(q)).all()

    groups = []
    for uid, cnt, last_seen, label in rows:
        groups.append(
            {
                "label_uid": uid,
                "count": int(cnt),
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
        },
    )
