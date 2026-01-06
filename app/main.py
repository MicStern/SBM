import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func

from .analysis import analysis_loop
from .db import SessionLocal, engine
from .fetcher import fetch_loop
from .models import Base, Measurement, MeasurementGroup
from .settings import settings
from .status import status
from .storage import save_item, update_label_for_group

UTC = ZoneInfo("UTC")
BERLIN = ZoneInfo("Europe/Berlin")

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

    asyncio.create_task(analysis_loop(analysis_result))


@app.get("/healthz")
async def healthz():
    return PlainTextResponse("ok")


@app.post("/fetch/start")
async def fetch_start(start_dt: str = Form(...)):
    """
    start_dt kommt aus <input type="datetime-local"> als z.B. "2025-12-10T08:00"
    Wir interpretieren als Europe/Berlin und speichern als UTC ISO.
    """
    try:
        dt = datetime.fromisoformat(start_dt)
    except Exception:
        return PlainTextResponse("Invalid datetime", status_code=400)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=BERLIN)
    dt_utc = dt.astimezone(UTC)

    status.fetch_cursor_iso = dt_utc.isoformat()
    status.fetch_enabled = True
    await status.log_error(f"FETCH START: cursor={status.fetch_cursor_iso} window={getattr(settings,'API_WINDOW_SEC',300)}s poll={settings.API_POLL_INTERVAL_SEC}s")
    return RedirectResponse(url="/", status_code=303)


@app.post("/fetch/stop")
async def fetch_stop():
    status.fetch_enabled = False
    await status.log_error("FETCH STOP")
    return RedirectResponse(url="/", status_code=303)


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
            "fetch_enabled": status.fetch_enabled,
            "fetch_cursor_iso": status.fetch_cursor_iso,
        }
    )


@app.post("/label")
async def set_label(label_uid: str = Form(...), label: str = Form("")):
    await update_label_for_group(label_uid, label)
    return RedirectResponse(url="/", status_code=303)


@app.get("/group_weights")
async def group_weights(label_uid: str):
    """
    Frames für Visualizer: nach timestamp_sensor sortiert.
    weight_d kann fehlen -> als 0 (aber in DB ist es Spalte, kann NULL sein)
    """
    async with SessionLocal() as session:
        q = (
            select(Measurement.timestamp_sensor, Measurement.weight_a, Measurement.weight_b, Measurement.weight_c, Measurement.weight_d)
            .where(Measurement.label_uid == label_uid)
            .order_by(Measurement.timestamp_sensor.asc())
        )
        rows = (await session.execute(q)).all()

    def nz(x):
        try:
            return float(x)
        except Exception:
            return 0.0

    frames = []
    for ts, wa, wb, wc, wd in rows:
        frames.append(
            {
                "timestamp": ts.isoformat() if ts else None,
                "weight_a": nz(wa),
                "weight_b": nz(wb),
                "weight_c": nz(wc),
                "weight_d": nz(wd),
            }
        )

    return JSONResponse({"label_uid": label_uid, "frames": frames})


@app.get("/", response_class=HTMLResponse)
async def status_page(request: Request):
    """
    Gruppenseite:
    - groups aus measurement_groups
    - count/last_seen aus measurements aggregiert
    """
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
    for label_uid, cnt, last_seen, label in rows:
        groups.append(
            {
                "label_uid": label_uid,
                "count": int(cnt or 0),
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
