# app/main.py
import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .db import SessionLocal, engine
from .fetcher import fetch_loop, fetch_theta, build_theta_url
from .models import Base, Measurement, MeasurementGroup
from .settings import settings
from .status import status
from .storage import save_item


app = FastAPI()
templates = Jinja2Templates(directory="app/templates")

BERLIN_TZ = ZoneInfo("Europe/Berlin")
THETA_TZ = ZoneInfo(settings.THETA_TIMEZONE)

def ts_berlin(value):
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

        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
            except Exception:
                return value

        return str(value)
    except Exception:
        return str(value)

templates.env.filters["ts_berlin"] = ts_berlin

queue: asyncio.Queue | None = None


def _parse_dt_local_to_utc(dt_local_str: str, tz: ZoneInfo) -> datetime:
    """
    dt_local_str kommt aus <input type="datetime-local"> => "YYYY-MM-DDTHH:MM" (naiv)
    Wir interpretieren in tz und wandeln nach UTC.
    """
    dt_local = datetime.fromisoformat(dt_local_str)  # naive
    dt_zoned = dt_local.replace(tzinfo=tz)
    return dt_zoned.astimezone(timezone.utc)


@app.on_event("startup")
async def startup():
    global queue
    status.started_at = datetime.now(timezone.utc).timestamp()

    queue = asyncio.Queue(maxsize=settings.QUEUE_MAXSIZE)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    asyncio.create_task(fetch_loop(queue))

    async def saver_loop(q: asyncio.Queue):
        while True:
            item = await q.get()
            try:
                await save_item(item)
            finally:
                q.task_done()

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
            "fetch_enabled": status.fetch_enabled,
            "fetch_started_at": status.fetch_started_at,
            "fetch_cursor_utc": status.fetch_cursor_utc,
            "fetch_window_sec": status.fetch_window_sec,
            "fetch_poll_sec": status.fetch_poll_sec,
        }
    )


@app.post("/fetch/start")
async def fetch_start(
    start_dt: str = Form(...),
    window_sec: int = Form(settings.DEFAULT_WINDOW_SEC),
    poll_sec: int = Form(settings.DEFAULT_POLL_SEC),
):
    # UI-Zeit ist Berlin-lokal
    dt_utc = _parse_dt_local_to_utc(start_dt, BERLIN_TZ)

    status.fetch_enabled = True
    status.fetch_started_at = datetime.now(timezone.utc).timestamp()
    status.fetch_cursor_utc = dt_utc.isoformat()
    status.fetch_window_sec = int(window_sec)
    status.fetch_poll_sec = int(poll_sec)

    status.error_logs.appendleft(
        f"FETCH START: cursor_utc={status.fetch_cursor_utc} window={status.fetch_window_sec}s poll={status.fetch_poll_sec}s"
    )
    return RedirectResponse(url="/", status_code=303)


@app.post("/fetch/stop")
async def fetch_stop():
    status.fetch_enabled = False
    status.fetch_started_at = None
    status.error_logs.appendleft("FETCH STOP")
    return RedirectResponse(url="/", status_code=303)


@app.post("/fetch/once")
async def fetch_once(
    start_dt: str = Form(...),
    end_dt: str = Form(...),
):
    """
    Einmaliger Fetch von/bis aus UI.
    UI-Zeit interpretieren wir in der gleichen TZ wie Theta (settings.THETA_TIMEZONE),
    damit '15:00' auch wirklich '15:00' ist wie in Postman.
    """
    start_utc = _parse_dt_local_to_utc(start_dt, THETA_TZ)
    end_utc = _parse_dt_local_to_utc(end_dt, THETA_TZ)

    if end_utc <= start_utc:
        status.error_logs.appendleft("FETCH ONCE ERROR: end <= start")
        return RedirectResponse(url="/", status_code=303)

    url = build_theta_url(start_utc, end_utc)
    status.error_logs.appendleft(f"FETCH ONCE REQ: {url}")

    items = await fetch_theta(start_utc, end_utc)

    saved = 0
    for it in items:
        try:
            await save_item(it)
            saved += 1
        except Exception:
            # save_item loggt selbst in status.error_logs
            pass

    status.error_logs.appendleft(f"FETCH ONCE DONE: fetched={len(items)} saved={saved}")
    return RedirectResponse(url="/", status_code=303)


@app.post("/label")
async def set_label(label_uid: str = Form(...), label: str = Form("")):
    label_val = label.strip() or None

    async with SessionLocal() as session:
        grp_stmt = (
            pg_insert(MeasurementGroup)
            .values(label_uid=label_uid, label=label_val, measurement_ids=[])
            .on_conflict_do_update(
                index_elements=[MeasurementGroup.label_uid],
                set_={"label": label_val},
            )
        )
        await session.execute(grp_stmt)

        await session.execute(
            update(Measurement)
            .where(Measurement.label_uid == label_uid)
            .values(label=label_val)
        )

        await session.commit()

    return RedirectResponse(url="/", status_code=303)


@app.get("/group_frames")
async def group_frames(label_uid: str):
    async with SessionLocal() as session:
        q = (
            select(Measurement)
            .where(Measurement.label_uid == label_uid)
            .order_by(Measurement.timestamp_sensor.asc().nullslast(), Measurement.id.asc())
        )
        rows = (await session.execute(q)).scalars().all()

    frames = []

    def _abs_or_zero(v):
        try:
            return abs(float(v))
        except Exception:
            return 0.0

    for m in rows:
        frames.append(
            {
                "timestamp_sensor_iso": m.timestamp_sensor_iso or None,
                "timestamp": (
                    m.timestamp_sensor.astimezone(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
                    if m.timestamp_sensor
                    else None
                ),
                "weight_a": _abs_or_zero(m.weight_a),
                "weight_b": _abs_or_zero(m.weight_b),
                "weight_c": _abs_or_zero(m.weight_c),
                "weight_d": _abs_or_zero(m.weight_d),
            }
        )

    return JSONResponse({"label_uid": label_uid, "frames": frames})


@app.get("/", response_class=HTMLResponse)
async def status_page(request: Request):
    fetch = {
        "enabled": bool(status.fetch_enabled),
        "cursor_utc": status.fetch_cursor_utc,
        "window_sec": status.fetch_window_sec,
        "poll_sec": status.fetch_poll_sec,
        "started_at": status.fetch_started_at,
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
            "groups": groups,
            "fetch": fetch,
            "theta_timezone": settings.THETA_TIMEZONE,
        },
    )
