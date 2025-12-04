import asyncio
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .settings import settings
from .status import status
from .fetcher import fetch_loop
from .storage import save_item
from .db import SessionLocal, engine
from .models import Base, Record, PacketLabel
from .analysis import analysis_loop

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")

queue: asyncio.Queue | None = None
analysis_result: dict = {}

@app.on_event("startup")
async def startup():
    global queue
    queue = asyncio.Queue(maxsize=settings.QUEUE_MAXSIZE)
    # DB Tabellen sicherstellen (für schnelles Starten; in Prod via Alembic migrieren)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Loops
    asyncio.create_task(fetch_loop(queue))
    for _ in range(settings.SAVE_CONCURRENCY):
        asyncio.create_task(saver_loop(queue))
    asyncio.create_task(analysis_loop(analysis_result))

async def saver_loop(queue: asyncio.Queue):
    while True:
        item = await queue.get()
        try:
            await save_item(item)
        finally:
            queue.task_done()

@app.get("/healthz")
async def healthz():
    return PlainTextResponse("ok")

@app.get("/status.json")
async def status_json():
    return JSONResponse({
        "started_at": status.started_at,
        "last_fetch_at": status.last_fetch_at,
        "last_save_at": status.last_save_at,
        "fetched_total": status.fetched_total,
        "saved_total": status.saved_total,
        "fetch_errors": status.fetch_errors,
        "save_errors": status.save_errors,
        "queue_size": queue.qsize() if queue else 0,
        "analysis": analysis_result,
    })

@app.get("/packets.json")
async def packets_json():
    async with SessionLocal() as session:
        q = (
            select(
                Record.packet_id,
                func.count(Record.id).label("count"),
                func.max(Record.created_at).label("last_seen"),
                PacketLabel.label
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
        result.append({
            "packet_id": pid,
            "count": cnt,
            "last_seen": last_seen.isoformat() if last_seen else None,
            "label": label
        })
    return JSONResponse(result)

@app.post("/label")
async def set_label(packet_id: str = Form(...), label: str = Form("")):
    label_val = label.strip() or None
    async with SessionLocal() as session:
        stmt = pg_insert(PacketLabel).values(packet_id=packet_id, label=label_val)
        stmt = stmt.on_conflict_do_update(
            index_elements=[PacketLabel.packet_id],
            set_={"label": label_val}
        )
        await session.execute(stmt)
        await session.commit()
    return RedirectResponse(url="/", status_code=303)

@app.get("/", response_class=HTMLResponse)
async def status_page(request: Request):
    async with SessionLocal() as session:
        q = (
            select(
                Record.packet_id,
                func.count(Record.id).label("count"),
                func.max(Record.created_at).label("last_seen"),
                PacketLabel.label
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
        packets.append({
            "packet_id": pid,
            "count": cnt,
            "last_seen": last_seen.isoformat() if last_seen else None,
            "label": label
        })

    return templates.TemplateResponse("status.html", {
        "request": request,
        "s": status,
        "queue_size": queue.qsize() if queue else 0,
        "analysis": analysis_result,
        "packets": packets
    })
