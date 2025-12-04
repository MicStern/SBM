from hashlib import sha1
from sqlalchemy.dialects.postgresql import insert as pg_insert
from .db import SessionLocal
from .models import Record
from .status import status
from .settings import settings


def _make_external_id(item: dict) -> str:
    info = item.get("info") or ""
    data = item.get("data") or {}
    dt = str(data.get("dateTime") or "")
    pid = str(data.get(settings.GROUP_KEY_NAME) or "")
    base = f"{info}|{dt}|{pid}"
    return sha1(base.encode("utf-8")).hexdigest()


def _extract_packet_id(item: dict) -> str | None:
    data = item.get("data") or {}
    return data.get(settings.GROUP_KEY_NAME)


async def save_item(item: dict):
    external_id = _make_external_id(item)
    packet_id = _extract_packet_id(item)
    payload = item.get("data") or {}

    async with SessionLocal() as session:
        try:
            stmt = pg_insert(Record).values(
                external_id=external_id,
                packet_id=packet_id,
                payload=payload,
            ).on_conflict_do_nothing(index_elements=[Record.external_id])
            await session.execute(stmt)
            await session.commit()
            await status.inc("saved_total")
            await status.set_time("last_save_at")
        except Exception:
            await status.inc("save_errors")
            raise
