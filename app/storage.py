from hashlib import sha1

from sqlalchemy.dialects.postgresql import insert as pg_insert

from .db import SessionLocal
from .models import Record
from .settings import settings
from .status import status


def _make_external_id(item: dict) -> str:
    """
    Eindeutiger Schlüssel für einen Datensatz, um Duplikate zu vermeiden.

    Jetzt: probe_id + timestamp_sensor_iso + label_uid (+ serial als safety)
    """
    serial = str(item.get("serial") or "")
    probe_id = str(item.get("probe_id") or "")
    ts = str(item.get("timestamp_sensor_iso") or "")
    pid = str(item.get(settings.GROUP_KEY_NAME) or "")
    base = f"{serial}|{probe_id}|{ts}|{pid}"
    return sha1(base.encode("utf-8")).hexdigest()


def _extract_packet_id(item: dict) -> str | None:
    """
    Gruppierungs-ID (packet_id) z.B. label_uid.
    """
    return item.get(settings.GROUP_KEY_NAME)


async def save_item(item: dict):
    """
    Speichert einen Datensatz in der DB.
    Dubletten werden über external_id unterdrückt.
    """
    external_id = _make_external_id(item)
    packet_id = _extract_packet_id(item)

    # payload ist jetzt das gesamte Objekt
    payload = item

    async with SessionLocal() as session:
        try:
            stmt = (
                pg_insert(Record)
                .values(
                    external_id=external_id,
                    packet_id=packet_id,
                    payload=payload,
                )
                .on_conflict_do_nothing(index_elements=[Record.external_id])
            )

            await session.execute(stmt)
            await session.commit()

            await status.inc("saved_total")
            await status.set_time("last_save_at")

        except Exception as e:
            await status.inc("save_errors")
            await status.log_error(f"SAVE ERROR: {repr(e)}")
            raise
