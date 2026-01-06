from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import select, literal, case, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .db import SessionLocal
from .models import Measurement, MeasurementGroup
from .settings import settings
from .status import status


def _parse_ts(ts: Optional[str]) -> Optional[datetime]:
    """
    API liefert "YYYY-MM-DD HH:MM:SS" ohne TZ.
    Wir speichern als naive datetime (TIMESTAMP ohne tz).
    """
    if not ts:
        return None
    try:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


async def save_item(item: dict) -> None:
    """
    Speichert genau einen Messpunkt:
    1) measurements insert (on conflict do nothing)
    2) falls inserted: measurement_groups upsert + append id (dedupe)
    """
    label_uid = item.get("label_uid")
    if not label_uid:
        await status.inc("save_errors")
        await status.log_error("SAVE ERROR: label_uid missing")
        return

    serial = item.get("serial") or ""

    ts_sensor = _parse_ts(item.get("timestamp_sensor_iso"))

    values = {
        "label_uid": str(label_uid),
        "serial": str(serial),
        "timestamp_sensor": ts_sensor,
        "probe_id": item.get("probe_id"),
        "measurementid": item.get("measurementid"),
        "temp_a": item.get("temp_a"),
        "temp_b": item.get("temp_b"),
        "temp_c": item.get("temp_c"),
        "temp_d": item.get("temp_d"),
        "weight_a": item.get("weight_a"),
        "weight_b": item.get("weight_b"),
        "weight_c": item.get("weight_c"),
        "weight_d": item.get("weight_d"),
        "rawstrain_a": item.get("rawstrain_a"),
        "rawstrain_b": item.get("rawstrain_b"),
        "rawstrain_c": item.get("rawstrain_c"),
        "rawstrain_d": item.get("rawstrain_d"),
        "label": item.get("label"),
        "label_cnt": item.get("label_cnt"),
        "systemstate": item.get("systemstate"),
        "debugsw1": item.get("debugsw1"),
        "debugsw2": item.get("debugsw2"),
        "debugval1": item.get("debugval1"),
    }

    async with SessionLocal() as session:
        try:
            # 1) Insert measurement (dupe-safe)
            stmt = (
                pg_insert(Measurement)
                .values(**values)
                .on_conflict_do_nothing(index_elements=["serial", "probe_id"])
                .returning(Measurement.id)
            )

            res = await session.execute(stmt)
            new_id = res.scalar_one_or_none()

            # Wenn Conflict -> keine neue Zeile -> keine Gruppenänderung nötig
            if new_id is None:
                await session.commit()
                return

            # 2) Upsert group: append ID wenn nicht vorhanden
            # exists check via "= ANY(measurement_ids)"
            exists_expr = literal(new_id).op("= ANY")(MeasurementGroup.measurement_ids)

            upd_measurement_ids = case(
                (exists_expr, MeasurementGroup.measurement_ids),
                else_=func.array_append(MeasurementGroup.measurement_ids, literal(new_id)),
            )

            grp_stmt = (
                pg_insert(MeasurementGroup)
                .values(label_uid=str(label_uid), measurement_ids=[new_id])
                .on_conflict_do_update(
                    index_elements=[MeasurementGroup.label_uid],
                    set_={"measurement_ids": upd_measurement_ids},
                )
            )

            await session.execute(grp_stmt)
            await session.commit()

            await status.inc("saved_total")
            await status.set_time("last_save_at")

        except Exception as e:
            await session.rollback()
            await status.inc("save_errors")
            await status.log_error(f"SAVE ERROR: {repr(e)}")
            raise
