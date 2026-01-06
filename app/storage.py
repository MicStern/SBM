from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import func, literal, case
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .db import SessionLocal
from .models import Measurement, MeasurementGroup
from .status import status


def _parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        # API liefert: "YYYY-MM-DD HH:MM:SS"
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


async def save_item(item: dict) -> None:
    """
    Speichert 1 Messpunkt in measurements (dupe-safe über unique(serial, probe_id)).
    Wenn neu inserted: measurement_groups upsert + measurement_id in measurement_ids anhängen (dedupe).
    """
    label_uid = item.get("label_uid")
    if not label_uid:
        await status.inc("save_errors")
        await status.log_error("SAVE ERROR: label_uid missing")
        return

    values = {
        "label_uid": str(label_uid),
        "serial": str(item.get("serial") or ""),
        "timestamp_sensor": _parse_ts(item.get("timestamp_sensor_iso")),
        "probe_id": item.get("probe_id"),

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
        "measurementid": item.get("measurementid"),

        "systemstate": item.get("systemstate"),
        "debugsw1": item.get("debugsw1"),
        "debugsw2": item.get("debugsw2"),
        "debugval1": item.get("debugval1"),
    }

    async with SessionLocal() as session:
        try:
            # 1) Measurement insert (Duplikate unterdrücken)
            stmt = (
                pg_insert(Measurement)
                .values(**values)
                .on_conflict_do_nothing(index_elements=["serial", "probe_id"])
                .returning(Measurement.id)
            )
            res = await session.execute(stmt)
            new_id = res.scalar_one_or_none()

            # Wenn schon vorhanden -> nichts an Gruppen ändern
            if new_id is None:
                await session.commit()
                return

            # 2) Group upsert: append new_id falls nicht enthalten
            # WICHTIG: .any() erzeugt korrekt:  <new_id> = ANY(measurement_ids)
            already_in_group = MeasurementGroup.measurement_ids.any(literal(new_id))

            updated_ids = case(
                (already_in_group, MeasurementGroup.measurement_ids),
                else_=func.array_append(MeasurementGroup.measurement_ids, literal(new_id)),
            )

            grp_stmt = (
                pg_insert(MeasurementGroup)
                .values(label_uid=str(label_uid), measurement_ids=[new_id])
                .on_conflict_do_update(
                    index_elements=[MeasurementGroup.label_uid],
                    set_={"measurement_ids": updated_ids},
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
