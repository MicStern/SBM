from __future__ import annotations

from hashlib import sha1
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select, func, case, cast
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY

from .db import SessionLocal
from .models import Measurement, MeasurementGroup
from .status import status


BERLIN = ZoneInfo("Europe/Berlin")


def _parse_timestamp_sensor_iso(ts: str) -> datetime:
    """
    API liefert z.B. "2026-01-05 15:25:48" (ohne TZ).
    Wir interpretieren das als Europe/Berlin und speichern timezone-aware.
    """
    # robust: trim
    ts = (ts or "").strip()
    dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=BERLIN)
    return dt.astimezone(timezone.utc)  # intern sauber in UTC


def _make_external_id(payload: dict) -> str:
    """
    Eindeutiger Schlüssel pro Messpunkt:
    serial|timestamp_sensor_iso|probe_id|label_uid
    """
    serial = str(payload.get("serial") or "")
    ts = str(payload.get("timestamp_sensor_iso") or "")
    probe_id = str(payload.get("probe_id") or "")
    label_uid = str(payload.get("label_uid") or "")
    base = f"{serial}|{ts}|{probe_id}|{label_uid}"
    return sha1(base.encode("utf-8")).hexdigest()


def _f(payload: dict, key: str) -> float | None:
    v = payload.get(key, None)
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _i(payload: dict, key: str) -> int | None:
    v = payload.get(key, None)
    try:
        if v is None:
            return None
        return int(v)
    except (TypeError, ValueError):
        return None


async def save_item(item: dict):
    """
    item ist ein einzelnes Objekt aus der API-Liste (direkt payload),
    also z.B.:
    {
      "serial": "...",
      "timestamp_sensor_iso": "...",
      ...
      "label_uid": "...",
      "label": "..."
    }
    """
    payload = item or {}

    label_uid = (payload.get("label_uid") or "").strip()
    if not label_uid:
        await status.inc("save_errors")
        await status.log_error("SAVE ERROR: missing label_uid")
        return

    serial = (payload.get("serial") or "").strip()
    ts_raw = (payload.get("timestamp_sensor_iso") or "").strip()
    if not serial or not ts_raw:
        await status.inc("save_errors")
        await status.log_error("SAVE ERROR: missing serial/timestamp_sensor_iso")
        return

    try:
        ts = _parse_timestamp_sensor_iso(ts_raw)
    except Exception as e:
        await status.inc("save_errors")
        await status.log_error(f"SAVE ERROR: bad timestamp_sensor_iso {ts_raw!r} -> {repr(e)}")
        return

    external_id = _make_external_id(payload)

    # optional: label aus API (kann None/leer sein)
    label_val = (payload.get("label") or "").strip() or None

    async with SessionLocal() as session:
        try:
            # 1) Measurement upsert (do nothing on duplicate)
            m_stmt = (
                pg_insert(Measurement)
                .values(
                    external_id=external_id,
                    label_uid=label_uid,
                    serial=serial,
                    timestamp_sensor=ts,
                    probe_id=_i(payload, "probe_id"),
                    temp_a=_f(payload, "temp_a"),
                    temp_b=_f(payload, "temp_b"),
                    temp_c=_f(payload, "temp_c"),
                    temp_d=_f(payload, "temp_d"),
                    weight_a=_f(payload, "weight_a"),
                    weight_b=_f(payload, "weight_b"),
                    weight_c=_f(payload, "weight_c"),
                    weight_d=_f(payload, "weight_d"),
                    rawstrain_a=payload.get("rawstrain_a"),
                    rawstrain_b=payload.get("rawstrain_b"),
                    rawstrain_c=payload.get("rawstrain_c"),
                    rawstrain_d=payload.get("rawstrain_d"),
                    label=label_val,
                    label_cnt=_i(payload, "label_cnt"),
                    measurementid=_i(payload, "measurementid"),
                    systemstate=(payload.get("systemstate") if payload.get("systemstate") is not None else None),
                    debugsw1=_i(payload, "debugsw1"),
                    debugsw2=_i(payload, "debugsw2"),
                    debugval1=(str(payload.get("debugval1")) if payload.get("debugval1") is not None else None),
                )
                .on_conflict_do_nothing(index_elements=[Measurement.external_id])
                .returning(Measurement.id)
            )

            res = await session.execute(m_stmt)
            new_id = res.scalar_one_or_none()

            # Wenn duplicate: id holen
            if new_id is None:
                q = select(Measurement.id).where(Measurement.external_id == external_id)
                new_id = (await session.execute(q)).scalar_one()

            # 2) MeasurementGroup upsert + measurement_ids append (falls noch nicht drin)
            #    Fix für deinen SyntaxError: ANY(...) braucht Klammern.
            #    Außerdem nutzen wir coalesce, falls measurement_ids leer ist.
            current_ids = func.coalesce(
                MeasurementGroup.measurement_ids,
                cast([], PG_ARRAY(Measurement.id.type)),
            )

            already_in = (new_id == func.any(current_ids))

            new_ids_expr = case(
                (already_in, current_ids),
                else_=func.array_append(current_ids, cast(new_id, Measurement.id.type)),
            )

            grp_values = {
                "label_uid": label_uid,
                "measurement_ids": [new_id],
            }
            # label in group initial setzen, wenn vorhanden
            if label_val is not None:
                grp_values["label"] = label_val

            grp_stmt = pg_insert(MeasurementGroup).values(**grp_values)

            # beim conflict:
            # - measurement_ids appenden (wenn nicht drin)
            # - label updaten, wenn label_val vorhanden (sonst beibehalten)
            set_map = {"measurement_ids": new_ids_expr}
            if label_val is not None:
                set_map["label"] = label_val

            grp_stmt = grp_stmt.on_conflict_do_update(
                index_elements=[MeasurementGroup.label_uid],
                set_=set_map,
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
