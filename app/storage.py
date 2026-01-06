from datetime import datetime
from zoneinfo import ZoneInfo
from hashlib import sha1

from sqlalchemy import select, update, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .db import SessionLocal
from .models import Measurement, MeasurementGroup
from .status import status

BERLIN = ZoneInfo("Europe/Berlin")


def _parse_timestamp_sensor_iso(ts: str) -> datetime:
    """
    API liefert z.B. "2026-01-05 15:25:48" (ohne TZ).
    Wir interpretieren das als Europe/Berlin und speichern als UTC (tz-aware).
    """
    dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=BERLIN)
    return dt.astimezone(ZoneInfo("UTC"))


def _make_external_hash(payload: dict) -> str:
    """
    Dedupe-Key: label_uid + serial + timestamp + probe_id
    (robust gegen doppelte API-Fenster)
    """
    base = f"{payload.get('label_uid')}|{payload.get('serial')}|{payload.get('timestamp_sensor_iso')}|{payload.get('probe_id')}"
    return sha1(base.encode("utf-8")).hexdigest()


async def save_item(payload: dict):
    """
    - Insert measurement (falls nicht schon vorhanden)
    - Upsert measurement_groups: measurement_id an ARRAY anhängen (ohne Duplikate)
    - label in group setzen, wenn payload.label existiert (oder später über UI)
    """
    try:
        label_uid = str(payload.get("label_uid") or "").strip()
        if not label_uid:
            await status.inc("save_errors")
            await status.log_error("SAVE ERROR: missing label_uid")
            return

        ts_raw = payload.get("timestamp_sensor_iso")
        if not ts_raw:
            await status.inc("save_errors")
            await status.log_error("SAVE ERROR: missing timestamp_sensor_iso")
            return

        ts = _parse_timestamp_sensor_iso(str(ts_raw))

        # measurement values
        def fnum(key: str) -> float | None:
            v = payload.get(key)
            if v is None:
                return None
            try:
                return float(v)
            except Exception:
                return None

        def inum(key: str) -> int | None:
            v = payload.get(key)
            if v is None:
                return None
            try:
                return int(v)
            except Exception:
                return None

        external_hash = _make_external_hash(payload)

        async with SessionLocal() as session:
            # 1) Insert measurement, but avoid duplicates using a deterministic unique "hash" stored nowhere:
            # We implement dedupe by checking existing row quickly.
            # (Wenn du willst, kann man eine echte external_id-Spalte wieder hinzufügen.)
            exists_q = select(Measurement.id).where(
                Measurement.label_uid == label_uid,
                Measurement.serial == payload.get("serial"),
                Measurement.timestamp_sensor == ts,
                Measurement.probe_id == inum("probe_id"),
            )
            existing_id = (await session.execute(exists_q)).scalar_one_or_none()

            if existing_id is None:
                m = Measurement(
                    label_uid=label_uid,
                    serial=payload.get("serial"),
                    timestamp_sensor=ts,
                    probe_id=inum("probe_id"),
                    temp_a=fnum("temp_a"),
                    temp_b=fnum("temp_b"),
                    temp_c=fnum("temp_c"),
                    temp_d=fnum("temp_d"),
                    weight_a=fnum("weight_a"),
                    weight_b=fnum("weight_b"),
                    weight_c=fnum("weight_c"),
                    weight_d=fnum("weight_d"),
                    rawstrain_a=payload.get("rawstrain_a"),
                    rawstrain_b=payload.get("rawstrain_b"),
                    rawstrain_c=payload.get("rawstrain_c"),
                    rawstrain_d=payload.get("rawstrain_d"),
                    label=payload.get("label"),
                    label_cnt=inum("label_cnt"),
                    measurementid=inum("measurementid"),
                    systemstate=payload.get("systemstate"),
                    debugsw1=inum("debugsw1"),
                    debugsw2=inum("debugsw2"),
                    debugval1=payload.get("debugval1"),
                )
                session.add(m)
                await session.flush()  # damit m.id da ist
                mid = int(m.id)
            else:
                mid = int(existing_id)

            # 2) Upsert measurement group: append mid if not already in array
            # array_position(...) returns NULL if not found
            grp_stmt = pg_insert(MeasurementGroup).values(
                label_uid=label_uid,
                measurement_ids=[mid],
                label=payload.get("label"),
            )
            grp_stmt = grp_stmt.on_conflict_do_update(
                index_elements=[MeasurementGroup.label_uid],
                set_={
                    "measurement_ids": func.case(
                        (
                            func.array_position(MeasurementGroup.measurement_ids, mid) != None,  # noqa: E711
                            MeasurementGroup.measurement_ids,
                        ),
                        else_=func.array_append(MeasurementGroup.measurement_ids, mid),
                    ),
                    # label nur setzen, wenn payload label nicht leer ist
                    "label": func.coalesce(
                        func.nullif(func.cast(payload.get("label"), str), ""),
                        MeasurementGroup.label,
                    ),
                },
            )
            await session.execute(grp_stmt)

            await session.commit()

        await status.inc("saved_total")
        await status.set_time("last_save_at")

    except Exception as e:
        await status.inc("save_errors")
        await status.log_error(f"SAVE ERROR: {repr(e)}")
        raise


async def update_label_for_group(label_uid: str, new_label: str | None):
    """
    Wenn Label in UI geändert wird:
    - measurement_groups.label updaten
    - measurements.label für alle in Gruppe updaten
    """
    label_uid = label_uid.strip()
    label_val = (new_label or "").strip() or None

    async with SessionLocal() as session:
        await session.execute(
            update(MeasurementGroup)
            .where(MeasurementGroup.label_uid == label_uid)
            .values(label=label_val)
        )
        await session.execute(
            update(Measurement)
            .where(Measurement.label_uid == label_uid)
            .values(label=label_val)
        )
        await session.commit()
