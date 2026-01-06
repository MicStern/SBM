from hashlib import sha1
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import case, literal
from sqlalchemy.sql import any_

from .db import SessionLocal
from .models import Measurement, MeasurementGroup
from .status import status


BERLIN_TZ = ZoneInfo("Europe/Berlin")


def _make_external_id(payload: dict) -> str:
    """
    Eindeutiger Schlüssel zur Dublettenunterdrückung.
    Robust: serial + timestamp_sensor_iso + probe_id + label_uid
    """
    serial = str(payload.get("serial") or "")
    ts = str(payload.get("timestamp_sensor_iso") or "")
    probe_id = str(payload.get("probe_id") or "")
    label_uid = str(payload.get("label_uid") or "")
    base = f"{serial}|{ts}|{probe_id}|{label_uid}"
    return sha1(base.encode("utf-8")).hexdigest()


def _parse_ts_sensor_iso(ts: str | None):
    """
    API liefert: "YYYY-MM-DD HH:MM:SS" (ohne TZ).
    Wir interpretieren das als Europe/Berlin und speichern tz-aware.
    """
    if not ts:
        return None
    try:
        dt_naive = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        return dt_naive.replace(tzinfo=BERLIN_TZ)
    except Exception:
        return None


def _to_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


async def save_item(item: dict):
    """
    Speichert ein einzelnes Measurement.
    Erwartet item == JSON-Objekt (wie API liefert).
    - measurements: alle Felder in Spalten
    - measurement_groups: label_uid + label + measurement_ids[] (append)
    """
    payload = item or {}

    external_id = _make_external_id(payload)
    label_uid = payload.get("label_uid")
    label = payload.get("label")

    timestamp_sensor_iso = payload.get("timestamp_sensor_iso")
    timestamp_sensor = _parse_ts_sensor_iso(timestamp_sensor_iso)

    async with SessionLocal() as session:
        try:
            # 1) Measurement insert (dedupe über external_id unique)
            stmt = (
                pg_insert(Measurement)
                .values(
                    external_id=external_id,
                    serial=payload.get("serial"),
                    timestamp_sensor_iso=timestamp_sensor_iso,
                    timestamp_sensor=timestamp_sensor,
                    probe_id=payload.get("probe_id"),

                    temp_a=_to_float(payload.get("temp_a"), None),
                    temp_b=_to_float(payload.get("temp_b"), None),
                    temp_c=_to_float(payload.get("temp_c"), None),
                    temp_d=_to_float(payload.get("temp_d"), None),

                    weight_a=_to_float(payload.get("weight_a"), 0.0),
                    weight_b=_to_float(payload.get("weight_b"), 0.0),
                    weight_c=_to_float(payload.get("weight_c"), 0.0),
                    weight_d=_to_float(payload.get("weight_d"), 0.0),

                    rawstrain_a=payload.get("rawstrain_a") or [],
                    rawstrain_b=payload.get("rawstrain_b") or [],
                    rawstrain_c=payload.get("rawstrain_c") or [],
                    rawstrain_d=payload.get("rawstrain_d") or [],

                    label_uid=label_uid,
                    label=label,
                    label_cnt=payload.get("label_cnt"),
                    measurementid=payload.get("measurementid"),
                    systemstate=payload.get("systemstate"),
                    debugsw1=payload.get("debugsw1"),
                    debugsw2=payload.get("debugsw2"),
                    debugval1=payload.get("debugval1"),
                )
                .on_conflict_do_nothing(index_elements=[Measurement.external_id])
                .returning(Measurement.id)
            )

            res = await session.execute(stmt)
            measurement_id = res.scalar()

            # Wenn Dublette: nichts zu tun, aber Status trotzdem sauber
            if measurement_id is None:
                await session.commit()
                return

            # 2) Group upsert + measurement_id append (ohne func.case!)
            if label_uid:
                grp_stmt = (
                    pg_insert(MeasurementGroup)
                    .values(
                        label_uid=label_uid,
                        label=label,
                        measurement_ids=[measurement_id],
                    )
                    .on_conflict_do_update(
                        index_elements=[MeasurementGroup.label_uid],
                        set_={
                            # label immer mitziehen
                            "label": label,

                            # append wenn nicht vorhanden
                            "measurement_ids": case(
                                (
                                    literal(measurement_id)
                                    == any_(MeasurementGroup.measurement_ids),
                                    MeasurementGroup.measurement_ids,
                                ),
                                else_=MeasurementGroup.measurement_ids + [measurement_id],
                            ),
                        },
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
