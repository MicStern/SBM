from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .db import SessionLocal
from .models import Measurement, MeasurementGroup
from .status import status

BERLIN = ZoneInfo("Europe/Berlin")


def _parse_ts_sensor_iso(ts: str) -> datetime | None:
    """
    Theta JSON liefert: "2026-01-05 15:25:48" (ohne tz)
    Interpretation: lokale Zeit Berlin.
    Wir speichern als aware UTC in timestamp_sensor.
    """
    if not ts:
        return None
    try:
        dt_naive = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        dt_berlin = dt_naive.replace(tzinfo=BERLIN)
        return dt_berlin.astimezone(timezone.utc)
    except Exception:
        return None


async def save_item(item: dict):
    """
    Speichert:
    - measurements: spaltenbasiertes Mapping
    - measurement_groups: label_uid + measurement_ids[] + label (optional)
    """
    try:
        label_uid = item.get("label_uid")
        if not label_uid:
            raise ValueError("missing label_uid")

        ts_iso = item.get("timestamp_sensor_iso")
        ts_sensor = _parse_ts_sensor_iso(ts_iso)

        # ✅ hart validieren, weil timestamp_sensor in DB nullable=False ist
        if ts_sensor is None:
            raise ValueError(f"invalid timestamp_sensor_iso: {ts_iso!r}")

        async with SessionLocal() as session:
            # 1) Measurement insert
            m = Measurement(
                serial=item.get("serial"),
                label_uid=label_uid,
                label=item.get("label"),
                timestamp_sensor=ts_sensor,
                timestamp_sensor_iso=ts_iso,  # ✅ existiert jetzt im Model
                probe_id=item.get("probe_id"),
                measurementid=item.get("measurementid"),
                systemstate=item.get("systemstate"),
                debugsw1=item.get("debugsw1"),
                debugsw2=item.get("debugsw2"),
                debugval1=item.get("debugval1"),
                temp_a=item.get("temp_a"),
                temp_b=item.get("temp_b"),
                temp_c=item.get("temp_c"),
                temp_d=item.get("temp_d"),
                weight_a=item.get("weight_a"),
                weight_b=item.get("weight_b"),
                weight_c=item.get("weight_c"),
                weight_d=item.get("weight_d"),
                rawstrain_a=item.get("rawstrain_a"),
                rawstrain_b=item.get("rawstrain_b"),
                rawstrain_c=item.get("rawstrain_c"),
                rawstrain_d=item.get("rawstrain_d"),
                label_cnt=item.get("label_cnt"),
            )
            session.add(m)
            await session.flush()  # m.id verfügbar

            # 2) Group upsert + ID anhängen (dedup)
            grp_label = item.get("label")
            grp_stmt = (
                pg_insert(MeasurementGroup)
                .values(
                    label_uid=label_uid,
                    label=grp_label,
                    measurement_ids=[m.id],
                )
                .on_conflict_do_update(
                    index_elements=[MeasurementGroup.label_uid],
                    set_={
                        "label": grp_label,
                        "measurement_ids": text(
                            """
                            (
                              SELECT ARRAY(
                                SELECT DISTINCT x FROM (
                                  SELECT unnest(measurement_groups.measurement_ids) AS x
                                  UNION ALL
                                  SELECT :new_id AS x
                                ) t
                                ORDER BY x
                              )
                            )
                            """
                        ).bindparams(new_id=m.id),
                    },
                )
            )
            await session.execute(grp_stmt)

            await session.commit()

        status.saved_total += 1
        status.last_save_at = datetime.now(timezone.utc).timestamp()

    except Exception as e:
        status.save_errors += 1
        status.error_logs.appendleft(f"SAVE ERROR: {repr(e)}")
        raise
