from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .db import SessionLocal
from .models import Measurement, MeasurementGroup, WatchMeasurement, WatchMeasurementGroup
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


def _safe_int(value):
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def _safe_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _watch_values_to_row(values: list[dict]) -> dict:
    row = {}

    for entry in values or []:
        key = entry.get("name")
        val = entry.get("value")

        if key == "seqno":
            row["seqno"] = _safe_int(val)

        elif key == "green":
            row["green"] = _safe_float(val)
        elif key == "red":
            row["red"] = _safe_float(val)
        elif key == "infra_red":
            row["infra_red"] = _safe_float(val)

        elif key == "hr":
            row["hr"] = _safe_float(val)
        elif key == "hr_conf":
            row["hr_conf"] = _safe_float(val)

        elif key == "spo2":
            row["spo2"] = _safe_float(val)
        elif key == "spo2_conf":
            row["spo2_conf"] = _safe_float(val)

        elif key == "eda":
            row["eda"] = _safe_float(val)
        elif key == "gsr":
            row["gsr"] = str(val) if val is not None else None

        elif key == "acc_x":
            row["acc_x"] = _safe_float(val)
        elif key == "acc_y":
            row["acc_y"] = _safe_float(val)
        elif key == "acc_z":
            row["acc_z"] = _safe_float(val)

        elif key == "bosch_acc_x":
            row["bosch_acc_x"] = _safe_float(val)
        elif key == "bosch_acc_y":
            row["bosch_acc_y"] = _safe_float(val)
        elif key == "bosch_acc_z":
            row["bosch_acc_z"] = _safe_float(val)

        elif key == "bosch_gyr_x":
            row["bosch_gyr_x"] = _safe_float(val)
        elif key == "bosch_gyr_y":
            row["bosch_gyr_y"] = _safe_float(val)
        elif key == "bosch_gyr_z":
            row["bosch_gyr_z"] = _safe_float(val)

        elif key == "rr":
            row["rr"] = _safe_float(val)
        elif key == "rr_conf":
            row["rr_conf"] = _safe_float(val)
        elif key == "rr_milli":
            row["rr_milli"] = _safe_float(val)

        elif key == "skin_temperature":
            row["skin_temperature"] = _safe_float(val)
        elif key == "ntc_temperature":
            row["ntc_temperature"] = _safe_float(val)
        elif key == "steps_bosch":
            row["steps_bosch"] = _safe_float(val)

    return row


async def save_watch_packet_list(label_uid: str, packet_list: list[dict]):
    """
    packet_list:
    [
      {
        "received_at_ms": ...,
        "uuid": "...",
        "payload": { "dataList": [...] }
      },
      ...
    ]
    """
    if not label_uid:
        raise ValueError("missing label_uid for watch data")

    async with SessionLocal() as session:
        inserted_ids = []
        watch_uuid_for_group = None

        for packet in packet_list or []:
            received_at_ms = _safe_int(packet.get("received_at_ms"))
            watch_uuid = packet.get("uuid")
            if watch_uuid_for_group is None and watch_uuid:
                watch_uuid_for_group = str(watch_uuid)

            payload = packet.get("payload") or {}
            data_list = payload.get("dataList") or []

            for item in data_list:
                values_row = _watch_values_to_row(item.get("values") or [])

                m = WatchMeasurement(
                    label_uid=label_uid,
                    watch_uuid=str(watch_uuid) if watch_uuid is not None else None,
                    received_at_ms=received_at_ms,
                    sensor_time_ms=_safe_int(item.get("time")),
                    sensor_name=item.get("name"),
                    sensor_tag=_safe_int(item.get("tag")),
                    seqno=values_row.get("seqno"),
                    green=values_row.get("green"),
                    red=values_row.get("red"),
                    infra_red=values_row.get("infra_red"),
                    hr=values_row.get("hr"),
                    hr_conf=values_row.get("hr_conf"),
                    spo2=values_row.get("spo2"),
                    spo2_conf=values_row.get("spo2_conf"),
                    eda=values_row.get("eda"),
                    gsr=values_row.get("gsr"),
                    acc_x=values_row.get("acc_x"),
                    acc_y=values_row.get("acc_y"),
                    acc_z=values_row.get("acc_z"),
                    bosch_acc_x=values_row.get("bosch_acc_x"),
                    bosch_acc_y=values_row.get("bosch_acc_y"),
                    bosch_acc_z=values_row.get("bosch_acc_z"),
                    bosch_gyr_x=values_row.get("bosch_gyr_x"),
                    bosch_gyr_y=values_row.get("bosch_gyr_y"),
                    bosch_gyr_z=values_row.get("bosch_gyr_z"),
                    rr=values_row.get("rr"),
                    rr_conf=values_row.get("rr_conf"),
                    rr_milli=values_row.get("rr_milli"),
                    skin_temperature=values_row.get("skin_temperature"),
                    ntc_temperature=values_row.get("ntc_temperature"),
                    steps_bosch=values_row.get("steps_bosch"),
                )
                session.add(m)
                await session.flush()
                inserted_ids.append(m.id)

        if inserted_ids:
            grp_stmt = (
                pg_insert(WatchMeasurementGroup)
                .values(
                    label_uid=label_uid,
                    watch_uuid=watch_uuid_for_group,
                    measurement_ids=inserted_ids,
                )
                .on_conflict_do_update(
                    index_elements=[WatchMeasurementGroup.label_uid],
                    set_={
                        "watch_uuid": watch_uuid_for_group,
                        "measurement_ids": text(
                            """
                            (
                              SELECT ARRAY(
                                SELECT DISTINCT x FROM (
                                  SELECT unnest(watch_measurement_groups.measurement_ids) AS x
                                  UNION ALL
                                  SELECT unnest(CAST(:new_ids AS bigint[])) AS x
                                ) t
                                ORDER BY x
                              )
                            )
                            """
                        ).bindparams(new_ids=inserted_ids),
                    },
                )
            )
            await session.execute(grp_stmt)

        await session.commit()


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
