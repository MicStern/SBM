from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import BigInteger, String, DateTime, func, Integer, Float, Text
from sqlalchemy.dialects.postgresql import ARRAY


class Base(DeclarativeBase):
    pass


class Measurement(Base):
    __tablename__ = "measurements"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Gruppierung
    label_uid: Mapped[str] = mapped_column(String(128), index=True, nullable=False)

    # Datenfelder (finales JSON)
    serial: Mapped[str | None] = mapped_column(String(64), index=True)

    # ✅ parsed + normalized timestamp (UTC, aware)
    # (ich lasse nullable=False, weil dein Visualizer / Queries auf Zeit basieren)
    timestamp_sensor: Mapped[object] = mapped_column(DateTime(timezone=True), index=True, nullable=False)

    # ✅ RAW timestamp string aus Theta (z.B. "2026-01-05 15:25:48")
    timestamp_sensor_iso: Mapped[str | None] = mapped_column(String(32), index=True, nullable=True)

    probe_id: Mapped[int | None] = mapped_column(Integer)

    temp_a: Mapped[float | None] = mapped_column(Float)
    temp_b: Mapped[float | None] = mapped_column(Float)
    temp_c: Mapped[float | None] = mapped_column(Float)
    temp_d: Mapped[float | None] = mapped_column(Float)

    weight_a: Mapped[float | None] = mapped_column(Float)
    weight_b: Mapped[float | None] = mapped_column(Float)
    weight_c: Mapped[float | None] = mapped_column(Float)
    weight_d: Mapped[float | None] = mapped_column(Float)

    rawstrain_a: Mapped[list[int] | None] = mapped_column(ARRAY(Integer))
    rawstrain_b: Mapped[list[int] | None] = mapped_column(ARRAY(Integer))
    rawstrain_c: Mapped[list[int] | None] = mapped_column(ARRAY(Integer))
    rawstrain_d: Mapped[list[int] | None] = mapped_column(ARRAY(Integer))

    label: Mapped[str | None] = mapped_column(String(200))
    label_cnt: Mapped[int | None] = mapped_column(Integer)
    measurementid: Mapped[int | None] = mapped_column(Integer)

    systemstate: Mapped[str | None] = mapped_column(Text)
    debugsw1: Mapped[int | None] = mapped_column(Integer)
    debugsw2: Mapped[int | None] = mapped_column(Integer)
    debugval1: Mapped[str | None] = mapped_column(String(200))

    # DB meta
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class MeasurementGroup(Base):
    __tablename__ = "measurement_groups"

    label_uid: Mapped[str] = mapped_column(String(128), primary_key=True)

    # Liste der Measurement-IDs in dieser Gruppe
    measurement_ids: Mapped[list[int]] = mapped_column(ARRAY(BigInteger), nullable=False, server_default="{}")

    # Label zur Übersicht (wie du wolltest)
    label: Mapped[str | None] = mapped_column(String(200), nullable=True)


class WatchMeasurementGroup(Base):
    __tablename__ = "watch_measurement_groups"

    label_uid: Mapped[str] = mapped_column(String(128), primary_key=True, index=True)
    watch_uuid: Mapped[str | None] = mapped_column(String(128), index=True, nullable=True)

    # Liste der WatchMeasurement-IDs in dieser Gruppe
    measurement_ids: Mapped[list[int]] = mapped_column(ARRAY(BigInteger), nullable=False, server_default="{}")

    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class WatchMeasurement(Base):
    __tablename__ = "watch_measurements"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Gruppierung / Zuordnung
    label_uid: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    watch_uuid: Mapped[str | None] = mapped_column(String(128), index=True, nullable=True)

    # Meta der einzelnen Watch-Messung
    received_at_ms: Mapped[int | None] = mapped_column(BigInteger, index=True, nullable=True)
    sensor_time_ms: Mapped[int | None] = mapped_column(BigInteger, index=True, nullable=True)
    sensor_name: Mapped[str | None] = mapped_column(String(64), index=True, nullable=True)
    sensor_tag: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Wertefelder
    seqno: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    green: Mapped[float | None] = mapped_column(Float, nullable=True)
    red: Mapped[float | None] = mapped_column(Float, nullable=True)
    infra_red: Mapped[float | None] = mapped_column(Float, nullable=True)

    hr: Mapped[float | None] = mapped_column(Float, nullable=True)
    hr_conf: Mapped[float | None] = mapped_column(Float, nullable=True)

    spo2: Mapped[float | None] = mapped_column(Float, nullable=True)
    spo2_conf: Mapped[float | None] = mapped_column(Float, nullable=True)

    eda: Mapped[float | None] = mapped_column(Float, nullable=True)
    gsr: Mapped[str | None] = mapped_column(String(64), nullable=True)

    acc_x: Mapped[float | None] = mapped_column(Float, nullable=True)
    acc_y: Mapped[float | None] = mapped_column(Float, nullable=True)
    acc_z: Mapped[float | None] = mapped_column(Float, nullable=True)

    bosch_acc_x: Mapped[float | None] = mapped_column(Float, nullable=True)
    bosch_acc_y: Mapped[float | None] = mapped_column(Float, nullable=True)
    bosch_acc_z: Mapped[float | None] = mapped_column(Float, nullable=True)

    bosch_gyr_x: Mapped[float | None] = mapped_column(Float, nullable=True)
    bosch_gyr_y: Mapped[float | None] = mapped_column(Float, nullable=True)
    bosch_gyr_z: Mapped[float | None] = mapped_column(Float, nullable=True)

    rr: Mapped[float | None] = mapped_column(Float, nullable=True)
    rr_conf: Mapped[float | None] = mapped_column(Float, nullable=True)
    rr_milli: Mapped[float | None] = mapped_column(Float, nullable=True)

    skin_temperature: Mapped[float | None] = mapped_column(Float, nullable=True)
    ntc_temperature: Mapped[float | None] = mapped_column(Float, nullable=True)
    steps_bosch: Mapped[float | None] = mapped_column(Float, nullable=True)

    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
