from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import BigInteger, String, JSON, DateTime, func, Integer, Float
from sqlalchemy.dialects.postgresql import ARRAY


class Base(DeclarativeBase):
    pass


class Measurement(Base):
    __tablename__ = "measurements"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Dedupe-Key
    external_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)

    # Group key
    label_uid: Mapped[str] = mapped_column(String(64), index=True, nullable=False)

    # Core fields
    serial: Mapped[str] = mapped_column(String(32), index=True, nullable=False)

    # Sensor timestamp as "real" datetime (timezone-aware)
    timestamp_sensor: Mapped[DateTime] = mapped_column(DateTime(timezone=True), index=True, nullable=False)

    probe_id: Mapped[int | None] = mapped_column(BigInteger, index=True, nullable=True)

    # temps
    temp_a: Mapped[float | None] = mapped_column(Float, nullable=True)
    temp_b: Mapped[float | None] = mapped_column(Float, nullable=True)
    temp_c: Mapped[float | None] = mapped_column(Float, nullable=True)
    temp_d: Mapped[float | None] = mapped_column(Float, nullable=True)

    # weights
    weight_a: Mapped[float | None] = mapped_column(Float, nullable=True)
    weight_b: Mapped[float | None] = mapped_column(Float, nullable=True)
    weight_c: Mapped[float | None] = mapped_column(Float, nullable=True)
    weight_d: Mapped[float | None] = mapped_column(Float, nullable=True)

    # rawstrains (listen → JSON ist am unkompliziertesten)
    rawstrain_a: Mapped[list | None] = mapped_column(JSON, nullable=True)
    rawstrain_b: Mapped[list | None] = mapped_column(JSON, nullable=True)
    rawstrain_c: Mapped[list | None] = mapped_column(JSON, nullable=True)
    rawstrain_d: Mapped[list | None] = mapped_column(JSON, nullable=True)

    # label data (keine separate label-tabelle)
    label: Mapped[str | None] = mapped_column(String(200), nullable=True)
    label_cnt: Mapped[int | None] = mapped_column(Integer, nullable=True)

    measurementid: Mapped[int | None] = mapped_column(Integer, nullable=True)
    systemstate: Mapped[str | None] = mapped_column(String(64), nullable=True)

    debugsw1: Mapped[int | None] = mapped_column(Integer, nullable=True)
    debugsw2: Mapped[int | None] = mapped_column(Integer, nullable=True)
    debugval1: Mapped[str | None] = mapped_column(String(128), nullable=True)

    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class MeasurementGroup(Base):
    __tablename__ = "measurement_groups"

    # group id
    label_uid: Mapped[str] = mapped_column(String(64), primary_key=True)

    # group label (für Übersicht)
    label: Mapped[str | None] = mapped_column(String(200), nullable=True)

    # Liste der Measurement-IDs, die zu dieser Gruppe gehören
    measurement_ids: Mapped[list[int]] = mapped_column(ARRAY(BigInteger), nullable=False, server_default="{}")
