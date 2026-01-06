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
    timestamp_sensor: Mapped[object] = mapped_column(DateTime(timezone=True), index=True, nullable=False)
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
