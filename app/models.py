from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import (
    BigInteger,
    String,
    DateTime,
    func,
    Integer,
    Boolean,
    Float,
    UniqueConstraint,
    Index,
)
from sqlalchemy.dialects.postgresql import ARRAY


class Base(DeclarativeBase):
    pass


class MeasurementGroup(Base):
    """
    Minimal wie gewünscht:
    - label_uid (PK)
    - measurement_ids (Liste der IDs der Messpunkte)
    """
    __tablename__ = "measurement_groups"

    label_uid: Mapped[str] = mapped_column(String(64), primary_key=True)
    measurement_ids: Mapped[List[int]] = mapped_column(
        ARRAY(BigInteger), nullable=False, server_default="{}"
    )


class Measurement(Base):
    """
    Eine Zeile pro JSON-Objekt / Messpunkt.
    Alle Werte aus der finalen JSON-Struktur als Spalten.
    """
    __tablename__ = "measurements"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    label_uid: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    serial: Mapped[str] = mapped_column(String(32), nullable=False, index=True)

    # API liefert "YYYY-MM-DD HH:MM:SS" ohne TZ -> wir speichern als TIMESTAMP (ohne tz)
    timestamp_sensor: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True, index=True)

    probe_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    measurementid: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    temp_a: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    temp_b: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    temp_c: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    temp_d: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    weight_a: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    weight_b: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    weight_c: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    weight_d: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    rawstrain_a: Mapped[Optional[list[int]]] = mapped_column(ARRAY(Integer), nullable=True)
    rawstrain_b: Mapped[Optional[list[int]]] = mapped_column(ARRAY(Integer), nullable=True)
    rawstrain_c: Mapped[Optional[list[int]]] = mapped_column(ARRAY(Integer), nullable=True)
    rawstrain_d: Mapped[Optional[list[int]]] = mapped_column(ARRAY(Integer), nullable=True)

    label: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    label_cnt: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    systemstate: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    debugsw1: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    debugsw2: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    debugval1: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)

    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), index=True
    )

    __table_args__ = (
        # Duplikate verhindern:
        # Falls probe_id pro serial eindeutig ist -> gut.
        UniqueConstraint("serial", "probe_id", name="ux_measurements_serial_probe"),
        Index("ix_measurements_label_uid_ts", "label_uid", "timestamp_sensor"),
    )


class FetchConfig(Base):
    """
    Singleton-Konfiguration für Fetch-Loop (Startzeit, Cursor, Fenster, Poll).
    Cursor wird als timestamptz gespeichert.
    """
    __tablename__ = "fetch_config"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)  # immer 1
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    cursor: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    window_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=300)
    poll_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=30)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
