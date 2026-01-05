from __future__ import annotations

from datetime import datetime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import BigInteger, String, JSON, DateTime, func, Boolean, Integer


class Base(DeclarativeBase):
    pass


class Record(Base):
    __tablename__ = "records"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # eindeutig pro Datensatz (Hash), verhindert Dubletten
    external_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)

    # Gruppierung (bei dir: label_uid)
    packet_id: Mapped[str | None] = mapped_column(String(128), index=True)

    # gesamter API-Datensatz als JSON
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class PacketLabel(Base):
    __tablename__ = "packet_labels"

    packet_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    label: Mapped[str | None] = mapped_column(String(200), nullable=True)


class FetchConfig(Base):
    """
    Singleton-Konfiguration (id=1), damit Fetch erst startet, wenn du Startzeit setzt.
    """
    __tablename__ = "fetch_config"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)  # immer 1

    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Cursor für nächste Abfrage
    cursor: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Fensterlänge pro Abfrage (Sekunden)
    window_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=300)

    # Pause zwischen Abfragen (Sekunden)
    poll_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=30)

    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
