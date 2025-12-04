from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import BigInteger, String, JSON, DateTime, func, Index, UniqueConstraint

class Base(DeclarativeBase):
    pass

class Record(Base):
    __tablename__ = "records"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    external_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    packet_id: Mapped[str | None] = mapped_column(String(128), index=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[str] = mapped_column(DateTime(timezone=True), server_default=func.now())


class PacketLabel(Base):
    __tablename__ = "packet_labels"
    packet_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    label: Mapped[str | None] = mapped_column(String(200), nullable=True)
