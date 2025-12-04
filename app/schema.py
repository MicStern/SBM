from pydantic import BaseModel
from typing import Any, Dict

class IncomingItem(BaseModel):
    info: str
    data: Dict[str, Any]  # enthält u.a. dateTime, labelUUID, etc.

class LabelUpdate(BaseModel):
    packet_id: str
    label: str | None = None
