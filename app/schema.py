from pydantic import BaseModel, ConfigDict


class IncomingItem(BaseModel):
    model_config = ConfigDict(extra="allow")

class WatchUpload(BaseModel):
    label_uid: str
    packets: list[dict]
