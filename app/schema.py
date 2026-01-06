from pydantic import BaseModel, ConfigDict


class IncomingItem(BaseModel):
    model_config = ConfigDict(extra="allow")
