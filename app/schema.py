from pydantic import BaseModel, ConfigDict


class IncomingItem(BaseModel):
    """
    Neue API liefert flache Objekte.
    Wir erlauben erstmal alles (extra=allow), damit Änderungen der API
    nicht sofort unser Parsing brechen.
    """
    model_config = ConfigDict(extra="allow")
