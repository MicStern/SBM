from typing import Any, List, Optional
from pydantic import BaseModel, ConfigDict


class IncomingItem(BaseModel):
    model_config = ConfigDict(extra="allow")

    serial: Optional[str] = None
    timestamp_sensor_iso: Optional[str] = None
    probe_id: Optional[int] = None

    temp_a: Optional[float] = None
    temp_b: Optional[float] = None
    temp_c: Optional[float] = None
    temp_d: Optional[float] = None

    weight_a: Optional[float] = None
    weight_b: Optional[float] = None
    weight_c: Optional[float] = None
    weight_d: Optional[float] = None

    rawstrain_a: Optional[List[int]] = None
    rawstrain_b: Optional[List[int]] = None
    rawstrain_c: Optional[List[int]] = None
    rawstrain_d: Optional[List[int]] = None

    label_uid: Optional[str] = None
    label: Optional[str] = None
    label_cnt: Optional[int] = None

    measurementid: Optional[int] = None
    systemstate: Optional[str] = None

    debugsw1: Optional[int] = None
    debugsw2: Optional[int] = None
    debugval1: Optional[str] = None
