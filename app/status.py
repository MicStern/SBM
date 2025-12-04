import asyncio
from dataclasses import dataclass, field
from time import time
from typing import Optional, List

MAX_ERROR_LOGS = 20


@dataclass
class Status:
    started_at: float = field(default_factory=time)
    last_fetch_at: Optional[float] = None
    last_save_at: Optional[float] = None
    fetched_total: int = 0
    saved_total: int = 0
    fetch_errors: int = 0
    save_errors: int = 0

    # neue Fehlerliste
    error_logs: List[str] = field(default_factory=list)

    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    async def inc(self, attr: str, value: int = 1):
        async with self._lock:
            setattr(self, attr, getattr(self, attr) + value)

    async def set_time(self, attr: str):
        async with self._lock:
            setattr(self, attr, time())

    async def log_error(self, msg: str):
        async with self._lock:
            self.error_logs.append(msg)
            if len(self.error_logs) > MAX_ERROR_LOGS:
                self.error_logs = self.error_logs[-MAX_ERROR_LOGS:]


# WICHTIG: globale Instanz, auf die main/fetcher/storage zugreifen
status = Status()
