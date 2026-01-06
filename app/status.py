import time
from collections import deque
from typing import Deque, Optional


class Status:
    def __init__(self):
        self.started_at = time.time()
        self.last_fetch_at: Optional[float] = None
        self.last_save_at: Optional[float] = None
        self.fetched_total = 0
        self.saved_total = 0
        self.fetch_errors = 0
        self.save_errors = 0

        self.error_logs: Deque[str] = deque(maxlen=200)

        # Fetch-Steuerung (über /fetch/start gesetzt)
        self.fetch_cursor_iso: Optional[str] = None
        self.fetch_enabled: bool = False

    async def inc(self, key: str, by: int = 1):
        setattr(self, key, int(getattr(self, key, 0)) + by)

    async def set_time(self, key: str):
        setattr(self, key, time.time())

    async def log_error(self, msg: str):
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        self.error_logs.appendleft(f"{ts}  {msg}")


status = Status()
