import time
from collections import deque

class Status:
    def __init__(self):
        self.started_at = time.time()
        self.last_fetch_at = None
        self.last_save_at = None

        self.fetched_total = 0
        self.saved_total = 0
        self.fetch_errors = 0
        self.save_errors = 0

        self.error_logs = deque(maxlen=200)

    async def set_time(self, field: str):
        setattr(self, field, time.time())

    async def inc(self, field: str, amount: int = 1):
        setattr(self, field, getattr(self, field) + amount)

    async def log_error(self, msg: str):
        self.error_logs.appendleft(msg)

status = Status()
