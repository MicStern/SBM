from collections import deque


class Status:
    def __init__(self):
        # runtime stats (epoch seconds floats)
        self.started_at = None
        self.last_fetch_at = None
        self.last_save_at = None

        self.fetched_total = 0
        self.saved_total = 0
        self.fetch_errors = 0
        self.save_errors = 0

        # rolling logs
        self.error_logs = deque(maxlen=200)

        # fetcher state
        self.fetch_enabled = False
        self.fetch_started_at = None  # epoch seconds (utc)
        self.fetch_cursor_utc = None  # ISO string with tz, e.g. "2026-01-05T10:00:00+00:00"
        self.fetch_window_sec = 300
        self.fetch_poll_sec = 30


status = Status()
