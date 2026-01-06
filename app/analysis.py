import asyncio
from .settings import settings


async def analysis_loop(state: dict):
    while True:
        try:
            state["ok"] = True
        except Exception:
            pass
        await asyncio.sleep(getattr(settings, "ANALYSIS_INTERVAL_SEC", 300.0))
