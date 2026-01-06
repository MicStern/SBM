import asyncio
from .settings import settings


async def analysis_loop(result: dict):
    while True:
        # TBD – später Analysen rein
        result["ok"] = True
        await asyncio.sleep(getattr(settings, "ANALYSIS_INTERVAL_SEC", 300.0))
