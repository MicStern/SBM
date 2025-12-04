import asyncio
from .settings import settings

async def analysis_loop(shared_result: dict):
    # aktuell keine echte Analyse
    while True:
        shared_result.clear()
        shared_result.update({"status": "TBD"})
        await asyncio.sleep(settings.ANALYSIS_INTERVAL_SEC)
