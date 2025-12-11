# lifespan.py
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .config import UPLOAD_ROOT, CLEANUP_INTERVAL_SECONDS
from .tickets import cleanup_expired_uploading_tickets


async def cleanup_loop() -> None:
    while True:
        try:
            await cleanup_expired_uploading_tickets()
        except Exception as exc:  # noqa: BLE001
            print("Error during cleanup: %s" % exc)
        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context.
    """
    UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    cleanup_task = asyncio.create_task(cleanup_loop())

    try:
        yield
    finally:
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass
