import asyncio
import logging
from pyrogram.errors import RPCError, FloodWait
from functools import wraps

logger = logging.getLogger("connection_manager")

MAX_RETRIES = 5
BACKOFF = 2  # seconds


def retry_on_failure(max_retries=MAX_RETRIES):
    """
    Decorator to retry an async function on connection failures or temporary Telegram errors.
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < max_retries:
                try:
                    return await func(*args, **kwargs)
                except (RPCError, ConnectionError, asyncio.TimeoutError, FloodWait) as e:
                    attempt += 1
                    wait = BACKOFF * attempt
                    logger.warning(f"Connection lost, retry {attempt}/{max_retries} in {wait}s: {e}")
                    await asyncio.sleep(wait)
            raise ConnectionError(f"Failed after {max_retries} retries")
        return wrapper
    return decorator


class ConnectionManager:
    """
    Helper class to manage Pyrogram sessions and safe media downloads.
    Ensures retries on connection lost or temporary Telegram errors.
    """

    def __init__(self, client):
        self.client = client

    @retry_on_failure()
    async def safe_invoke(self, method, *args, **kwargs):
        """
        Invoke a Pyrogram raw function safely with retries.
        """
        return await self.client.invoke(method, *args, **kwargs)

    @retry_on_failure()
    async def safe_download(self, file_location, offset=0, limit=1024*1024):
        """
        Download a chunk of a Telegram file safely.
        """
        return await self.client.send(
            self.client.raw.functions.upload.GetFile(
                location=file_location,
                offset=offset,
                limit=limit
            )
        )

    @retry_on_failure()
    async def safe_call(self, coro, *args, **kwargs):
        """
        Generic safe call for any coroutine.
        Example: wrap Pyrogram methods that may fail.
        """
        return await coro(*args, **kwargs)
