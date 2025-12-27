
import asyncio
import logging
from pyrogram.errors import FloodWait, RPCError

log = logging.getLogger(__name__)

# Shared lock to prevent socket flood
_send_lock = asyncio.Lock()

async def send(func, *args, retries=3, delay=2, **kwargs):
    """
    Safely send/forward/edit messages avoiding socket flooding and retry storms.
    Only 3 attempts per call by default.
    """

    async with _send_lock:
        last_exception = None

        for attempt in range(1, retries + 1):
            try:
                return await func(*args, **kwargs)

            except FloodWait as e:
                log.warning(f"FloodWait => Sleeping {e.value}s")
                await asyncio.sleep(e.value + 1)

            except (asyncio.TimeoutError, ConnectionError) as e:
                last_exception = e
                log.warning(f"Connection lost, retry {attempt}/{retries}")
                await asyncio.sleep(delay * attempt)

            except RPCError as e:
                log.error(f"RPCError: {e}")
                break

            except Exception as e:
                last_exception = e
                log.error(f"Unknown send error: {e}")
                break

        log.error("Failed to send after retries")
        return None
