# web/utils/safe_stream.py

import asyncio
import logging
from pyrogram.errors import FloodWait, RPCError

log = logging.getLogger(__name__)

async def safe_stream(func, *args, retries=5, **kwargs):
    """
    Safely retry streaming/send/edit operations
    Handles:
    - Request timed out
    - FloodWait
    - Network socket drops
    """

    attempt = 0

    while attempt < retries:
        try:
            return await func(*args, **kwargs)

        except FloodWait as e:
            log.warning(f"FloodWait: sleeping {e.value}s")
            await asyncio.sleep(e.value + 1)

        except asyncio.TimeoutError:
            attempt += 1
            log.warning(f"Timeout during streaming (retry {attempt}/{retries})")
            await asyncio.sleep(3)

        except RPCError as e:
            attempt += 1
            log.warning(f"RPCError: {e} (retry {attempt}/{retries})")
            await asyncio.sleep(3)

        except Exception as e:
            attempt += 1
            log.warning(f"Stream error: {e} (retry {attempt}/{retries})")
            await asyncio.sleep(3)

    log.error("Streaming failed after retries")
    return None
