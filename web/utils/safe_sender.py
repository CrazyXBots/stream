import asyncio
import logging
from pyrogram.errors import FloodWait, RPCError

_send_lock = asyncio.Lock()

async def safe_send(func, *args, retries=3, delay=2, **kwargs):
    """
    Safe sender to prevent socket retry storms
    """

    async with _send_lock:
        last_error = None

        for attempt in range(1, retries + 1):
            try:
                return await func(*args, **kwargs)

            except FloodWait as e:
                wait = int(e.value) + 1
                logging.warning(f"FloodWait {wait}s")
                await asyncio.sleep(wait)

            except (asyncio.TimeoutError, ConnectionError) as e:
                last_error = e
                logging.warning(
                    f"Connection lost, retry {attempt}/{retries}"
                )
                await asyncio.sleep(delay * attempt)

            except RPCError as e:
                # Telegram RPC errors – don't spam retries
                logging.error(f"RPCError: {e}")
                break

            except Exception as e:
                last_error = e
                logging.error(f"Send failed: {e}")
                break

        logging.error("Failed to send after retries")
        return None
