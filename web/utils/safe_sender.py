import asyncio
import logging
from pyrogram.errors import FloodWait
from pyrogram import Client

log = logging.getLogger(__name__)

_send_lock = asyncio.Lock()


async def safe_send(
    client: Client,
    chat_id: int,
    text: str,
    reply_to_message_id: int | None = None,
    max_retries: int = 6
):
    """
    Safely send message with retry + backoff + connection handling
    """

    async with _send_lock:
        delay = 1

        for attempt in range(1, max_retries + 1):
            try:
                return await client.send_message(
                    chat_id=chat_id,
                    text=text,
                    reply_to_message_id=reply_to_message_id
                )

            except FloodWait as e:
                log.warning(f"FloodWait {e.value}s")
                await asyncio.sleep(e.value + 1)

            except (ConnectionError, OSError, asyncio.TimeoutError) as e:
                log.warning(
                    f"Connection lost, retry {attempt}/{max_retries} ({e})"
                )

                # reconnect client safely
                try:
                    if not client.is_connected:
                        await client.connect()
                except Exception:
                    pass

                if attempt >= max_retries:
                    log.error("Failed to send after retries")
                    return None

                await asyncio.sleep(delay)
                delay = min(delay * 2, 15)

            except Exception as e:
                log.exception(f"Unexpected send error: {e}")
                return None
