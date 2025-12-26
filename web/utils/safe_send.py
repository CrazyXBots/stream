import asyncio
from pyrogram.errors import RPCError

async def safe_send(func, *args, **kwargs):
    try:
        return await func(*args, **kwargs)
    except (RPCError, OSError, ConnectionResetError, asyncio.TimeoutError):
        return None
    """
    Safely send media/messages via Pyrogram with retry logic.
    
    Args:
        session: Pyrogram Client or media session
        *args, **kwargs: Arguments for session.send()
    
    Returns:
        Result of session.send() if successful.
    
    Raises:
        Exception if all retries fail.
    """
    max_retries = 3
    delay = 2  # seconds between retries

    for attempt in range(1, max_retries + 1):
        try:
            return await session.send(*args, **kwargs)
        except (OSError, ConnectionResetError, RPCError) as e:
            print(f"[safe_send] Attempt {attempt}/{max_retries} failed: {e}")
            await asyncio.sleep(delay)

    raise Exception(f"Failed to send after {max_retries} attempts.")
