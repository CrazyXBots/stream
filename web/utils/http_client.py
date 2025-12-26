import aiohttp
import asyncio
import logging

logger = logging.getLogger("http_client")

MAX_RETRIES = 5
BACKOFF = 2  # seconds

async def fetch(url, headers=None, chunk_size=1024*1024):
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as resp:
                    resp.raise_for_status()
                    while True:
                        chunk = await resp.content.read(chunk_size)
                        if not chunk:
                            break
                        yield chunk
                    return
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            attempt += 1
            wait = BACKOFF * attempt
            logger.warning(f"HTTP fetch failed ({attempt}/{MAX_RETRIES}), retry in {wait}s: {e}")
            await asyncio.sleep(wait)
    logger.error(f"Failed to fetch {url} after {MAX_RETRIES} retries")
