import asyncio
import logging
from typing import AsyncGenerator

from pyrogram.raw.functions.upload import GetFile
from pyrogram.raw.types import InputFileLocation

log = logging.getLogger(__name__)

# ================= CONFIG ================= #

DEFAULT_CHUNK_SIZE = 512 * 1024  # 512 KB
MIN_CHUNK_SIZE = 32 * 1024       # 32 KB
MAX_RETRIES = 5
BASE_BACKOFF = 1.5

# Global limiter (FIX 9)
GLOBAL_STREAM_LIMIT = asyncio.Semaphore(10)

# ========================================= #


class CustomDownloader:
    def __init__(self, client):
        self.client = client

    # ------------------------------------------------ #
    # FIX 3: NEVER reuse a broken session
    # ------------------------------------------------ #
    async def reset_media_session(self, dc_id: int):
        try:
            session = self.client.media_sessions.pop(dc_id, None)
            if session:
                await session.close()
        except Exception:
            pass

    async def generate_media_session(self, dc_id: int):
        """
        Pyrogram-safe:
        - DO NOT check session.is_connected (does NOT exist)
        - Session health is detected only on send failure
        """
        session = self.client.media_sessions.get(dc_id)
        if session:
            return session

        session = await self.client.session.start_media_session(dc_id)
        self.client.media_sessions[dc_id] = session
        return session

    # ------------------------------------------------ #
    # FIX 2 / 4 / 5 / 8: SAFE chunk fetch with retries
    # ------------------------------------------------ #
    async def safe_get_chunk(
        self,
        session,
        location: InputFileLocation,
        offset: int,
        limit: int,
        dc_id: int
    ) -> bytes:
        dynamic_limit = limit

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = await session.send(
                    GetFile(
                        location=location,
                        offset=offset,
                        limit=dynamic_limit
                    )
                )
                return bytes(result.bytes)

            except (OSError, ConnectionResetError, asyncio.TimeoutError) as e:
                log.warning(f"Connection retry {attempt}/{MAX_RETRIES}: {e}")

                # FIX 8: Adaptive chunk size
                dynamic_limit = max(MIN_CHUNK_SIZE, dynamic_limit // 2)

                # FIX 3: Reset broken session
                await self.reset_media_session(dc_id)
                session = await self.generate_media_session(dc_id)

                await asyncio.sleep(BASE_BACKOFF ** attempt)

        raise RuntimeError("Failed to fetch chunk after retries")

    # ------------------------------------------------ #
    # FIX 1 → 13: FINAL STREAM GENERATOR
    # ------------------------------------------------ #
    async def yield_file(
        self,
        file_id,
        file_size: int,
        offset: int = 0
    ) -> AsyncGenerator[bytes, None]:

        async with GLOBAL_STREAM_LIMIT:  # FIX 9

            location = file_id.location
            dc_id = file_id.dc_id

            chunk_size = DEFAULT_CHUNK_SIZE
            current_offset = offset  # FIX 7

            session = await self.generate_media_session(dc_id)

            while current_offset < file_size:
                try:
                    data = await self.safe_get_chunk(
                        session=session,
                        location=location,
                        offset=current_offset,
                        limit=chunk_size,
                        dc_id=dc_id
                    )

                except Exception as e:
                    log.error(f"Chunk fetch failed at offset {current_offset}: {e}")
                    break

                if not data:
                    break

                # FIX 7: Resume EXACT byte
                current_offset += len(data)

                yield data

    # ------------------------------------------------ #
    # Optional helper (used by stream.py)
    # ------------------------------------------------ #
    async def stream_file(self, file_id, file_size, offset=0):
        async for chunk in self.yield_file(file_id, file_size, offset):
            yield chunk
