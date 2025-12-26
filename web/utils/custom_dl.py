import asyncio
import logging
from typing import AsyncGenerator

from pyrogram.raw.functions.upload import GetFile
from pyrogram.raw.types import InputFileLocation

log = logging.getLogger(__name__)

# ================= CONFIG =================

DEFAULT_CHUNK_SIZE = 512 * 1024  # 512 KB
MIN_CHUNK_SIZE = 64 * 1024       # 64 KB
MAX_RETRIES = 5
BACKOFF_BASE = 1.5

# Limit concurrent streams (VERY IMPORTANT for aiohttp)
GLOBAL_STREAM_LIMIT = asyncio.Semaphore(10)

# =========================================


class CustomDownloader:
    def __init__(self, client):
        self.client = client

    # -------------------------------------------------
    # SESSION MANAGEMENT (NO is_connected ANYWHERE)
    # -------------------------------------------------
    async def generate_media_session(self, dc_id: int):
        """
        Pyrogram Session has NO is_connected attribute.
        If a session exists, reuse it.
        If it is broken, send() will fail and we recreate it.
        """
        session = self.client.media_sessions.get(dc_id)
        if session:
            return session

        session = await self.client.session.start_media_session(dc_id)
        self.client.media_sessions[dc_id] = session
        return session

    async def reset_media_session(self, dc_id: int):
        """
        Destroy broken session safely.
        """
        try:
            session = self.client.media_sessions.pop(dc_id, None)
            if session:
                await session.close()
        except Exception:
            pass

    # -------------------------------------------------
    # SAFE CHUNK DOWNLOAD WITH RETRY
    # -------------------------------------------------
    async def fetch_chunk(
        self,
        session,
        location: InputFileLocation,
        offset: int,
        limit: int,
        dc_id: int
    ) -> bytes:

        chunk_limit = limit

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = await session.send(
                    GetFile(
                        location=location,
                        offset=offset,
                        limit=chunk_limit
                    )
                )
                return bytes(result.bytes)

            except (asyncio.TimeoutError, ConnectionResetError, OSError) as e:
                log.warning(
                    f"[Chunk Retry {attempt}/{MAX_RETRIES}] "
                    f"offset={offset} error={e}"
                )

                # Reduce chunk size on failure
                chunk_limit = max(MIN_CHUNK_SIZE, chunk_limit // 2)

                # Reset session completely
                await self.reset_media_session(dc_id)
                session = await self.generate_media_session(dc_id)

                await asyncio.sleep(BACKOFF_BASE ** attempt)

        raise RuntimeError("Chunk download failed after retries")

    # -------------------------------------------------
    # MAIN STREAM GENERATOR (aiohttp SAFE)
    # -------------------------------------------------
    async def yield_file(
        self,
        file_id,
        file_size: int,
        offset: int = 0
    ) -> AsyncGenerator[bytes, None]:

        async with GLOBAL_STREAM_LIMIT:

            location = file_id.location
            dc_id = file_id.dc_id
            current_offset = offset
            chunk_size = DEFAULT_CHUNK_SIZE

            session = await self.generate_media_session(dc_id)

            while current_offset < file_size:
                try:
                    data = await self.fetch_chunk(
                        session=session,
                        location=location,
                        offset=current_offset,
                        limit=chunk_size,
                        dc_id=dc_id
                    )
                except Exception as e:
                    log.error(f"Streaming stopped at {current_offset}: {e}")
                    break

                if not data:
                    break

                current_offset += len(data)
                yield data

    # -------------------------------------------------
    # COMPATIBILITY HELPER
    # -------------------------------------------------
    async def stream_file(self, file_id, file_size, offset=0):
        async for chunk in self.yield_file(file_id, file_size, offset):
            yield chunk
