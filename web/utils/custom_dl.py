import asyncio
import logging
from pyrogram import Client
from pyrogram.raw import functions
from pyrogram.raw.types import InputFileLocation
from aiohttp import web

logger = logging.getLogger("custom_dl")

GLOBAL_STREAM_LIMIT = asyncio.Semaphore(4)  # Example, adjust as needed
work_loads = {}

MIN_CHUNK = 512 * 1024
MAX_CHUNK = 5 * 1024 * 1024

class CustomDownloader:
    def __init__(self, client: Client):
        self.client = client
        self.client.media_sessions = {}  # store sessions by dc_id
        self.dc_locks = {}

    def get_dc_lock(self, dc_id):
        if dc_id not in self.dc_locks:
            self.dc_locks[dc_id] = asyncio.Lock()
        return self.dc_locks[dc_id]

    async def reset_media_session(self, dc_id):
        session = self.client.media_sessions.get(dc_id)
        if session:
            try:
                await session.stop()
            except Exception:
                pass
        self.client.media_sessions.pop(dc_id, None)

    async def generate_media_session(self, file_id):
        """Create or reuse a media session for the file's DC."""
        dc_id = file_id.dc_id

        # Try to reuse session if exists
        session = self.client.media_sessions.get(dc_id)
        if session:
            try:
                # Test if session works
                await session.send(functions.help.GetConfig())
                return session
            except Exception:
                await self.reset_media_session(dc_id)
                session = None

        # Ensure only one session is created per DC
        async with self.get_dc_lock(dc_id):
            try:
                session = self.client.session  # Use main client session
                # Optional: create separate media session per DC if needed
                self.client.media_sessions[dc_id] = session
                return session
            except Exception as e:
                logger.error(f"Media session create failed: {e}")
                await self.reset_media_session(dc_id)
                raise

    async def get_location(self, file_id):
        """Return InputFileLocation for Pyrogram raw API"""
        # Example, adjust based on your actual file structure
        return InputFileLocation(
            volume_id=file_id.volume_id,
            local_id=file_id.local_id,
            secret=file_id.secret
        )

    async def safe_get_chunk(self, session, location, offset, chunk_size, dc_id):
        """Fetch chunk safely"""
        try:
            result = await session.send(
                functions.upload.GetFile(
                    location=location,
                    offset=offset,
                    limit=chunk_size
                )
            )
            return result
        except Exception as e:
            logger.error(f"Chunk fetch error: {e}")
            return None

    async def yield_file(
        self,
        file_id,
        index,
        offset,
        first_part_cut,
        last_part_cut,
        part_count,
        chunk_size,
    ):
        """Generator that yields file chunks safely"""
        async with GLOBAL_STREAM_LIMIT:
            work_loads[index] = work_loads.get(index, 0) + 1
            dc_id = file_id.dc_id

            try:
                # Attempt to create session
                try:
                    session = await self.generate_media_session(file_id)
                except Exception as e:
                    logger.error(f"Failed to create media session: {e}")
                    return  # stop generator safely

                # Get file location for chunk fetching
                try:
                    location = await self.get_location(file_id)
                except Exception as e:
                    logger.error(f"Failed to get location: {e}")
                    return

                current_offset = offset
                part = 1
                dynamic_chunk = min(MAX_CHUNK, max(chunk_size, MIN_CHUNK))

                while part <= part_count:
                    try:
                        r = await self.safe_get_chunk(
                            session, location, current_offset, dynamic_chunk, dc_id
                        )
                    except Exception as e:
                        logger.error(f"Error fetching chunk: {e}")
                        return

                    if not r or not hasattr(r, "bytes"):
                        return

                    data = r.bytes
                    if not data:
                        return

                    # Yield correct slice of the chunk
                    if part_count == 1:
                        yield data[first_part_cut:last_part_cut]
                    elif part == 1:
                        yield data[first_part_cut:]
                    elif part == part_count:
                        yield data[:last_part_cut]
                    else:
                        yield data

                    current_offset += len(data)
                    part += 1

            finally:
                work_loads[index] -= 1
