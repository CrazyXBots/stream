import asyncio 
import logging
import time
from typing import Dict, AsyncGenerator

from pyrogram import Client, raw, utils
from pyrogram.errors import FloodWait, AuthBytesInvalid
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Session, Auth

from info import BIN_CHANNEL
from web.server import work_loads
from web.server.exceptions import FIleNotFound
from web.utils.file_properties import get_file_ids

# ----------------------------
# Configuration
# ----------------------------
MAX_RETRIES = 6
BASE_BACKOFF = 2
MAX_STREAMS_PER_DC = 2
GLOBAL_STREAM_LIMIT = asyncio.Semaphore(10)
SESSION_IDLE_TIMEOUT = 300
MIN_CHUNK = 64 * 1024
MAX_CHUNK = 512 * 1024
IDLE_CLEAN_INTERVAL = 60

# ----------------------------
# Logger
# ----------------------------
logger = logging.getLogger("custom_dl")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# =========================
# ByteStreamer Class
# =========================
class ByteStreamer:
    def __init__(self, client: Client):
        self.client = client
        self.cached_file_ids: Dict[int, FileId] = {}
        self.dc_locks: Dict[int, asyncio.Semaphore] = {}
        self.last_used: Dict[int, float] = {}

        # periodic maintenance
        asyncio.create_task(self._cache_cleaner())
        asyncio.create_task(self._session_cleanup())

    # ---------------------------
    # File ID caching
    # ---------------------------
    async def get_file_properties(self, msg_id: int) -> FileId:
        if msg_id not in self.cached_file_ids:
            file_id = await get_file_ids(self.client, BIN_CHANNEL, msg_id)
            if not file_id:
                logger.error(f"File ID not found for {msg_id}")
                raise FIleNotFound
            self.cached_file_ids[msg_id] = file_id
        return self.cached_file_ids[msg_id]

    async def _cache_cleaner(self):
      async def generate_media_session(self, file_id: FileId) -> Session:
        dc_id = file_id.dc_id

        # Try to reuse session if exists
        session = self.client.media_sessions.get(dc_id)
        if session:
            # Attempt a simple call to verify session is valid
            try:
                await session.send(raw.functions.help.GetConfig())
                return session
            except Exception:
                await self.reset_media_session(dc_id)
                session = None

        async with self.get_dc_lock(dc_id):
            try:
                # Always create a fresh media session for target DC
                session = Session(
                    self.client,
                    dc_id,
                    await self.client.storage.auth_key(),
                    await self.client.storage.test_mode(),
                    is_media=True,
                )
                await session.start()

                # Save for reuse
                self.client.media_sessions[dc_id] = session
                return session

            except Exception as e:
                logger.error(f"Media session create failed: {e}")
                # ensure removal and stop
                await self.reset_media_session(dc_id)
                raise  while True:
            await asyncio.sleep(1800)
            self.cached_file_ids.clear()

    # ---------------------------
    # Session management
    # ---------------------------
    def _dc_lock(self, dc_id: int):
        if dc_id not in self.dc_locks:
            self.dc_locks[dc_id] = asyncio.Semaphore(MAX_STREAMS_PER_DC)
        return self.dc_locks[dc_id]

    async def _reset_media_session(self, dc_id: int):
        session = self.client.media_sessions.pop(dc_id, None)
        if session:
            try:
                await session.stop()
            except Exception as e:
                logger.debug(f"Session reset error: {e}")

    

    async def _session_cleanup(self):
        while True:
            await asyncio.sleep(IDLE_CLEAN_INTERVAL)
            now = time.time()
            for dc_id, last in list(self.last_used.items()):
                if now - last > SESSION_IDLE_TIMEOUT:
                    await self._reset_media_session(dc_id)
                    self.last_used.pop(dc_id, None)

    # ---------------------------
    # File location
    # ---------------------------
    @staticmethod
    async def _get_location(file_id: FileId):
        if file_id.file_type == FileType.CHAT_PHOTO:
            peer = (
                raw.types.InputPeerUser(
                    user_id=file_id.chat_id,
                    access_hash=file_id.chat_access_hash,
                )
                if file_id.chat_id > 0
                else raw.types.InputPeerChannel(
                    channel_id=utils.get_channel_id(file_id.chat_id),
                    access_hash=file_id.chat_access_hash,
                )
            )
            return raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )

        if file_id.file_type == FileType.PHOTO:
            return raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )

        return raw.types.InputDocumentFileLocation(
            id=file_id.media_id,
            access_hash=file_id.access_hash,
            file_reference=file_id.file_reference,
            thumb_size=file_id.thumbnail_size,
        )

    # ---------------------------
    # Chunk fetch with retry & backoff
    # ---------------------------
    async def _fetch_chunk(
        self, session: Session, location, offset: int, limit: int, dc_id: int
    ):
        current_limit = limit
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self.last_used[dc_id] = time.time()
                result = await session.send(
                    raw.functions.upload.GetFile(
                        location=location, offset=offset, limit=current_limit
                    )
                )
                return result
            except FloodWait as e:
                wait = e.value or getattr(e, "seconds", None) or 5
                logger.info(f"FloodWait {wait}s on dc {dc_id}")
                await asyncio.sleep(wait)
            except (OSError, ConnectionResetError, asyncio.TimeoutError) as e:
                # halve chunk on network errors
                current_limit = max(MIN_CHUNK, current_limit // 2)
                backoff = BASE_BACKOFF**attempt
                logger.warning(f"Network error at offset {offset}, backoff {backoff}s: {e}")
                await asyncio.sleep(backoff)
                await self._reset_media_session(dc_id)
            except Exception as e:
                logger.error(f"Unexpected chunk fetch error: {e}")
                break
        return None

    # ---------------------------
    # Streaming generator
    # ---------------------------
    async def yield_file(
        self,
        file_id: FileId,
        index: int,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
    ):
        async with GLOBAL_STREAM_LIMIT:
            work_loads[index] += 1
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

                    if not r or not isinstance(r, raw.types.upload.File):
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
