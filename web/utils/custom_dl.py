import asyncio
import logging
import time
from typing import Dict, AsyncGenerator

from pyrogram import Client, raw
from pyrogram.errors import FloodWait
from pyrogram.file_id import FileId

from info import BIN_CHANNEL
from web.server import work_loads
from web.server.exceptions import FIleNotFound
from web.utils.file_properties import get_file_ids

# ----------------------------
# Configuration
# ----------------------------
MAX_RETRIES = 6
BASE_BACKOFF = 2
GLOBAL_STREAM_LIMIT = asyncio.Semaphore(10)
SESSION_IDLE_TIMEOUT = 300
IDLE_CLEAN_INTERVAL = 60
MIN_CHUNK = 64 * 1024
MAX_CHUNK = 512 * 1024

# ----------------------------
# Logger
# ----------------------------
logger = logging.getLogger("custom_dl")
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class ByteStreamer:
    def __init__(self, client: Client):
        self.client = client
        self.cached_file_ids: Dict[int, FileId] = {}
        self.dc_locks: Dict[int, asyncio.Semaphore] = {}
        self.last_used: Dict[int, float] = {}

        # maintenance tasks
        asyncio.create_task(self._cache_cleaner())
        asyncio.create_task(self._session_cleanup())

    # ---------------------------
    # File ID caching
    # ---------------------------
    async def get_file_properties(self, msg_id: int) -> FileId:
        """
        Gets FileId object for the Telegram message;
        caches result so repeated lookups don’t require Pyrogram RPC.
        """
        if msg_id not in self.cached_file_ids:
            file_id = await get_file_ids(self.client, BIN_CHANNEL, msg_id)
            if not file_id:
                logger.error(f"File ID not found for message {msg_id}")
                raise FIleNotFound
            self.cached_file_ids[msg_id] = file_id
        return self.cached_file_ids[msg_id]

    async def _cache_cleaner(self):
        while True:
            await asyncio.sleep(1800)
            self.cached_file_ids.clear()

    # ---------------------------
    # DC locking
    # ---------------------------
    def _dc_lock(self, dc_id: int):
        if dc_id not in self.dc_locks:
            self.dc_locks[dc_id] = asyncio.Semaphore(2)
        return self.dc_locks[dc_id]

    async def _session_cleanup(self):
        """
        Clears unused sessions after they've been idle to avoid memory leaks.
        """
        while True:
            await asyncio.sleep(IDLE_CLEAN_INTERVAL)
            now = time.time()
            for dc_id, last in list(self.last_used.items()):
                if now - last > SESSION_IDLE_TIMEOUT:
                    self.client.media_sessions.pop(dc_id, None)
                    self.last_used.pop(dc_id, None)

    # ---------------------------
    # File location logic
    # ---------------------------
    @staticmethod
    async def _get_location(file_id: FileId):
        """
        Build a raw input file location suitable for upload.GetFile
        (this does NOT use FileType/ThumbnailSource since pyrogram 2.x removed them).
        """
        # Photo
        if file_id.file_type == "photo":
            return raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )

        # Document
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
        self,
        session: Client,
        location,
        offset: int,
        limit: int,
        dc_id: int,
    ):
        current_limit = limit

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # mark this client as recently used
                self.last_used[dc_id] = time.time()

                # Pyrogram Client can send raw Upload.GetFile
                result = await session.send(
                    raw.functions.upload.GetFile(
                        location=location,
                        offset=offset,
                        limit=current_limit,
                    )
                )
                return result

            except FloodWait as e:
                wait = e.value or getattr(e, "seconds", None) or 5
                logger.warning(f"FloodWait {wait}s on dc {dc_id}")
                await asyncio.sleep(wait)

            except (OSError, ConnectionResetError, asyncio.TimeoutError) as e:
                # network errors → reduce chunk size + backoff
                current_limit = max(MIN_CHUNK, current_limit // 2)
                backoff = BASE_BACKOFF**attempt
                logger.warning(
                    f"Network error at offset {offset}, backoff {backoff}s: {e}"
                )
                await asyncio.sleep(backoff)

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
    ) -> AsyncGenerator[bytes, None]:
        """
        Generator that yields exact byte chunks for streaming,
        always yielding something or exiting cleanly.
        """
        async with GLOBAL_STREAM_LIMIT:
            work_loads[index] += 1
            dc_id = file_id.dc_id

            try:
                # location for the file download
                location = await self._get_location(file_id)

                current_offset = offset
                part = 1
                dynamic_chunk = min(MAX_CHUNK, max(chunk_size, MIN_CHUNK))

                while part <= part_count:

                    result = await self._fetch_chunk(
                        self.client,
                        location,
                        current_offset,
                        dynamic_chunk,
                        dc_id,
                    )

                    if not result or not hasattr(result, "bytes"):
                        # no chunk → stop generator safely
                        return

                    data = result.bytes
                    if not data:
                        return

                    # handle slicing for first/last parts
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
