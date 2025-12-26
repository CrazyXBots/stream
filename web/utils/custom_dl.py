import asyncio
import logging
import time
from typing import Dict, AsyncGenerator

from pyrogram import Client, raw
from pyrogram.raw.types import FileType, ThumbnailSource
from pyrogram.file_id import FileId

from web.utils.connection_manager import ConnectionManager  # <-- Added
from web.server.exceptions import FIleNotFound
from web.utils.file_properties import get_file_ids
from web.server import work_loads

# ----------------------------
# Configs
# ----------------------------
MAX_RETRIES = 6
BASE_BACKOFF = 2
GLOBAL_STREAM_LIMIT = asyncio.Semaphore(10)
MIN_CHUNK = 64 * 1024
MAX_CHUNK = 512 * 1024

logger = logging.getLogger("custom_dl")
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class ByteStreamer:
    def __init__(self, client: Client):
        self.client = client
        self.dc_locks: Dict[int, asyncio.Semaphore] = {}
        self.cached_file_ids: Dict[int, FileId] = {}
        self.last_used: Dict[int, float] = {}

        # Create a ConnectionManager for safe calls
        self.conn_mgr = ConnectionManager(client)  # <-- Added

        asyncio.create_task(self._cache_cleaner())
        asyncio.create_task(self._session_cleanup())

    async def _cache_cleaner(self):
        while True:
            await asyncio.sleep(1800)
            self.cached_file_ids.clear()

    async def _session_cleanup(self):
        while True:
            await asyncio.sleep(60)
            now = time.time()
            for dc_id, last in list(self.last_used.items()):
                if now - last > 300:
                    self.last_used.pop(dc_id, None)

    def _dc_lock(self, dc_id: int):
        if dc_id not in self.dc_locks:
            self.dc_locks[dc_id] = asyncio.Semaphore(2)
        return self.dc_locks[dc_id]

    async def get_file_properties(self, msg_id: int) -> FileId:
        if msg_id not in self.cached_file_ids:
            file_id = await get_file_ids(self.client, msg_id)
            if not file_id:
                raise FIleNotFound
            self.cached_file_ids[msg_id] = file_id
        return self.cached_file_ids[msg_id]

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

    async def _fetch_chunk(
        self,
        location,
        offset: int,
        limit: int,
        dc_id: int
    ):
        current_limit = limit
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self.last_used[dc_id] = time.time()
                # Use ConnectionManager to safely download
                result = await self.conn_mgr.safe_download(location, offset=offset, limit=current_limit)
                return result

            except Exception as e:
                # Backoff retry logic
                backoff = BASE_BACKOFF**attempt
                logger.warning(f"[fetch_chunk] retry {attempt}/{MAX_RETRIES} backoff={backoff}s: {e}")
                await asyncio.sleep(backoff)

        return None

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

        async with GLOBAL_STREAM_LIMIT:
            work_loads[index] += 1
            try:
                location = await self._get_location(file_id)
            except Exception as e:
                logger.error(f"[yield_file] location error: {e}")
                work_loads[index] -= 1
                return

            current_offset = offset
            part = 1
            dynamic_chunk = min(MAX_CHUNK, max(chunk_size, MIN_CHUNK))

            while part <= part_count:
                try:
                    r = await self._fetch_chunk(
                        location,
                        current_offset,
                        dynamic_chunk,
                        file_id.dc_id,
                    )
                except Exception as e:
                    logger.error(f"[yield_file] chunk fetch error: {e}")
                    break

                if not r or not hasattr(r, "bytes"):
                    break

                data = r.bytes
                if not data:
                    break

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

            work_loads[index] -= 1
