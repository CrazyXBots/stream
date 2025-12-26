import asyncio
import logging
import time
from typing import Dict

from pyrogram import Client, raw, utils
from pyrogram.errors import FloodWait, AuthBytesInvalid
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Session, Auth

from info import BIN_CHANNEL
from web.server import work_loads
from web.server.exceptions import FIleNotFound
from web.utils.file_properties import get_file_ids

# ================= CONFIG ================= #

MAX_RETRIES = 5
BASE_BACKOFF = 2
MAX_STREAMS_PER_DC = 2
GLOBAL_STREAM_LIMIT = asyncio.Semaphore(10)

SESSION_IDLE_TIMEOUT = 300
MIN_CHUNK = 32 * 1024
MAX_CHUNK = 512 * 1024

# ========================================= #

class ByteStreamer:
    def __init__(self, client: Client):
        self.client = client
        self.cached_file_ids: Dict[int, FileId] = {}
        self.dc_locks: Dict[int, asyncio.Semaphore] = {}
        self.last_used: Dict[int, float] = {}

        asyncio.create_task(self.clean_cache())
        asyncio.create_task(self.cleanup_idle_sessions())

    # ---------------- FILE ID CACHE ---------------- #

    async def get_file_properties(self, msg_id: int) -> FileId:
        if msg_id not in self.cached_file_ids:
            file_id = await get_file_ids(self.client, BIN_CHANNEL, msg_id)
            if not file_id:
                raise FIleNotFound
            self.cached_file_ids[msg_id] = file_id
        return self.cached_file_ids[msg_id]

    async def clean_cache(self):
        while True:
            await asyncio.sleep(1800)
            self.cached_file_ids.clear()

    # ---------------- SESSION MANAGEMENT ---------------- #

    def get_dc_lock(self, dc_id: int):
        if dc_id not in self.dc_locks:
            self.dc_locks[dc_id] = asyncio.Semaphore(MAX_STREAMS_PER_DC)
        return self.dc_locks[dc_id]

    async def reset_media_session(self, dc_id: int):
        session = self.client.media_sessions.pop(dc_id, None)
        if session:
            try:
                await session.stop()
            except Exception:
                pass

    async def generate_media_session(self, file_id: FileId) -> Session:
        dc_id = file_id.dc_id
        session = self.client.media_sessions.get(dc_id)

        if session and not session.is_connected:
            await self.reset_media_session(dc_id)
            session = None

        if session:
            return session

        async with self.get_dc_lock(dc_id):
            try:
                if dc_id != await self.client.storage.dc_id():
                    auth = await Auth(
                        self.client,
                        dc_id,
                        await self.client.storage.test_mode()
                    ).create()

                    session = Session(
                        self.client,
                        dc_id,
                        auth,
                        await self.client.storage.test_mode(),
                        is_media=True
                    )
                    await session.start()

                    for _ in range(5):
                        exported = await self.client.invoke(
                            raw.functions.auth.ExportAuthorization(dc_id=dc_id)
                        )
                        try:
                            await session.send(
                                raw.functions.auth.ImportAuthorization(
                                    id=exported.id,
                                    bytes=exported.bytes
                                )
                            )
                            break
                        except AuthBytesInvalid:
                            continue
                else:
                    session = Session(
                        self.client,
                        dc_id,
                        await self.client.storage.auth_key(),
                        await self.client.storage.test_mode(),
                        is_media=True
                    )
                    await session.start()

                self.client.media_sessions[dc_id] = session
                return session

            except Exception:
                await self.reset_media_session(dc_id)
                raise

    async def cleanup_idle_sessions(self):
        while True:
            await asyncio.sleep(60)
            now = time.time()
            for dc_id, last in list(self.last_used.items()):
                if now - last > SESSION_IDLE_TIMEOUT:
                    await self.reset_media_session(dc_id)
                    self.last_used.pop(dc_id, None)

    # ---------------- FILE LOCATION ---------------- #

    @staticmethod
    async def get_location(file_id: FileId):
        if file_id.file_type == FileType.CHAT_PHOTO:
            peer = (
                raw.types.InputPeerUser(
                    user_id=file_id.chat_id,
                    access_hash=file_id.chat_access_hash
                )
                if file_id.chat_id > 0
                else raw.types.InputPeerChannel(
                    channel_id=utils.get_channel_id(file_id.chat_id),
                    access_hash=file_id.chat_access_hash
                )
            )
            return raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG
            )

        if file_id.file_type == FileType.PHOTO:
            return raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size
            )

        return raw.types.InputDocumentFileLocation(
            id=file_id.media_id,
            access_hash=file_id.access_hash,
            file_reference=file_id.file_reference,
            thumb_size=file_id.thumbnail_size
        )

    # ---------------- SAFE CHUNK FETCH ---------------- #

    async def safe_get_chunk(self, session, location, offset, limit, dc_id):
        dynamic_limit = limit

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self.last_used[dc_id] = time.time()
                return await session.send(
                    raw.functions.upload.GetFile(
                        location=location,
                        offset=offset,
                        limit=dynamic_limit
                    )
                )

            except FloodWait as e:
                await asyncio.sleep(e.value or e.seconds or 5)

            except (OSError, ConnectionResetError, asyncio.TimeoutError):
                dynamic_limit = max(MIN_CHUNK, dynamic_limit // 2)
                await asyncio.sleep(BASE_BACKOFF ** attempt)

        await self.reset_media_session(dc_id)
        return None

    # ---------------- STREAMING ---------------- #

    async def yield_file(
        self,
        file_id: FileId,
        index: int,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int
    ):
        async with GLOBAL_STREAM_LIMIT:
            work_loads[index] += 1
            dc_id = file_id.dc_id

            try:
                session = await self.generate_media_session(file_id)
                location = await self.get_location(file_id)

                current_offset = offset
                part = 1
                dynamic_chunk = min(MAX_CHUNK, max(chunk_size, MIN_CHUNK))

                while part <= part_count:
                    r = await self.safe_get_chunk(
                        session,
                        location,
                        current_offset,
                        dynamic_chunk,
                        dc_id
                    )

                    if not r or not isinstance(r, raw.types.upload.File):
                        return

                    data = r.bytes
                    if not data:
                        return

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
