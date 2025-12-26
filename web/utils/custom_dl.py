import math
import asyncio
import logging
from info import *
from typing import Dict, Union
from web.server import work_loads
from pyrogram import Client, utils, raw
from web.utils.file_properties import get_file_ids
from pyrogram.session import Session, Auth
from pyrogram.errors import AuthBytesInvalid, FloodWait, RPCError
from web.server.exceptions import FIleNotFound
from pyrogram.file_id import FileId, FileType, ThumbnailSource
import os
from web.utils.safe_send import send

# Dont Remove My Credit
# @MSLANDERS
# For Any Kind Of Error Ask Us In Support Group @MSLANDERS_HELP

class ByteStreamer:
    def __init__(self, client: Client):
        """
        A custom class that holds the cache of a specific client and class functions.
        """
        self.clean_timer = 30 * 60
        self.client: Client = client
        self.cached_file_ids: Dict[int, FileId] = {}
        asyncio.create_task(self.clean_cache())

    async def get_file_properties(self, id: int) -> FileId:
        if id not in self.cached_file_ids:
            await self.generate_file_properties(id)
            logging.debug(f"Cached file properties for message with ID {id}")
        return self.cached_file_ids[id]

    async def generate_file_properties(self, id: int) -> FileId:
        file_id = await get_file_ids(self.client, BIN_CHANNEL, id)
        logging.debug(f"Generated file ID and Unique ID for message with ID {id}")
        if not file_id:
            logging.debug(f"Message with ID {id} not found")
            raise FIleNotFound
        self.cached_file_ids[id] = file_id
        logging.debug(f"Cached media message with ID {id}")
        return file_id

    async def generate_media_session(self, client: Client, file_id: FileId) -> Session:
        """
        Generates the media session for the DC that contains the media file.
        """
        media_session = client.media_sessions.get(file_id.dc_id, None)

        if media_session is None:
            # Trying direct or exported auth if different DC
            try:
                if file_id.dc_id != await client.storage.dc_id():
                    media_session = Session(
                        client,
                        file_id.dc_id,
                        await Auth(
                            client,
                            file_id.dc_id,
                            await client.storage.test_mode()
                        ).create(),
                        await client.storage.test_mode(),
                        is_media=True,
                    )
                    await media_session.start()

                    for _ in range(6):
                        exported_auth = await client.invoke(
                            raw.functions.auth.ExportAuthorization(dc_id=file_id.dc_id)
                        )
                        try:
                            await media_session.send(
                                raw.functions.auth.ImportAuthorization(
                                    id=exported_auth.id,
                                    bytes=exported_auth.bytes,
                                )
                            )
                            break
                        except AuthBytesInvalid:
                            logging.debug(f"Invalid authorization bytes for DC {file_id.dc_id}")
                            continue
                    else:
                        await media_session.stop()
                        raise AuthBytesInvalid

                else:
                    media_session = Session(
                        client,
                        file_id.dc_id,
                        await client.storage.auth_key(),
                        await client.storage.test_mode(),
                        is_media=True,
                    )
                    await media_session.start()

                logging.debug(f"Created media session for DC {file_id.dc_id}")
                client.media_sessions[file_id.dc_id] = media_session

            except Exception as e:
                logging.error(f"Media session creation failed: {e}")
                raise

        else:
            logging.debug(f"Using cached media session for DC {file_id.dc_id}")

        return media_session

    @staticmethod
    async def get_location(file_id: FileId) -> Union[
        raw.types.InputPhotoFileLocation,
        raw.types.InputDocumentFileLocation,
        raw.types.InputPeerPhotoFileLocation,
    ]:
        file_type = file_id.file_type

        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(
                    user_id=file_id.chat_id,
                    access_hash=file_id.chat_access_hash
                )
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(
                        channel_id=utils.get_channel_id(file_id.chat_id),
                        access_hash=file_id.chat_access_hash,
                    )
            location = raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )

        elif file_type == FileType.PHOTO:
            location = raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        else:
            location = raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )

        return location

    async def yield_file(
        self,
        file_id: FileId,
        index: int,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
    ) -> Union[bytes, None]:
        """
        Custom generator that yields the bytes of the media file with safe retries
        """
        client = self.client
        work_loads[index] += 1
        logging.debug(f"Starting to stream file with client {index}.")

        try:
            media_session = await self.generate_media_session(client, file_id)
            location = await self.get_location(file_id)

            current_part = 1

            while current_part <= part_count:
                # Retry logic for Telegram connection errors
                for attempt in range(6):
                    try:
                        r = await media_session.send(
                            raw.functions.upload.GetFile(
                                location=location,
                                offset=offset,
                                limit=chunk_size
                            )
                        )
                        break
                    except (OSError, ConnectionResetError) as e:
                        logging.warning(f"Connection lost, retry {attempt+1}/6...")
                        await asyncio.sleep(2 ** attempt)
                    except FloodWait as e:
                        logging.warning(f"Flood wait {e.x}s")
                        await asyncio.sleep(e.x)
                else:
                    logging.error("Failed to send after retries")
                    return

                if not isinstance(r, raw.types.upload.File):
                    logging.error("Unexpected type returned from Telegram")
                    return

                while True:
                    chunk = r.bytes
                    if not chunk:
                        break

                    # yield correct part
                    if part_count == 1:
                        yield chunk[first_part_cut:last_part_cut]
                    elif current_part == 1:
                        yield chunk[first_part_cut:]
                    elif current_part == part_count:
                        yield chunk[:last_part_cut]
                    else:
                        yield chunk

                    current_part += 1
                    offset += chunk_size

                    if current_part > part_count:
                        break

                    # fetch next chunk
                    r = await media_session.send(
                        raw.functions.upload.GetFile(
                            location=location,
                            offset=offset,
                            limit=chunk_size
                        )
                    )

        except Exception as e:
            logging.error(f"Error while streaming: {e}")

        finally:
            work_loads[index] -= 1
            logging.debug(f"Finished yielding file (client {index}).")

    async def clean_cache(self) -> None:
        """
        function to clean the cache to reduce memory usage
        """
        while True:
            await asyncio.sleep(self.clean_timer)
            self.cached_file_ids.clear()
            logging.debug("Cleaned the cache")

# Dont Remove My Credit
# @MSLANDERS
# For Any Kind Of Error Ask Us In Support Group @MSLANDERS_HELP
