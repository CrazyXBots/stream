import asyncio
import logging
import os
import time

from database.users_db import db
from web.utils.file_properties import get_hash
from pyrogram import Client, filters, enums
from info import URL, BOT_USERNAME, BIN_CHANNEL, BAN_ALERT, FSUB, CHANNEL
from utils import get_size
from Script import script
from pyrogram.errors import FloodWait, MessageIdInvalid, RPCError
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from plugins.mslandersbot import is_user_joined, is_user_allowed

# ----------------------------
# Logger setup
# ----------------------------
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ----------------------------
# Private message handler
# ----------------------------
@Client.on_message(
    (filters.private) & (filters.document | filters.video | filters.audio),
    group=4,
)
async def private_receive_handler(c: Client, m: Message):
    try:
        # --- Free subscription check ---
        if FSUB and not await is_user_joined(c, m):
            await m.reply("⚠️ कृपया पहले चैनल जॉइन करें।")
            return

        # --- Ban check ---
        if await db.is_banned(int(m.from_user.id)):
            await m.reply(BAN_ALERT)
            return

        user_id = m.from_user.id

        # --- Rate limit / upload limit check ---
        is_allowed, remaining_time = await is_user_allowed(user_id)
        if not is_allowed:
            await m.reply_text(
                f"⚠️ आप पहले ही 10 फ़ाइलें भेज चुके हैं।\n"
                f"कृपया {remaining_time} सेकंड बाद पुन: प्रयास करें।",
                quote=True,
            )
            return

        # --- Extract file info ---
        file_obj = m.document or m.video or m.audio
        file_name = file_obj.file_name if file_obj else "Unknown"
        file_size = get_size(file_obj.file_size) if file_obj else "Unknown"

        # --- Forward file to BIN_CHANNEL ---
        msg = await m.forward(chat_id=BIN_CHANNEL)

        # --- Build response links ---
        file_hash = get_hash(msg)
        stream_url = f"{URL}watch/{msg.id}?hash={file_hash}"
        download_url = f"{URL}{msg.id}?hash={file_hash}"
        file_link = f"https://t.me/{BOT_USERNAME}?start=file_{msg.id}"
        share_link = f"https://t.me/share/url?url={file_link}"

        # --- Prepare reply text --
        reply_text = (
            f"📌 **Requested By:** [{m.from_user.first_name}](tg://user?id={m.from_user.id})\n"
            f"👤 **User ID:** `{m.from_user.id}`\n"
            f"🔗 **Stream Link:** {stream_url}\n"
        )

        await msg.reply_text(
            text=reply_text,
            disable_web_page_preview=True,
            quote=True,
        )

        # --- Send formatted info with buttons ---
        buttons = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("📺 STREAM", url=stream_url),
                    InlineKeyboardButton("⬇️ DOWNLOAD", url=download_url),
                ],
                [
                    InlineKeyboardButton("📎 GET FILE", url=file_link),
                    InlineKeyboardButton("🔗 SHARE", url=share_link),
                ],
                [
                    InlineKeyboardButton("❌ CLOSE", callback_data="close_data")
                ],
            ]
        )

        await m.reply_text(
            text=script.CAPTION_TXT.format(
                CHANNEL, file_name, file_size, stream_url, download_url
            ),
            quote=True,
            disable_web_page_preview=True,
            reply_markup=buttons,
        )

    except FloodWait as e:
        wait_time = getattr(e, "value", None) or getattr(e, "seconds", None) or 1
        logger.warning(f"FloodWait {wait_time}s for user {m.from_user.id}, sleeping before retry...")
        await asyncio.sleep(wait_time)
        return await private_receive_handler(c, m)

    except MessageIdInvalid:
        logger.error("Invalid message ID encountered.")
    except RPCError as e:
        logger.error(f"Tg RPC Error: {e}")
    except Exception as e:
        logger.exception(f"Unhandled error in private_receive_handler: {e}")
