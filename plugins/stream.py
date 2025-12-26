import asyncio
import os
import time

from database.users_db import db
from web.utils.file_properties import get_hash
from pyrogram import Client, filters
from info import URL, BOT_USERNAME, BIN_CHANNEL, BAN_ALERT, FSUB, CHANNEL
from utils import get_size
from Script import script
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from plugins.mslandersbot import is_user_joined, is_user_allowed

# ----------------------------
# Safe Forward Helper
# ----------------------------
async def safe_forward(bot: Client, message: Message, chat_id: int):
    """
    Safely forward a message handling FloodWait until complete.
    """
    while True:
        try:
            return await message.forward(chat_id=chat_id)
        except FloodWait as e:
            wait_time = getattr(e, "value", None) or getattr(e, "seconds", None) or 5
            await asyncio.sleep(wait_time)
        except RPCError as e:
            print(f"[safe_forward] RPC Error: {e}")
            return None
        except Exception as e:
            print(f"[safe_forward] Unexpected error: {e}")
            return None


@Client.on_message(
    (filters.private) & (filters.document | filters.video | filters.audio),
    group=4,
)
async def private_receive_handler(c: Client, m: Message):

    try:
        # --- Subscription / Join check ---
        if FSUB and not await is_user_joined(c, m):
            await m.reply("⚠️ कृपया पहले चैनल जॉइन करें।")
            return

        # --- Ban check ---
        if await db.is_banned(int(m.from_user.id)):
            await m.reply(BAN_ALERT)
            return

        # --- Rate limiting ---
        user_id = m.from_user.id
        is_allowed, remaining_time = await is_user_allowed(user_id)
        if not is_allowed:
            await m.reply_text(
                f"⚠️ आप पहले ही 10 फ़ाइल भेज चुके हैं।\n"
                f"कृपया {remaining_time} सेकंड बाद पुन: प्रयास करें।",
                quote=True,
            )
            return

        # --- Extract file info ---
        file_obj = m.document or m.video or m.audio
        if not file_obj:
            await m.reply_text("⚠️ फ़ाइल नहीं मिली।")
            return

        file_name = file_obj.file_name or None
        file_size = get_size(file_obj.file_size)

        # --- Forward safely ---
        msg = await safe_forward(c, m, BIN_CHANNEL)
        if msg is None:
            await m.reply_text("⚠️ फ़ाइल अग्रेषण में विफल। कृपया पुनः प्रयास करें।")
            return

        # --- Get file hash (safe) ---
        try:
            file_hash = get_hash(msg)
        except Exception as e:
            print(f"[stream] Failed to get hash: {e}")
            file_hash = ""

        # --- URLs ---
        stream_url = f"{URL}watch/{msg.id}?hash={file_hash}"
        download_url = f"{URL}{msg.id}?hash={file_hash}"
        file_link = f"https://t.me/{BOT_USERNAME}?start=file_{msg.id}"
        share_link = f"https://t.me/share/url?url={file_link}"

        # --- Reply with info ---
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

        # --- Buttons ---
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

        # --- Send caption with buttons ---
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
        await asyncio.sleep(wait_time)
        return await private_receive_handler(c, m)

    except Exception as e:
        print(f"[stream] Unexpected error: {e}")
