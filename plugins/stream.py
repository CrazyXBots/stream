import asyncio
import os
import time

from database.users_db import db
from web.utils.file_properties import get_hash

from pyrogram import Client, filters, enums
from info import URL, BOT_USERNAME, BIN_CHANNEL, BAN_ALERT, FSUB, CHANNEL
from utils import get_size
from Script import script
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from plugins.mslandersbot import is_user_joined, is_user_allowed

# Dont Remove My Credit
# @MSLANDERS
# For Any Kind Of Error Ask Us In Support Group @MSLANDERS_HELP

async def safe_forward(bot: Client, message: Message, chat_id: int):
    """
    Safely forward a message handling FloodWait until complete.
    """
    while True:
        try:
            return await message.forward(chat_id=chat_id)
        except FloodWait as e:
            wait_time = getattr(e, "value", None) or getattr(e, "seconds", None) or 5
            print(f"FloodWait for {wait_time}s")
            await asyncio.sleep(wait_time)
        except RPCError as e:
            # Should not happen often, but log
            print(f"RPC Error during forward: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error during forward: {e}")
            return None


@Client.on_message(
    (filters.private)
    & (filters.document | filters.video | filters.audio),
    group=4
)
async def private_receive_handler(c: Client, m: Message):

    # Subscription / Join check
    if FSUB:
        if not await is_user_joined(c, m):
            return

    # Ban check
    if await db.is_banned(int(m.from_user.id)):
        return await m.reply(BAN_ALERT)

    user_id = m.from_user.id

    # Limit system
    is_allowed, remaining_time = await is_user_allowed(user_id)
    if not is_allowed:
        await m.reply_text(
            f" **आप 10 फाइल पहले ही भेज चुके हैं!**\nकृपया **{remaining_time} सेकंड** बाद फिर से प्रयास करें।",
            quote=True
        )
        return

    file_obj = m.document or m.video or m.audio

    file_name = file_obj.file_name if file_obj.file_name else None
    file_size = get_size(file_obj.file_size)

    try:
        # Forward safely
        msg = await safe_forward(c, m, BIN_CHANNEL)
        if msg is None:
            await m.reply_text("⚠️ फ़ाइल अग्रेषण में विफल। कृपया पुनः प्रयास करें।")
            return

        file_hash = get_hash(msg)
        stream = f"{URL}watch/{msg.id}?hash={file_hash}"
        download = f"{URL}{msg.id}?hash={file_hash}"
        file_link = f"https://t.me/{BOT_USERNAME}?start=file_{msg.id}"
        share_link = f"https://t.me/share/url?url={file_link}"

        # Respond to user
        await msg.reply_text(
            text=(
                f"Requested By: [{m.from_user.first_name}](tg://user?id={m.from_user.id})\n"
                f"User ID: {m.from_user.id}\n"
                f"Stream Link: {stream}"
            ),
            disable_web_page_preview=True,
            quote=True
        )

        # Caption with buttons
        if file_name:
            await m.reply_text(
                text=script.CAPTION_TXT.format(
                    CHANNEL, file_name, file_size, stream, download
                ),
                quote=True,
                disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(" STREAM ", url=stream),
                        InlineKeyboardButton(" DOWNLOAD ", url=download)
                    ],
                    [
                        InlineKeyboardButton(" GET FILE ", url=file_link),
                        InlineKeyboardButton(" SHARE ", url=share_link),
                        InlineKeyboardButton(" CLOSE ", callback_data="close_data")
                    ]
                ])
            )
        else:
            await m.reply_text(
                text=script.CAPTION2_TXT.format(
                    CHANNEL, file_name, file_size, download
                ),
                quote=True,
                disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(" DOWNLOAD ", url=download),
                        InlineKeyboardButton(" GET FILE ", url=file_link)
                    ],
                    [
                        InlineKeyboardButton(" Share ", url=share_link),
                        InlineKeyboardButton(" CLOSE ", callback_data="close_data")
                    ]
                ])
            )

    except Exception as e:
        print(f"Unexpected error: {e}")
