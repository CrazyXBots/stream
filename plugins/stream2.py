import os
import time
import asyncio
from database.users_db import db
from web.utils.file_properties import get_hash
from pyrogram import Client, filters, enums
from info import URL, BOT_USERNAME, BIN_CHANNEL, BAN_ALERT, FSUB, CHANNEL
from Script import script
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait
from plugins.mslandersbot import is_user_joined, is_user_allowed
from web.utils.safe_stream import safe_stream  # added safe streaming
from web.utils.safe_sender import send
from safe_stream import download

#Dont Remove My Credit @MSLANDERS
# For Any Kind Of Error Ask Us In Support Group @MSLANDERS_HELP

@Client.on_message((filters.private) & (filters.document | filters.video | filters.audio), group=4)
async def private_receive_handler(c: Client, m: Message):

    if FSUB:
        if not await is_user_joined(c, m):
            return

    ban_chk = await db.is_banned(int(m.from_user.id))
    if ban_chk == True:
        return await m.reply(BAN_ALERT)

    user_id = m.from_user.id

    # check limit for user
    is_allowed, remaining_time = await is_user_allowed(user_id)
    if not is_allowed:
        await m.reply_text(
            f" **आप 10 फाइल पहले ही भेज चुके हैं!**\nकृपया **{remaining_time} सेकंड** बाद फिर से प्रयास करें।",
            quote=True
        )
        return

    file_id = m.document or m.video or m.audio
    file_name = file_id.file_name if file_id.file_name else None
    file_size = get_size(file_id.file_size)

    try:
        msg = await safe_stream(m.forward, chat_id=BIN_CHANNEL)

        stream = f"{URL}watch/{msg.id}?hash={get_hash(msg)}"
        download = f"{URL}{msg.id}?hash={get_hash(msg)}"
        file_link = f"https://t.me/{BOT_USERNAME}?start=file_{msg.id}"
        share_link = f"https://t.me/share/url?url={file_link}"

        await safe_stream(msg.reply_text,
            text=f"Requested By: [{m.from_user.first_name}](tg://user?id={m.from_user.id})\n"
                 f"User ID: {m.from_user.id}\nStream Link: {stream}",
            disable_web_page_preview=True, quote=True
        )

        if file_name:
            await safe_stream(m.reply_text,
                text=script.CAPTION_TXT.format(CHANNEL, file_name, file_size, stream, download),
                quote=True, disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(" STREAM ", url=stream),
                        InlineKeyboardButton(" DOWNLOAD ", url=download)
                    ],
                    [
                        InlineKeyboardButton('GET FILE', url=file_link),
                        InlineKeyboardButton('SHARE', url=share_link),
                        InlineKeyboardButton('CLOSE', callback_data='close_data')
                    ]
                ])
            )
        else:
            await safe_stream(m.reply_text,
                text=script.CAPTION2_TXT.format(CHANNEL, file_name, file_size, download),
                quote=True, disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(" DOWNLOAD ", url=download),
                        InlineKeyboardButton('GET FILE', url=file_link)
                    ],
                    [
                        InlineKeyboardButton('Share', url=share_link),
                        InlineKeyboardButton('CLOSE', callback_data='close_data')
                    ]
                ])
            )

    except FloodWait as e:
        print(f"Sleeping for {e.value}s")
        await asyncio.sleep(e.value)

        await safe_stream(c.send_message,
            chat_id=BIN_CHANNEL,
            text=f"Gᴏᴛ FʟᴏᴏᴅWᴀɪᴛ ᴏғ {e.value}s from [{m.from_user.first_name}](tg://user?id={m.from_user.id})\n\n** :** `{m.from_user.id}`",
            disable_web_page_preview=True
        )

#Dont Remove My Credit @MSLANDERS
# For Any Kind Of Error Ask Us In Support Group @MSLANDERS_HELP
