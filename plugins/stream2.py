import asyncio
import os
import random

from web.utils.file_properties import get_hash
from pyrogram import Client, filters
from info import BIN_CHANNEL, BAN_CHNL, BANNED_CHANNELS, URL, BOT_USERNAME
from utils import get_size
from Script import script
from database.users_db import db
from pyrogram.errors import FloodWait, RPCError, BadRequest
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

# Dont Remove My Credit
# @MSLANDERS
# For Any Kind Of Error Ask Us In Support Group @MSLANDERS_HELP

async def safe_forward_channel(bot: Client, message: Message, chat_id: int):
    """
    Forward safely with FloodWait handling.
    """
    while True:
        try:
            return await message.forward(chat_id=chat_id)
        except FloodWait as e:
            wait_time = getattr(e, "value", None) or getattr(e, "seconds", None) or 5
            print(f"FloodWait for {wait_time}s in safe_forward_channel")
            await asyncio.sleep(wait_time)
        except RPCError as e:
            print(f"RPC Error in forward: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error in forward: {e}")
            return None


async def safe_edit_buttons(bot: Client,
                            chat_id: int,
                            message_id: int,
                            markup: InlineKeyboardMarkup):
    """
    Safely update message buttons with FloodWait support.
    """
    while True:
        try:
            return await bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=markup,
            )
        except FloodWait as e:
            wait_time = getattr(e, "value", None) or getattr(e, "seconds", None) or 5
            print(f"FloodWait for {wait_time}s in safe_edit_buttons")
            await asyncio.sleep(wait_time)
        except BadRequest as e:
            print(f"BadRequest while editing buttons: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error while editing buttons: {e}")
            return None


@Client.on_message(
    filters.channel & (filters.document | filters.video) & ~filters.forwarded,
    group=-1,
)
async def channel_receive_handler(bot: Client, broadcast: Message):

    chat_id = int(broadcast.chat.id)

    # Check ban lists
    if chat_id in BAN_CHNL:
        print("Channel in BAN_CHNL, no streaming link supplied.")
        return

    ban_status = await db.is_banned(chat_id)
    if chat_id in BANNED_CHANNELS or ban_status:
        await bot.leave_chat(chat_id)
        return

    try:
        # Extract file details
        file = broadcast.document or broadcast.video
        file_name = file.file_name if file else "Unknown File"
        file_size = get_size(file.file_size) if file else "Unknown Size"

        # Forward to bin channel safely
        msg = await safe_forward_channel(bot, broadcast, BIN_CHANNEL)
        if not msg:
            print("Failed to forward broadcast to BIN_CHANNEL.")
            return

        file_hash = get_hash(msg)

        # Generate links
        stream_link = f"{URL}watch/{msg.id}?hash={file_hash}"
        download_link = f"{URL}{msg.id}?hash={file_hash}"

        # Notify BIN_CHANNEL
        await msg.reply_text(
            text=(
                f"**Channel Name:** `{broadcast.chat.title}`\n"
                f"**CHANNEL ID:** `{broadcast.chat.id}`\n"
                f"**STREAM LINK:** {stream_link}"
            ),
            disable_web_page_preview=True,
            quote=True,
        )

        # Build buttons
        buttons = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(" STREAM ", url=stream_link),
                    InlineKeyboardButton(" DOWNLOAD ", url=download_link),
                ]
            ]
        )

        # Update channel message with buttons
        await safe_edit_buttons(bot, chat_id, broadcast.id, buttons)

    except asyncio.exceptions.TimeoutError:
        print("Request Timed Out! Waiting before retry.")
        await asyncio.sleep(5)

    except Exception as exc:
        # Log errors and send to BIN_CHANNEL for visibility
        error_msg = f"❌ **Error:** `{exc}`"
        print(f"❌ Can't handle channel receive: {exc}")
        try:
            await bot.send_message(
                chat_id=BIN_CHANNEL,
                text=error_msg,
                disable_web_page_preview=True,
            )
        except Exception:
            pass
