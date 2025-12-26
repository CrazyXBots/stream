import asyncio
import logging
import os
import random

from web.utils.file_properties import get_hash
from pyrogram import Client, filters, enums
from info import BIN_CHANNEL, BAN_CHNL, BANNED_CHANNELS, URL, BOT_USERNAME
from utils import get_size
from Script import script
from database.users_db import db
from pyrogram.errors import FloodWait, MessageIdInvalid, RPCError
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

# Dont Remove My Credit
# @MSLANDERS
# For Any Kind Of Error Ask Us In Support Group @MSLANDERS_HELP

@Client.on_message(
    filters.channel
    & (filters.document | filters.video)
    & ~filters.forwarded,
    group=-1,
)
async def channel_receive_handler(bot: Client, broadcast: Message):

    chat_id = broadcast.chat.id

    # Ignore banned channels
    if int(chat_id) in BAN_CHNL:
        logging.info(
            f"Channel {chat_id} is in BAN_CHNL — skipping stream link generation."
        )
        return

    # Database ban check
    if await db.is_banned(int(chat_id)) or int(chat_id) in BANNED_CHANNELS:
        logging.info(f"Channel {chat_id} is banned — leaving chat.")
        try:
            await bot.leave_chat(chat_id)
        except Exception as e:
            logging.error(f"Failed to leave banned channel {chat_id}: {e}")
        return

    try:
        # Extract file info
        file = broadcast.document or broadcast.video
        file_name = file.file_name if file else "Unknown File"
        file_size = get_size(file.file_size) if file else "Unknown Size"

        # Forward to BIN_CHANNEL
        msg = await broadcast.forward(chat_id=BIN_CHANNEL)

        # Build links
        stream_link = f"{URL}watch/{msg.id}?hash={get_hash(msg)}"
        download_link = f"{URL}{msg.id}?hash={get_hash(msg)}"

        # Notify BIN_CHANNEL
        await msg.reply_text(
            text=(
                f"**Channel Name:** `{broadcast.chat.title}`\n"
                f"**CHANNEL ID:** `{broadcast.chat.id}`\n"
                f"**Request URL:** {stream_link}"
            ),
            disable_web_page_preview=True,
            quote=True,
        )

        # Create buttons
        buttons = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(" STREAM ", url=stream_link),
                    InlineKeyboardButton(" DOWNLOAD ", url=download_link),
                ]
            ]
        )

        # Try to edit original channel message with buttons
        try:
            await bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=broadcast.id,
                reply_markup=buttons,
            )
        except MessageIdInvalid:
            logging.warning(
                f"Original message {broadcast.id} is invalid — cannot edit."
            )
        except FloodWait as fw:
            wait_time = getattr(fw, "value", None) or getattr(fw, "seconds", None) or 1
            logging.warning(f"FloodWait while editing message: sleeping {wait_time}s")
            await asyncio.sleep(wait_time)
            # No retry here to avoid multiple edits
        except RPCError as rpc_e:
            logging.error(f"RPC error editing original message: {rpc_e}")

    except FloodWait as fw:
        # If forwarding to BIN_CHANNEL raised a FloodWait
        wait_time = getattr(fw, "value", None) or getattr(fw, "seconds", None) or 1
        logging.warning(f"FloodWait in channel handler: sleeping {wait_time}s")
        await asyncio.sleep(wait_time)

    except asyncio.TimeoutError:
        logging.warning("TimeoutError in channel handler — waiting before retry.")
        await asyncio.sleep(5)

    except Exception as e:
        # Catch any other unexpected errors
        logging.error(f"Unexpected error in channel_receive_handler: {e}")
        try:
            await bot.send_message(
                chat_id=BIN_CHANNEL,
                text=f"❌ **Error in channel_receive_handler:** `{e}`",
                disable_web_page_preview=True,
            )
        except Exception as send_err:
            logging.error(f"Failed to send error message to BIN_CHANNEL: {send_err}")

# Dont Remove My Credit
# @MSLANDERS
# For Any Kind Of Error Ask Us In Support Group @MSLANDERS_HELP
