import asyncio
import logging

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, MessageIdInvalid, RPCError
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from database.users_db import db
from web.utils.file_properties import get_hash
from utils import get_size
from info import BIN_CHANNEL, BAN_CHNL, BANNED_CHANNELS, URL, BOT_USERNAME

# ----------------------------
# Logger setup
# ----------------------------
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


async def safe_forward(bot: Client, message: Message, chat_id: int):
    """Forward with FloodWait handling."""
    while True:
        try:
            return await message.forward(chat_id=chat_id)
        except FloodWait as e:
            wait_time = getattr(e, "value", None) or getattr(e, "seconds", None) or 5
            logger.warning(f"FloodWait in safe_forward: sleeping {wait_time}s")
            await asyncio.sleep(wait_time)
        except RPCError as e:
            logger.error(f"RPC error in safe_forward: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected safe_forward error: {e}")
            return None


async def safe_edit_buttons(bot: Client, chat_id: int, msg_id: int, markup):
    """Edit message buttons with FloodWait handling."""
    while True:
        try:
            return await bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=msg_id,
                reply_markup=markup,
            )
        except FloodWait as e:
            wait_time = getattr(e, "value", None) or getattr(e, "seconds", None) or 5
            logger.warning(f"FloodWait while editing buttons: sleeping {wait_time}s")
            await asyncio.sleep(wait_time)
        except MessageIdInvalid:
            logger.warning(f"Invalid message ID — cannot edit buttons for {msg_id}")
            return None
        except RPCError as e:
            logger.error(f"RPC error in editing buttons: {e}")
            return None
        except Exception as e:
            logger.error(f"Error editing buttons: {e}")
            return None


@Client.on_message(
    filters.channel & (filters.document | filters.video) & ~filters.forwarded,
    group=-1,
)
async def channel_receive_handler(bot: Client, broadcast: Message):

    chat_id = broadcast.chat.id

    # ----------------------------
    # Skip banned channels
    # ----------------------------
    if chat_id in BAN_CHNL or chat_id in BANNED_CHANNELS:
        logger.info(f"Channel {chat_id} is banned or blocked — skipping.")
        try:
            await bot.leave_chat(chat_id)
        except Exception as leave_err:
            logger.warning(f"Failed to leave banned channel {chat_id}: {leave_err}")
        return

    try:
        # ----------------------------
        # Extract file info
        # ----------------------------
        file_obj = broadcast.document or broadcast.video
        file_name = file_obj.file_name if file_obj else "Unknown File"
        file_size = get_size(file_obj.file_size) if file_obj else "Unknown Size"

        # ----------------------------
        # Forward to BIN_CHANNEL
        # ----------------------------
        msg = await safe_forward(bot, broadcast, BIN_CHANNEL)
        if not msg:
            logger.warning("Failed to forward to BIN_CHANNEL.")
            return

        # ----------------------------
        # Compute hash & links
        # ----------------------------
        try:
            file_hash = get_hash(msg)
        except Exception as e:
            logger.error(f"get_hash error: {e}")
            file_hash = ""

        stream_link = f"{URL}watch/{msg.id}?hash={file_hash}"
        download_link = f"{URL}{msg.id}?hash={file_hash}"

        # Notify in BIN_CHANNEL
        await msg.reply_text(
            text=(
                f"**Channel:** `{broadcast.chat.title}`\n"
                f"**CHANNEL ID:** `{broadcast.chat.id}`\n"
                f"**File:** `{file_name}` ({file_size})\n\n"
                f"🔗 **Stream Link:** {stream_link}"
            ),
            disable_web_page_preview=True,
            quote=True,
        )

        # ----------------------------
        # Build buttons
        # ----------------------------
        buttons = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("📺 STREAM", url=stream_link),
                    InlineKeyboardButton("⬇️ DOWNLOAD", url=download_link),
                ]
            ]
        )

        # Edit original channel message
        await safe_edit_buttons(bot, chat_id, broadcast.id, buttons)

    except asyncio.TimeoutError:
        logger.warning("Timeout in stream2 handler — sleeping then continuing.")
        await asyncio.sleep(5)

    except Exception as e:
        logger.error(f"Unexpected error in stream2 handler: {e}")
        # Try notifying BIN_CHANNEL
        try:
            await bot.send_message(
                chat_id=BIN_CHANNEL,
                text=f"❌ **Error in stream2 handler:** `{e}`",
                disable_web_page_preview=True,
            )
        except Exception as notify_err:
            logger.error(f"Failed to send error to BIN_CHANNEL: {notify_err}")
