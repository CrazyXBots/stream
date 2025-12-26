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
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


@Client.on_message(
    filters.channel
    & (filters.document | filters.video)
    & ~filters.forwarded,
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
        msg = await broadcast.forward(chat_id=BIN_CHANNEL)

        file_hash = get_hash(msg)
        stream_link = f"{URL}watch/{msg.id}?hash={file_hash}"
        download_link = f"{URL}{msg.id}?hash={file_hash}"

        # ----------------------------
        # Notify BIN_CHANNEL
        # ----------------------------
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
                ],
            ]
        )

        # ----------------------------
        # Edit original message
        # ----------------------------
        try:
            await bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=broadcast.id,
                reply_markup=buttons,
            )

        except MessageIdInvalid:
            logger.warning(f"Cannot edit message {broadcast.id}, invalid ID.")

        except FloodWait as fw:
            wait_time = fw.value if hasattr(fw, "value") else fw.seconds
            logger.warning(f"FloodWait while editing — sleeping {wait_time}s")
            await asyncio.sleep(wait_time)

        except RPCError as rpc_e:
            logger.error(f"RPC error while editing message: {rpc_e}")

    except FloodWait as fw:
        wait = fw.value if hasattr(fw, "value") else fw.seconds
        logger.warning(f"FloodWait huge — sleeping {wait}s before resume.")
        await asyncio.sleep(wait)

    except asyncio.TimeoutError:
        logger.warning("Timeout in channel_receive_handler — sleeping 5s.")
        await asyncio.sleep(5)

    except Exception as e:
        # General unexpected errors
        logger.error(f"Unexpected error in channel_receive_handler: {e}")

        # Try notifying BIN_CHANNEL
        try:
            await bot.send_message(
                chat_id=BIN_CHANNEL,
                text=f"❌ **Error in stream2 handler:** `{e}`",
                disable_web_page_preview=True,
            )
        except Exception as notify_err:
            logger.error(f"Failed to send error to BIN_CHANNEL: {notify_err}")
