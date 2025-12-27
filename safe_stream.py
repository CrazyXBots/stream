import asyncio
import aiohttp
import logging
from pyrogram import Client, filters
from pyrogram.types import Message
import os

# ------------------- CONFIG -------------------
API_ID = "22525529"               # Replace with your Pyrogram API ID
API_HASH = "840111f82bbd1d2d3de5055afccf6a92"    # Replace with your Pyrogram API HASH
BOT_TOKEN = "7402038391:AAGptZxMwOHX_ay7Qr853qzoWS4r1_mxAnM"  # Replace with your bot token

MAX_RETRIES = 3
TIMEOUT = 120  # seconds
DOWNLOAD_FOLDER = "./downloads"
# ----------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create download folder if it doesn't exist
if not os.path.exists(DOWNLOAD_FOLDER):
    os.makedirs(DOWNLOAD_FOLDER)

app = Client("SafeStreamBot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


async def download(url: str, destination: str) -> bool:
    """
    Downloads a file safely with retries and timeout.
    Returns True if successful, False otherwise.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"[Attempt {attempt}] Downloading: {url}")
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    with open(destination, "wb") as f:
                        while True:
                            chunk = await response.content.read(1024 * 1024)  # 1MB chunks
                            if not chunk:
                                break
                            f.write(chunk)
            logger.info(f"Download completed: {destination}")
            return True
        except Exception as e:
            logger.error(f"[Attempt {attempt}] Failed to download {url}: {e}")
            if attempt == MAX_RETRIES:
                logger.error(f"Failed to download {url} after {MAX_RETRIES} attempts.")
                return False
            await asyncio.sleep(2)  # wait 2 seconds before retrying


@app.on_message(filters.document | filters.video)
async def handle_file(client: Client, message: Message):
    """
    Handles document/video messages and downloads them safely.
    """
    file_name = message.document.file_name if message.document else message.video.file_name
    destination = os.path.join(DOWNLOAD_FOLDER, file_name)

    # Get file download URL
    file_url = await message.get_file()
    success = await download(file_url, destination)

    if success:
        await message.reply_text(f"✅ File downloaded successfully: {file_name}")
    else:
        await message.reply_text(f"❌ Failed to download file after {MAX_RETRIES} attempts.")


if __name__ == "__main__":
    logger.info("Starting SafeStreamBot...")
    app.run()
