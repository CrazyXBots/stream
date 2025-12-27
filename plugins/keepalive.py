import asyncio
import logging
from pyrogram import Client

# ----------------- CONFIG -----------------
API_ID = 123456           # replace with your API ID
API_HASH = "your_api_hash"   # replace with your API Hash
BOT_TOKEN = "your_bot_token" # replace with your Bot Token

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------- CLIENT -----------------
bot_client = Client(
    "keepalive_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# ----------------- KEEPALIVE -----------------
async def keep_alive(interval: int = 20):
    """
    Sends periodic ping to Telegram servers to prevent idle timeout.
    
    :param interval: Ping interval in seconds (default: 20)
    """
    while True:
        try:
            await bot_client.send_ping()
            logging.info("Pinged Telegram server to keep alive")
        except Exception as e:
            logging.warning(f"KeepAlive ping failed: {e}")
        await asyncio.sleep(interval)

# ----------------- RUN KEEPALIVE -----------------
async def main():
    await bot_client.start()
    logging.info("KeepAlive bot started")
    # Start KeepAlive task
    asyncio.create_task(keep_alive())

    # Keep bot running
    await bot_client.idle()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("KeepAlive stopped manually")
