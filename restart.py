import os
import sys
import time
import logging

logging.basicConfig(level=logging.INFO)

RESTART_INTERVAL = 1500  # 10 minutes

def main():
    logging.info("Bot started with auto-restart enabled")
    start_time = time.time()

    while True:
        if time.time() - start_time >= RESTART_INTERVAL:
            logging.warning("Restarting bot...")
            os.execv(sys.executable, [sys.executable] + sys.argv)

        time.sleep(5)

if __name__ == "__main__":
    main()
