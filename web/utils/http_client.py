import aiohttp
import asyncio

TIMEOUT = aiohttp.ClientTimeout(
    total=None,
    connect=30,
    sock_connect=30,
    sock_read=300
)

connector = aiohttp.TCPConnector(
    limit=20,
    force_close=True,
    enable_cleanup_closed=True,
    ttl_dns_cache=300,
    keepalive_timeout=120
)

async def get_session():
    return aiohttp.ClientSession(
        timeout=TIMEOUT,
        connector=connector
    )
