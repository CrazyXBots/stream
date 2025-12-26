import re
import math
import secrets
import mimetypes
import logging
import time

from aiohttp import web
from aiohttp.http_exceptions import BadStatusLine

from info import BOT_USERNAME, MULTI_CLIENT
from web.server import multi_clients, work_loads
from web.server.exceptions import FIleNotFound, InvalidHash
from web.utils.custom_dl import ByteStreamer
from utils import get_readable_time
from web.utils import StartTime, __version__
from web.utils.render_template import render_page

routes = web.RouteTableDef()

# ----------------------------
# Logger
# ----------------------------
logger = logging.getLogger("stream_routes")
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


# ----------------------------------------------------------
# Root status route
# ----------------------------------------------------------
@routes.get("/", allow_head=True)
async def root_route_handler(_):
    return web.json_response(
        {
            "server_status": "running",
            "uptime": get_readable_time(time.time() - StartTime),
            "telegram_bot": "@" + BOT_USERNAME,
            "connected_bots": len(multi_clients),
            "loads": {
                f"bot{c+1}": load
                for c, (_, load) in enumerate(
                    sorted(work_loads.items(), key=lambda x: x[1], reverse=True)
                )
            },
            "version": __version__,
        }
    )


# ----------------------------------------------------------
# Render HTML watch page
# ----------------------------------------------------------
@routes.get(r"/watch/{path:\S+}", allow_head=True)
async def watch_page_handler(request: web.Request):
    try:
        path = request.match_info["path"]

        match = re.search(r"^([a-zA-Z0-9_-]{6})(\d+)$", path)

        if match:
            secure_hash = match.group(1)
            file_id = int(match.group(2))
        else:
            # we match digits in the path like 1234abc → then get hash from query
            match2 = re.search(r"(\d+)", path)
            if not match2:
                raise web.HTTPBadRequest(text="Invalid path")
            file_id = int(match2.group(1))
            secure_hash = request.rel_url.query.get("hash")

        # render html
        html = await render_page(file_id, secure_hash)
        return web.Response(text=html, content_type="text/html")

    except InvalidHash as e:
        return web.HTTPForbidden(text=e.message)

    except FIleNotFound as e:
        return web.HTTPNotFound(text=e.message)

    except Exception as e:
        logger.error(f"Error in watch_page_handler: {e}")
        return web.HTTPInternalServerError(text="Internal Server Error")


# ----------------------------------------------------------
# Media streamer route (serves bytes with Range support)
# ----------------------------------------------------------
@routes.get(r"/{path:\S+}", allow_head=True)
async def media_route_handler(request: web.Request):
    try:
        path = request.match_info["path"]

        match = re.search(r"^([a-zA-Z0-9_-]{6})(\d+)$", path)

        if match:
            secure_hash = match.group(1)
            file_id = int(match.group(2))
        else:
            match2 = re.search(r"(\d+)", path)
            if not match2:
                raise web.HTTPBadRequest(text="Invalid path")
            file_id = int(match2.group(1))
            secure_hash = request.rel_url.query.get("hash")

        return await media_streamer(request, file_id, secure_hash)

    except InvalidHash as e:
        return web.HTTPForbidden(text=e.message)

    except FIleNotFound as e:
        return web.HTTPNotFound(text=e.message)

    except (AttributeError, BadStatusLine, ConnectionResetError):
        # If something goes wrong in the stream mid‑way
        return web.Response(status=500, text="Stream interrupted")

    except Exception as e:
        logger.error(f"Error in media_route_handler: {e}")
        return web.HTTPInternalServerError(text="Internal Server Error")


# ----------------------------------------------------------
# Core streaming logic
# ----------------------------------------------------------

class_cache = {}


async def media_streamer(request: web.Request, file_id: int, secure_hash: str):
    """
    Core logic to stream media bytes with support for Range headers.
    """

    range_header = request.headers.get("Range", None)

    # Pick the least busy client
    index = min(work_loads, key=work_loads.get)
    faster_client = multi_clients[index]

    if MULTI_CLIENT:
        logger.info(f"Client {index} serving {request.remote}")

    # Reuse or create ByteStreamer
    if faster_client in class_cache:
        streamer = class_cache[faster_client]
    else:
        streamer = ByteStreamer(faster_client)
        class_cache[faster_client] = streamer

    # Validate file ID + hash
    try:
        file_info = await streamer.get_file_properties(file_id)
    except Exception as e:
        logger.error(f"File lookup failed: {e}")
        return web.HTTPNotFound(text="File not found")

    # If hash mismatches → reject
    if secure_hash is None or file_info.unique_id[:6] != secure_hash:
        return web.HTTPForbidden(text="Invalid hash")

    total_size = file_info.file_size

    # Parse Range
    if range_header:
        try:
            hdr = range_header.strip().replace("bytes=", "")
            from_bytes, until_bytes = hdr.split("-")
            start = int(from_bytes)
            end = int(until_bytes) if until_bytes else total_size - 1
        except Exception:
            return web.Response(status=416, text="416 Range Not Satisfiable")
    else:
        start = 0
        end = total_size - 1

    if start < 0 or end >= total_size or end < start:
        return web.Response(
            status=416,
            text="416 Range Not Satisfiable",
            headers={"Content-Range": f"bytes */{total_size}"},
        )

    # Compute chunking
    chunk_size = 1024 * 1024
    end = min(end, total_size - 1)

    offset_base = start - (start % chunk_size)
    first_cut = start - offset_base
    last_cut = end % chunk_size + 1

    req_length = end - start + 1
    part_count = (
        math.ceil((end + 1) / chunk_size)
        - math.floor(offset_base / chunk_size)
    )

    # Create body generator
    try:
        body_gen = streamer.yield_file(
            file_info,
            index,
            offset_base,
            first_cut,
            last_cut,
            part_count,
            chunk_size,
        )
    except Exception as e:
        logger.error(f"Error creating body generator: {e}")
        return web.HTTPInternalServerError(text="Failed to stream")

    # Determine mime and file name safely
    mime_type = file_info.mime_type or "application/octet-stream"
    file_name = file_info.file_name or f"{secrets.token_hex(4)}.bin"

    guessed = mimetypes.guess_type(file_name)[0]
    mime_type = guessed or mime_type

    # Build headers
    headers = {
        "Content-Type": mime_type,
        "Accept-Ranges": "bytes",
        "Content-Range": f"bytes {start}-{end}/{total_size}",
        "Content-Length": str(req_length),
        "Content-Disposition": f'attachment; filename="{file_name}"',
    }

    return web.Response(
        status=(206 if range_header else 200),
        body=body_gen,
        headers=headers,
    )
