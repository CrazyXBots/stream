import re
import math
import logging
import secrets
import mimetypes
import time
import os

from aiohttp import web
from aiohttp.http_exceptions import BadStatusLine

from info import *
from web.server import multi_clients, work_loads, web_server
from web.server.exceptions import FileNotFound, InvalidHash
from web.utils.custom_dl import get_file_path
from web.utils.render_template import render_page
from utils import get_readable_time
from web.utils import StartTime, __version__


routes = web.RouteTableDef()

# ================= ROOT =================

@routes.get("/", allow_head=True)
async def root_route_handler(request: web.Request):
    return web.json_response(
        {
            "server_status": "running",
            "uptime": get_readable_time(int(time.time() - StartTime)),
            "telegram_bot": BOT_USERNAME,
            "connected_bots": len(multi_clients),
            "loads": dict(
                ("bot" + str(c + 1), l)
                for c, (_, l) in enumerate(
                    sorted(work_loads.items(), key=lambda x: x[1], reverse=True)
                )
            ),
            "version": __version__,
        }
    )

# ================= WATCH PAGE (HTML ONLY) =================

@routes.get(r"/watch/{path:.+}", allow_head=True)
async def watch_handler(request: web.Request):
    try:
        path = request.match_info["path"]

        match = re.search(r"^([A-Za-z0-9_-]{6})/(\d+)$", path)
        if match:
            secure_hash = match.group(1)
            file_id = int(match.group(2))
        else:
            file_id = int(re.search(r"(\d+)", path).group(1))
            secure_hash = request.rel_url.query.get("hash")

        return web.Response(
            text=await render_page(file_id, secure_hash),
            content_type="text/html"
        )

    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)

    except FileNotFound as e:
        raise web.HTTPNotFound(text=e.message)

    except Exception as e:
        logging.exception(e)
        raise web.HTTPInternalServerError(text=str(e))

# ================= STREAM / DOWNLOAD =================

@routes.get(r"/{path:.+}", allow_head=True)
async def stream_handler(request: web.Request):
    try:
        path = request.match_info["path"]

        match = re.search(r"^([A-Za-z0-9_-]{6})/(\d+)$", path)
        if match:
            secure_hash = match.group(1)
            file_id = int(match.group(2))
        else:
            file_id = int(re.search(r"(\d+)", path).group(1))
            secure_hash = request.rel_url.query.get("hash")

        file_path = await get_file_path(file_id, secure_hash)
        file_size = os.path.getsize(file_path)

        range_header = request.headers.get("Range")
        start = 0

        if range_header:
            start = int(range_header.replace("bytes=", "").split("-")[0])

        mime_type, _ = mimetypes.guess_type(file_path)
        mime_type = mime_type or "application/octet-stream"

        headers = {
            "Content-Type": mime_type,
            "Accept-Ranges": "bytes",
            "Content-Length": str(file_size - start),
            "Content-Range": f"bytes {start}-{file_size - 1}/{file_size}",
        }

        response = web.StreamResponse(
            status=206,
            headers=headers
        )

        await response.prepare(request)

        with open(file_path, "rb") as f:
            f.seek(start)
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                await response.write(chunk)

        await response.write_eof()
        return response

    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)

    except FileNotFound as e:
        raise web.HTTPNotFound(text=e.message)

    except (AttributeError, BadStatusLine, ConnectionResetError):
        pass

    except Exception as e:
        logging.exception(e)
        raise web.HTTPInternalServerError(text=str(e))
