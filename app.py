from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
import os

app = FastAPI()

VIDEO_FILE = "video.mp4"  # TEST with local file first

CHUNK_SIZE = 1024 * 512  # 512KB

@app.get("/download")
async def stream_video(request: Request):
    if not os.path.exists(VIDEO_FILE):
        raise HTTPException(status_code=404)

    file_size = os.path.getsize(VIDEO_FILE)
    range_header = request.headers.get("range")

    start = 0
    end = file_size - 1

    if range_header:
        bytes_range = range_header.replace("bytes=", "").split("-")
        start = int(bytes_range[0])
        if bytes_range[1]:
            end = int(bytes_range[1])

    length = end - start + 1

    def generator():
        with open(VIDEO_FILE, "rb") as f:
            f.seek(start)
            remaining = length
            while remaining > 0:
                read_size = min(CHUNK_SIZE, remaining)
                data = f.read(read_size)
                if not data:
                    break
                remaining -= len(data)
                yield data

    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(length),
        "Content-Type": "video/mp4",
        "Cache-Control": "no-cache",
    }

    return StreamingResponse(
        generator(),
        status_code=206,
        headers=headers,
              )
