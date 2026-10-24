import mimetypes
import os
import re
import uuid

import boto3
import psycopg
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse

app = FastAPI(title="File to URL API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Range", "Accept-Ranges", "Content-Length"],
)

# ---------- Config (environment variables) ----------
DATABASE_URL = os.environ.get("DATABASE_URL")
B2_KEY_ID = os.environ.get("B2_KEY_ID")
B2_APP_KEY = os.environ.get("B2_APP_KEY")
B2_BUCKET = os.environ.get("B2_BUCKET")
B2_ENDPOINT = os.environ.get("B2_ENDPOINT")  # e.g. https://s3.us-west-004.backblazeb2.com
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL")  # optional, e.g. https://myapp.koyeb.app
MAX_MB = int(os.environ.get("MAX_MB", "100"))
MAX_SIZE = MAX_MB * 1024 * 1024
CHUNK = 256 * 1024

# ---------- Extension rules ----------
AUDIO_EXT = {".mp3", ".wav", ".m4a", ".aac", ".ogg", ".opus", ".flac", ".wma", ".amr"}
VIDEO_EXT = {".mp4", ".mkv", ".mov", ".avi", ".webm", ".3gp", ".flv", ".m4v"}
IMAGE_EXT = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".heic", ".svg"}

KEY_RE = re.compile(r"^[a-f0-9]{12}\.[A-Za-z0-9]{1,8}$")


def pick_extension(filename: str, content_type: str) -> str:
    ext = os.path.splitext(filename or "")[1].lower()
    ct = (content_type or "").lower()

    if ct.startswith("audio/") or ext in AUDIO_EXT:
        return ext if ext in AUDIO_EXT else ".mp3"
    if ct.startswith("video/") or ext in VIDEO_EXT:
        return ext if ext in VIDEO_EXT else ".mp4"
    if ct.startswith("image/") or ext in IMAGE_EXT:
        return ext if ext in IMAGE_EXT else ".jpg"
    # documents / everything else: keep original extension, else .txt
    if ext and len(ext) <= 8:
        return ext
    return ".txt"


def parse_range(header: str, total: int):
    """Return (start, end) inclusive, or None if invalid/unsupported."""
    m = re.match(r"^bytes=(\d*)-(\d*)$", (header or "").strip())
    if not m:
        return None
    s, e = m.groups()
    if s == "" and e == "":
        return None
    if s == "":  # suffix: last N bytes
        n = int(e)
        if n == 0:
            return None
        start = max(total - n, 0)
        end = total - 1
    else:
        start = int(s)
        end = int(e) if e != "" else total - 1
        end = min(end, total - 1)
    if start > end or start >= total:
        return None
    return start, end


# ---------- Storage (Backblaze B2, S3 compatible, PRIVATE bucket) ----------
_s3 = None


def s3():
    global _s3
    if _s3 is None:
        if not all([B2_KEY_ID, B2_APP_KEY, B2_BUCKET, B2_ENDPOINT]):
            raise HTTPException(500, "B2 environment variables are not set")
        _s3 = boto3.client(
            "s3",
            endpoint_url=B2_ENDPOINT,
            aws_access_key_id=B2_KEY_ID,
            aws_secret_access_key=B2_APP_KEY,
            config=Config(signature_version="s3v4", retries={"max_attempts": 3}),
        )
    return _s3


def base_url(request: Request) -> str:
    if PUBLIC_BASE_URL:
        return PUBLIC_BASE_URL.rstrip("/")
    return str(request.base_url).rstrip("/")


# ---------- Database (Neon Postgres) ----------
CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS uploads (
    id SERIAL PRIMARY KEY,
    name TEXT,
    key TEXT NOT NULL,
    size BIGINT NOT NULL,
    content_type TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


def db():
    if not DATABASE_URL:
        raise HTTPException(500, "DATABASE_URL is not set")
    conn = psycopg.connect(DATABASE_URL, autocommit=True)
    conn.execute(CREATE_TABLE)
    return conn


# ---------- Routes ----------
@app.get("/")
def home():
    return {
        "status": "ok",
        "upload": "POST /upload (form-data, field: file)",
        "file": "GET /f/<name>",
        "stats": "GET /stats",
        "files": "GET /files?limit=20",
        "max_mb": MAX_MB,
    }


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/upload")
def upload(request: Request, file: UploadFile = File(...)):
    cl = request.headers.get("content-length")
    if cl and int(cl) > MAX_SIZE + 1024 * 1024:
        raise HTTPException(413, f"File too large (max {MAX_MB}MB)")

    f = file.file
    f.seek(0, os.SEEK_END)
    size = f.tell()
    f.seek(0)

    if size == 0:
        raise HTTPException(400, "Empty file")
    if size > MAX_SIZE:
        raise HTTPException(413, f"File too large (max {MAX_MB}MB)")

    ext = pick_extension(file.filename, file.content_type)
    key = f"{uuid.uuid4().hex[:12]}{ext}"
    content_type = (
        mimetypes.guess_type(key)[0] or file.content_type or "application/octet-stream"
    )

    try:
        s3().upload_fileobj(f, B2_BUCKET, key, ExtraArgs={"ContentType": content_type})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Storage upload failed: {e}")

    with db() as conn:
        conn.execute(
            "INSERT INTO uploads (name, key, size, content_type) VALUES (%s, %s, %s, %s)",
            (file.filename, key, size, content_type),
        )

    return {"url": f"{base_url(request)}/f/{key}", "size": size}


@app.api_route("/f/{key}", methods=["GET", "HEAD"])
def serve(key: str, request: Request):
    if not KEY_RE.match(key):
        raise HTTPException(404, "Not found")

    try:
        head = s3().head_object(Bucket=B2_BUCKET, Key=key)
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        if code in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(404, "Not found")
        raise HTTPException(502, "Storage error")

    total = int(head["ContentLength"])
    ctype = mimetypes.guess_type(key)[0] or head.get("ContentType") or "application/octet-stream"

    headers = {
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=31536000, immutable",
        "Content-Disposition": f'inline; filename="{key}"',
    }

    status = 200
    start, end = 0, total - 1
    range_header = request.headers.get("range")
    if range_header:
        r = parse_range(range_header, total)
        if r is None:
            return Response(
                status_code=416, headers={"Content-Range": f"bytes */{total}"}
            )
        start, end = r
        status = 206
        headers["Content-Range"] = f"bytes {start}-{end}/{total}"

    length = end - start + 1
    headers["Content-Length"] = str(length)

    if request.method == "HEAD":
        return Response(status_code=status, headers=headers, media_type=ctype)

    kwargs = {"Bucket": B2_BUCKET, "Key": key}
    if status == 206:
        kwargs["Range"] = f"bytes={start}-{end}"
    try:
        obj = s3().get_object(**kwargs)
    except ClientError:
        raise HTTPException(502, "Storage error")

    body = obj["Body"]

    def stream():
        try:
            for chunk in body.iter_chunks(CHUNK):
                yield chunk
        finally:
            body.close()

    return StreamingResponse(stream(), status_code=status, headers=headers, media_type=ctype)


@app.get("/stats")
def stats():
    with db() as conn:
        total, size, last = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(size), 0), MAX(created_at) FROM uploads"
        ).fetchone()
    return {
        "total_files": total,
        "total_size_mb": round(int(size) / 1024 / 1024, 2),
        "last_upload": last.isoformat() if last else None,
    }


@app.get("/files")
def files(request: Request, limit: int = 20):
    limit = max(1, min(limit, 100))
    with db() as conn:
        rows = conn.execute(
            "SELECT id, name, key, size, created_at FROM uploads ORDER BY id DESC LIMIT %s",
            (limit,),
        ).fetchall()
    b = base_url(request)
    return [
        {"id": i, "name": n, "url": f"{b}/f/{k}", "size": s, "uploaded_at": c.isoformat()}
        for i, n, k, s, c in rows
    ]
