#
#  Copyright 2026 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from __future__ import annotations

import base64
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import hashlib
import json
import time
import uuid

from rag.utils.redis_conn import REDIS_CONN


FrameSender = Callable[[dict], Awaitable[None]]
ResponseWaiter = Callable[[str], Awaitable[dict]]

MAX_CHUNK_BYTES = 512 * 1024


@dataclass(frozen=True)
class TemporaryMedia:
    media_id: str
    media_type: str
    expires_at: int


class MediaUploadFailure(Exception):
    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason


class TemporaryMediaCache:
    def __init__(self, ttl_seconds: int):
        self.ttl_seconds = ttl_seconds
        self._memory: dict[str, tuple[int, TemporaryMedia]] = {}

    def get(self, bot_id: str, media_type: str, digest: str) -> TemporaryMedia | None:
        key = self._key(bot_id, media_type, digest)
        cached = self._redis_get(key) or self._memory_get(key)
        if cached and cached.expires_at > int(time.time()):
            return cached
        return None

    def set(self, bot_id: str, media_type: str, digest: str, media: TemporaryMedia) -> None:
        key = self._key(bot_id, media_type, digest)
        ttl = min(max(self.ttl_seconds, 1), max(media.expires_at - int(time.time()), 1))
        self._memory[key] = (int(time.time()) + ttl, media)
        self._redis_set(key, media, ttl)

    @staticmethod
    def _key(bot_id: str, media_type: str, digest: str) -> str:
        return f"wecom:aibot:temp-media:{bot_id}:{media_type}:{digest}"

    def _memory_get(self, key: str) -> TemporaryMedia | None:
        item = self._memory.get(key)
        if not item:
            return None
        expires_at, media = item
        if expires_at <= int(time.time()):
            self._memory.pop(key, None)
            return None
        return media

    @staticmethod
    def _redis_get(key: str) -> TemporaryMedia | None:
        if not REDIS_CONN.is_alive():
            return None
        try:
            raw = REDIS_CONN.get(key)
            if not raw:
                return None
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            data = json.loads(raw)
            return TemporaryMedia(
                media_id=data["media_id"],
                media_type=data["media_type"],
                expires_at=int(data["expires_at"]),
            )
        except Exception:
            return None

    @staticmethod
    def _redis_set(key: str, media: TemporaryMedia, ttl_seconds: int) -> None:
        if not REDIS_CONN.is_alive():
            return
        try:
            value = json.dumps(media.__dict__, ensure_ascii=False)
            if hasattr(REDIS_CONN, "set"):
                REDIS_CONN.set(key, value, ttl_seconds)
            elif getattr(REDIS_CONN, "REDIS", None):
                REDIS_CONN.REDIS.set(key, value, ex=ttl_seconds)
        except Exception:
            return


class WeComTemporaryMediaUploader:
    def __init__(self, cache: TemporaryMediaCache):
        self.cache = cache

    async def upload(
        self,
        *,
        bot_id: str,
        media_type: str,
        filename: str,
        data: bytes,
        send_frame: FrameSender,
        wait_response: ResponseWaiter | None = None,
    ) -> TemporaryMedia:
        digest = hashlib.sha256(data).hexdigest()
        cached = self.cache.get(bot_id, media_type, digest)
        if cached:
            return cached

        if wait_response is None:
            raise MediaUploadFailure("response_waiter_unavailable", "Temporary media upload requires response waiting.")

        total_chunks = max((len(data) + MAX_CHUNK_BYTES - 1) // MAX_CHUNK_BYTES, 1)
        upload_id = await self._init_upload(media_type, filename, data, total_chunks, send_frame, wait_response)
        for index in range(total_chunks):
            chunk = data[index * MAX_CHUNK_BYTES : (index + 1) * MAX_CHUNK_BYTES]
            await self._upload_chunk(upload_id, index, chunk, send_frame, wait_response)

        media = await self._finish_upload(upload_id, media_type, send_frame, wait_response)
        self.cache.set(bot_id, media_type, digest, media)
        return media

    async def _init_upload(
        self,
        media_type: str,
        filename: str,
        data: bytes,
        total_chunks: int,
        send_frame: FrameSender,
        wait_response: ResponseWaiter,
    ) -> str:
        req_id = _req_id()
        await send_frame(
            {
                "cmd": "aibot_upload_media_init",
                "headers": {"req_id": req_id},
                "body": {
                    "type": media_type,
                    "filename": filename,
                    "total_size": len(data),
                    "total_chunks": total_chunks,
                    "md5": hashlib.md5(data).hexdigest(),
                },
            }
        )
        body = _ok_response(await wait_response(req_id))
        upload_id = body.get("upload_id") or ""
        if not upload_id:
            raise MediaUploadFailure("upload_init_failed", "Temporary media upload did not return upload_id.")
        return upload_id

    async def _upload_chunk(
        self,
        upload_id: str,
        chunk_index: int,
        chunk: bytes,
        send_frame: FrameSender,
        wait_response: ResponseWaiter,
    ) -> None:
        req_id = _req_id()
        await send_frame(
            {
                "cmd": "aibot_upload_media_chunk",
                "headers": {"req_id": req_id},
                "body": {
                    "upload_id": upload_id,
                    "chunk_index": chunk_index,
                    "base64_data": base64.b64encode(chunk).decode("ascii"),
                },
            }
        )
        _ok_response(await wait_response(req_id))

    async def _finish_upload(
        self,
        upload_id: str,
        media_type: str,
        send_frame: FrameSender,
        wait_response: ResponseWaiter,
    ) -> TemporaryMedia:
        req_id = _req_id()
        await send_frame(
            {
                "cmd": "aibot_upload_media_finish",
                "headers": {"req_id": req_id},
                "body": {"upload_id": upload_id},
            }
        )
        body = _ok_response(await wait_response(req_id))
        media_id = body.get("media_id") or ""
        if not media_id:
            raise MediaUploadFailure("upload_finish_failed", "Temporary media upload did not return media_id.")
        created_at = int(body.get("created_at") or time.time())
        return TemporaryMedia(media_id=media_id, media_type=body.get("type") or media_type, expires_at=created_at + 259200)


def _ok_response(response: dict) -> dict:
    errcode = response.get("errcode", (response.get("body") or {}).get("errcode", 0))
    try:
        errcode = int(errcode)
    except (TypeError, ValueError):
        errcode = -1
    if errcode != 0:
        raise MediaUploadFailure("wecom_error", "Temporary media upload failed.")
    return response.get("body") or {}


def _req_id() -> str:
    return str(uuid.uuid4())
