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
from dataclasses import dataclass
import mimetypes
from pathlib import Path
from typing import Awaitable, Callable
from urllib.parse import urlparse

import httpx
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7

from common.ssrf_guard import assert_url_is_safe, pin_dns_global

from api.apps.services.wecom_aibot.config import WeComAIBotConfig
from api.apps.services.wecom_aibot.protocol import WeComIncomingMedia


FetchMedia = Callable[[WeComIncomingMedia], Awaitable[tuple[bytes, str]]]


@dataclass(frozen=True)
class DownloadedMedia:
    data: bytes
    content_type: str
    filename: str
    size: int


class MediaDownloadFailure(Exception):
    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason


async def download_incoming_media(
    media: WeComIncomingMedia,
    config: WeComAIBotConfig,
    fetcher: FetchMedia | None = None,
) -> DownloadedMedia:
    if fetcher:
        data, content_type = await fetcher(media)
    else:
        data, content_type = _fixture_media(media) or await _download_url(media.download_url, config)

    if len(data) > config.media_max_download_bytes:
        raise MediaDownloadFailure("size_limit", "Media exceeds configured size limit.")

    content_type = _normalize_content_type(content_type or media.content_type or _guess_content_type(media.filename))
    if not _is_allowed_media(media.msgtype, content_type, media.filename, config.media_allowed_types):
        raise MediaDownloadFailure("type_not_allowed", "Media type is not allowed.")

    if media.aeskey:
        data = decrypt_wecom_media(data, media.aeskey)

    if len(data) > config.media_max_download_bytes:
        raise MediaDownloadFailure("size_limit", "Decrypted media exceeds configured size limit.")

    return DownloadedMedia(
        data=data,
        content_type=content_type or "application/octet-stream",
        filename=_safe_filename(media.filename, media.msgtype, content_type),
        size=len(data),
    )


def decrypt_wecom_media(data: bytes, aeskey: str) -> bytes:
    key = _decode_aeskey(aeskey)
    if len(key) != 32:
        raise MediaDownloadFailure("decryption_failed", "Invalid media aeskey.")

    iv = key[:16]
    try:
        decryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
        padded = decryptor.update(data) + decryptor.finalize()
        unpadder = PKCS7(256).unpadder()
        return unpadder.update(padded) + unpadder.finalize()
    except Exception as exc:
        raise MediaDownloadFailure("decryption_failed", "Failed to decrypt media.") from exc


async def _download_url(url: str, config: WeComAIBotConfig) -> tuple[bytes, str]:
    if not url:
        raise MediaDownloadFailure("missing_url", "Media download URL is missing.")

    try:
        hostname, resolved_ip = assert_url_is_safe(url, allowed_schemes=frozenset({"https"}))
    except ValueError as exc:
        raise MediaDownloadFailure("unsafe_url", "Media download URL is not safe.") from exc

    timeout = max(config.media_download_timeout_seconds, 1)
    data = bytearray()
    try:
        with pin_dns_global(hostname, resolved_ip):
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
                async with client.stream("GET", url) as response:
                    if 300 <= response.status_code < 400:
                        raise MediaDownloadFailure("redirect", "Media download redirects are not allowed.")
                    if response.status_code >= 400:
                        raise MediaDownloadFailure("http_error", "Media download failed.")

                    content_length = response.headers.get("content-length")
                    if content_length and int(content_length) > config.media_max_download_bytes:
                        raise MediaDownloadFailure("size_limit", "Media exceeds configured size limit.")

                    async for chunk in response.aiter_bytes():
                        data.extend(chunk)
                        if len(data) > config.media_max_download_bytes:
                            raise MediaDownloadFailure("size_limit", "Media exceeds configured size limit.")

                    return bytes(data), response.headers.get("content-type") or ""
    except MediaDownloadFailure:
        raise
    except httpx.TimeoutException as exc:
        raise MediaDownloadFailure("timeout", "Media download timed out.") from exc
    except httpx.RequestError as exc:
        raise MediaDownloadFailure("network_error", "Media download failed.") from exc
    except ValueError as exc:
        raise MediaDownloadFailure("size_limit", "Invalid media content length.") from exc


def _fixture_media(media: WeComIncomingMedia) -> tuple[bytes, str] | None:
    body = media.raw.get("body") or {}
    media_body = body.get(media.msgtype) or body.get("media") or {}
    encoded = media_body.get("data_base64") or media_body.get("content_base64")
    if not encoded:
        return None
    try:
        return base64.b64decode(encoded), media.content_type or media_body.get("content_type") or ""
    except Exception as exc:
        raise MediaDownloadFailure("fixture_invalid", "Invalid media fixture payload.") from exc


def _decode_aeskey(aeskey: str) -> bytes:
    raw = (aeskey or "").strip()
    for candidate in (raw, raw + "=" * (-len(raw) % 4)):
        try:
            decoded = base64.urlsafe_b64decode(candidate)
            if len(decoded) == 32:
                return decoded
        except Exception:
            pass
    return raw.encode("utf-8")


def _is_allowed_media(msgtype: str, content_type: str, filename: str, allowed_types: tuple[str, ...]) -> bool:
    content_type = _normalize_content_type(content_type)
    if content_type in allowed_types:
        return True
    if msgtype == "image" and content_type.startswith("image/"):
        return any(value.startswith("image/") for value in allowed_types)
    guessed = _guess_content_type(filename)
    return bool(guessed and guessed in allowed_types)


def _normalize_content_type(content_type: str) -> str:
    return (content_type or "").split(";", 1)[0].strip().lower()


def _guess_content_type(filename: str) -> str:
    guessed, _ = mimetypes.guess_type(filename or "")
    return _normalize_content_type(guessed or "")


def _safe_filename(filename: str, msgtype: str, content_type: str) -> str:
    suffix = Path(filename or "").suffix
    if not suffix:
        suffix = mimetypes.guess_extension(content_type) or (".jpg" if msgtype == "image" else ".bin")
    stem = Path(filename or f"wecom-{msgtype}").stem or f"wecom-{msgtype}"
    safe_stem = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in stem)[:128]
    return f"{safe_stem}{suffix}"
