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

from dataclasses import dataclass, field
import re

from api.apps.services.wecom_aibot.media_download import DownloadedMedia
from api.apps.services.wecom_aibot.media_public import MediaPublicFailure, PublicMedia, parse_storage_reference, publicize_media
from api.apps.services.wecom_aibot.media_upload import MediaUploadFailure
from api.apps.services.wecom_aibot.protocol import WeComIncomingMedia, build_file_frame, build_image_frame, build_markdown_frame


def build_image_markdown(image_urls: list[str]) -> str:
    return "\n".join(f"![图片]({url})" for url in image_urls if url.startswith("https://"))


@dataclass(frozen=True)
class StoredInboundMedia:
    bucket: str
    key: str
    filename: str
    content_type: str
    size: int

    @property
    def reference(self) -> str:
        return f"{self.bucket}-{self.key}"


@dataclass(frozen=True)
class OutgoingMedia:
    media_type: str
    source: str
    filename: str = ""
    content_type: str = "image/jpeg"


@dataclass
class MediaReplyDebug:
    public_urls: list[str] = field(default_factory=list)
    uploaded_media_ids: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)
    frames: list[dict] = field(default_factory=list)


def store_inbound_media(storage, tenant_id: str, agent_id: str, bot_id: str, message: WeComIncomingMedia, media: DownloadedMedia) -> StoredInboundMedia:
    bucket = "wecom-aibot-media"
    key = "/".join(
        [
            _safe_path(tenant_id),
            _safe_path(agent_id),
            _safe_path(bot_id),
            _safe_path(message.msgid or "no-msgid"),
            _safe_path(media.filename),
        ]
    )
    try:
        storage.put(bucket, key, media.data, tenant_id=tenant_id)
    except TypeError:
        storage.put(bucket, key, media.data)
    return StoredInboundMedia(
        bucket=bucket,
        key=key,
        filename=media.filename,
        content_type=media.content_type,
        size=media.size,
    )


def build_media_query(message: WeComIncomingMedia, stored: StoredInboundMedia) -> str:
    title = "企业微信图片" if message.msgtype == "image" else "企业微信文件"
    lines = [
        f"[{title}]",
        f"文件名: {stored.filename}",
        f"类型: {stored.content_type}",
        f"大小: {stored.size}",
        f"引用: {stored.reference}",
    ]
    if message.content:
        lines.extend(["", f"用户文本: {message.content}"])
    return "\n".join(lines)


def extract_outgoing_media(content: str, image_urls: list[str] | None = None) -> list[OutgoingMedia]:
    media: list[OutgoingMedia] = []
    seen: set[str] = set()
    for url in image_urls or []:
        _append_media(media, seen, OutgoingMedia(media_type="image", source=url, filename=_filename_from_source(url)))

    for match in re.finditer(r"!\[[^\]]*\]\(([^)\s]+)\)", content or ""):
        source = match.group(1)
        _append_media(media, seen, OutgoingMedia(media_type="image", source=source, filename=_filename_from_source(source)))

    for match in re.finditer(r"/api/v1/documents/images/([A-Za-z0-9_.\-/%]+)", content or ""):
        source = f"/api/v1/documents/images/{match.group(1).rstrip('.,)')}"
        _append_media(media, seen, OutgoingMedia(media_type="image", source=source, filename=_filename_from_source(source)))

    for match in re.finditer(r"\b([A-Za-z0-9][A-Za-z0-9_.]*-[A-Za-z0-9][A-Za-z0-9_./\-]{8,})\b", content or ""):
        source = match.group(1)
        _append_media(media, seen, OutgoingMedia(media_type="image", source=source, filename=_filename_from_source(source)))

    return media


async def build_reply_media_frames(
    *,
    req_id: str,
    outgoing: list[OutgoingMedia],
    binding: dict,
    config,
    storage,
    send_frame,
    uploader=None,
    wait_response=None,
) -> MediaReplyDebug:
    debug = MediaReplyDebug()
    reply_mode = (config.media_reply_mode or "auto").lower()
    if reply_mode not in {"auto", "public_url", "upload"}:
        reply_mode = "auto"

    for item in outgoing:
        public_media = _try_publicize(item, binding, config, storage, debug)
        frame = None
        if public_media and reply_mode in {"auto", "public_url"}:
            frame = build_markdown_frame(req_id, build_image_markdown([public_media.url]))
            debug.public_urls.append(public_media.url)
        elif reply_mode in {"auto", "upload"} and uploader:
            frame = await _try_upload(req_id, item, public_media, binding, storage, send_frame, uploader, wait_response, debug)
        elif public_media:
            frame = build_markdown_frame(req_id, build_image_markdown([public_media.url]))
            debug.public_urls.append(public_media.url)

        if frame:
            debug.frames.append(frame)

    return debug


def _try_publicize(item: OutgoingMedia, binding: dict, config, storage, debug: MediaReplyDebug) -> PublicMedia | None:
    try:
        return publicize_media(
            item.source,
            storage=storage,
            ttl_seconds=config.media_public_url_ttl_seconds,
            public_base_url=config.media_public_base_url,
            token_secret=config.media_public_token_secret,
            tenant_id=binding.get("tenant_id"),
        )
    except MediaPublicFailure as exc:
        debug.failures.append(exc.reason)
        return None


async def _try_upload(req_id: str, item: OutgoingMedia, public_media: PublicMedia | None, binding: dict, storage, send_frame, uploader, wait_response, debug: MediaReplyDebug) -> dict | None:
    data = _load_media_bytes(item, public_media, storage, tenant_id=binding.get("tenant_id"))
    if not data:
        if public_media:
            debug.public_urls.append(public_media.url)
            return build_markdown_frame(req_id, build_image_markdown([public_media.url]))
        debug.failures.append("upload_source_unavailable")
        return None
    try:
        uploaded = await uploader.upload(
            bot_id=binding["bot_id"],
            media_type=item.media_type,
            filename=item.filename or f"wecom-{item.media_type}",
            data=data,
            send_frame=send_frame,
            wait_response=wait_response,
        )
        debug.uploaded_media_ids.append(uploaded.media_id)
        if item.media_type == "file":
            return build_file_frame(req_id, uploaded.media_id)
        return build_image_frame(req_id, uploaded.media_id)
    except MediaUploadFailure as exc:
        debug.failures.append(exc.reason)
        if public_media:
            debug.public_urls.append(public_media.url)
            return build_markdown_frame(req_id, build_image_markdown([public_media.url]))
        return None


def _load_media_bytes(item: OutgoingMedia, public_media: PublicMedia | None, storage, tenant_id: str | None = None) -> bytes:
    reference = public_media.reference if public_media else parse_storage_reference(item.source, tenant_id=tenant_id)
    if not reference:
        return b""
    try:
        return storage.get(reference.bucket, reference.key, tenant_id=reference.tenant_id)
    except TypeError:
        return storage.get(reference.bucket, reference.key)
    except Exception:
        return b""


def _append_media(media: list[OutgoingMedia], seen: set[str], item: OutgoingMedia) -> None:
    if item.source in seen:
        return
    seen.add(item.source)
    media.append(item)


def _filename_from_source(source: str) -> str:
    return (source or "").split("?", 1)[0].rstrip("/").split("/")[-1] or "wecom-image"


def _safe_path(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value or ""))
    return cleaned[:160] or "unknown"
