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
from datetime import timedelta
import hashlib
import hmac
import json
import time
from urllib.parse import urlparse


@dataclass(frozen=True)
class StorageMediaReference:
    bucket: str
    key: str
    content_type: str = "image/jpeg"
    tenant_id: str | None = None


@dataclass(frozen=True)
class PublicMedia:
    url: str
    mode: str
    reference: StorageMediaReference | None = None


class MediaPublicFailure(Exception):
    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason


def publicize_media(
    reference: str | StorageMediaReference,
    *,
    storage,
    ttl_seconds: int,
    public_base_url: str,
    token_secret: str,
    tenant_id: str | None = None,
) -> PublicMedia:
    if isinstance(reference, str) and reference.startswith("https://") and "/api/v1/documents/images/" not in reference:
        return PublicMedia(url=reference, mode="https")

    storage_ref = reference if isinstance(reference, StorageMediaReference) else parse_storage_reference(reference, tenant_id=tenant_id)
    if not storage_ref:
        raise MediaPublicFailure("unsupported_reference", "Unsupported media reference.")

    presigned = _presigned_url(storage, storage_ref, ttl_seconds)
    if presigned and presigned.startswith("https://"):
        return PublicMedia(url=presigned, mode="presigned", reference=storage_ref)

    if not public_base_url or not public_base_url.startswith("https://") or not token_secret:
        raise MediaPublicFailure("public_url_unavailable", "Public media URL is not configured.")

    token = sign_public_media_token(storage_ref, ttl_seconds=ttl_seconds, secret=token_secret)
    return PublicMedia(
        url=f"{public_base_url.rstrip('/')}/api/v1/agents/wecom/media/{token}",
        mode="signed_route",
        reference=storage_ref,
    )


def sign_public_media_token(reference: StorageMediaReference, *, ttl_seconds: int, secret: str) -> str:
    expires_at = int(time.time()) + max(ttl_seconds, 1)
    payload = {
        "bucket": reference.bucket,
        "key": reference.key,
        "tenant_id": reference.tenant_id,
        "content_type": reference.content_type,
        "exp": expires_at,
    }
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), payload_bytes, hashlib.sha256).digest()
    return f"{_b64(payload_bytes)}.{_b64(signature)}"


def verify_public_media_token(token: str, *, secret: str, now: int | None = None) -> StorageMediaReference:
    if not secret:
        raise MediaPublicFailure("invalid_token", "Media token signing is not configured.")
    try:
        payload_part, signature_part = token.split(".", 1)
        payload_bytes = _unb64(payload_part)
        signature = _unb64(signature_part)
        expected = hmac.new(secret.encode("utf-8"), payload_bytes, hashlib.sha256).digest()
        if not hmac.compare_digest(signature, expected):
            raise MediaPublicFailure("invalid_token", "Invalid media token.")
        payload = json.loads(payload_bytes.decode("utf-8"))
    except MediaPublicFailure:
        raise
    except Exception as exc:
        raise MediaPublicFailure("invalid_token", "Invalid media token.") from exc

    if int(payload.get("exp") or 0) < int(now or time.time()):
        raise MediaPublicFailure("expired_token", "Media token has expired.")

    content_type = str(payload.get("content_type") or "")
    if not content_type.startswith("image/"):
        raise MediaPublicFailure("type_not_allowed", "Only image media can be publicized.")

    bucket = str(payload.get("bucket") or "")
    key = str(payload.get("key") or "")
    if not bucket or not key:
        raise MediaPublicFailure("invalid_token", "Invalid media token.")

    return StorageMediaReference(
        bucket=bucket,
        key=key,
        tenant_id=payload.get("tenant_id"),
        content_type=content_type,
    )


def parse_storage_reference(value: str, *, tenant_id: str | None = None) -> StorageMediaReference | None:
    raw = (value or "").strip()
    if not raw:
        return None

    parsed = urlparse(raw)
    path = parsed.path if parsed.scheme else raw
    marker = "/api/v1/documents/images/"
    if marker in path:
        image_id = path.split(marker, 1)[1].split("?", 1)[0].split("#", 1)[0]
        return _parse_composite_id(image_id, tenant_id=tenant_id)

    if parsed.scheme:
        return None

    return _parse_composite_id(path, tenant_id=tenant_id)


def load_signed_public_media(storage, token: str, *, secret: str) -> tuple[bytes, str]:
    reference = verify_public_media_token(token, secret=secret)
    if hasattr(storage, "obj_exist"):
        try:
            exists = storage.obj_exist(reference.bucket, reference.key, tenant_id=reference.tenant_id)
        except TypeError:
            exists = storage.obj_exist(reference.bucket, reference.key)
        if not exists:
            raise MediaPublicFailure("not_found", "Media object not found.")
    try:
        data = storage.get(reference.bucket, reference.key, tenant_id=reference.tenant_id)
    except TypeError:
        data = storage.get(reference.bucket, reference.key)
    if not data:
        raise MediaPublicFailure("not_found", "Media object not found.")
    return data, reference.content_type


def _parse_composite_id(value: str, *, tenant_id: str | None = None) -> StorageMediaReference | None:
    parts = value.split("-", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return None
    return StorageMediaReference(bucket=parts[0], key=parts[1], tenant_id=tenant_id)


def _presigned_url(storage, reference: StorageMediaReference, ttl_seconds: int) -> str:
    if not storage or not hasattr(storage, "get_presigned_url"):
        return ""
    expires = timedelta(seconds=max(int(ttl_seconds or 0), 1))
    for candidate in (expires, ttl_seconds):
        try:
            url = storage.get_presigned_url(reference.bucket, reference.key, candidate, tenant_id=reference.tenant_id) or ""
        except TypeError:
            try:
                url = storage.get_presigned_url(reference.bucket, reference.key, candidate) or ""
            except Exception:
                url = ""
        except Exception:
            url = ""
        if url:
            return url
    return ""


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _unb64(data: str) -> bytes:
    return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))
