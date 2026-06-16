import time

import pytest

from api.apps.services.wecom_aibot.media_public import (
    MediaPublicFailure,
    StorageMediaReference,
    load_signed_public_media,
    publicize_media,
    sign_public_media_token,
    verify_public_media_token,
)


class FakeStorage:
    def __init__(self, presigned_url="", data=b"img"):
        self.presigned_url = presigned_url
        self.data = data

    def get_presigned_url(self, bucket, key, ttl_seconds, tenant_id=None):
        return self.presigned_url

    def obj_exist(self, bucket, key, tenant_id=None):
        return bool(self.data)

    def get(self, bucket, key, tenant_id=None):
        return self.data


class TimedeltaStorage(FakeStorage):
    def get_presigned_url(self, bucket, key, expires, tenant_id=None):
        expires.total_seconds()
        return "https://storage.example.com/timedelta.png"


def test_publicize_media_preserves_https_url():
    result = publicize_media(
        "https://example.com/a.png",
        storage=FakeStorage(),
        ttl_seconds=300,
        public_base_url="",
        token_secret="secret",
    )

    assert result.url == "https://example.com/a.png"
    assert result.mode == "https"


def test_publicize_media_uses_presigned_url():
    result = publicize_media(
        "kb1-image.png",
        storage=FakeStorage(presigned_url="https://storage.example.com/image.png"),
        ttl_seconds=300,
        public_base_url="https://ragflow.example.com",
        token_secret="secret",
        tenant_id="tenant-1",
    )

    assert result.url == "https://storage.example.com/image.png"
    assert result.mode == "presigned"
    assert result.reference.bucket == "kb1"
    assert result.reference.key == "image.png"


def test_publicize_media_retries_presigned_url_with_timedelta():
    result = publicize_media(
        "kb1-image.png",
        storage=TimedeltaStorage(),
        ttl_seconds=300,
        public_base_url="https://ragflow.example.com",
        token_secret="secret",
        tenant_id="tenant-1",
    )

    assert result.url == "https://storage.example.com/timedelta.png"


def test_publicize_media_generates_signed_route_url():
    result = publicize_media(
        "/api/v1/documents/images/kb1-image.png",
        storage=FakeStorage(),
        ttl_seconds=300,
        public_base_url="https://ragflow.example.com",
        token_secret="secret",
        tenant_id="tenant-1",
    )

    assert result.mode == "signed_route"
    assert result.url.startswith("https://ragflow.example.com/api/v1/agents/wecom/media/")


def test_signed_public_media_token_loads_existing_image():
    token = sign_public_media_token(
        StorageMediaReference(bucket="kb1", key="image.png", tenant_id="tenant-1", content_type="image/png"),
        ttl_seconds=300,
        secret="secret",
    )

    data, content_type = load_signed_public_media(FakeStorage(data=b"png"), token, secret="secret")

    assert data == b"png"
    assert content_type == "image/png"


def test_signed_public_media_rejects_expired_token():
    token = sign_public_media_token(StorageMediaReference(bucket="kb1", key="image.png"), ttl_seconds=1, secret="secret")

    with pytest.raises(MediaPublicFailure) as exc_info:
        verify_public_media_token(token, secret="secret", now=int(time.time()) + 2)

    assert exc_info.value.reason == "expired_token"


def test_signed_public_media_rejects_invalid_token():
    token = sign_public_media_token(StorageMediaReference(bucket="kb1", key="image.png"), ttl_seconds=300, secret="secret")

    with pytest.raises(MediaPublicFailure) as exc_info:
        verify_public_media_token(token, secret="other")

    assert exc_info.value.reason == "invalid_token"


def test_signed_public_media_rejects_non_image():
    token = sign_public_media_token(
        StorageMediaReference(bucket="kb1", key="file.pdf", content_type="application/pdf"),
        ttl_seconds=300,
        secret="secret",
    )

    with pytest.raises(MediaPublicFailure) as exc_info:
        verify_public_media_token(token, secret="secret")

    assert exc_info.value.reason == "type_not_allowed"
