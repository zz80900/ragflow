import pytest
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7

from api.apps.services.wecom_aibot.config import WeComAIBotConfig
from api.apps.services.wecom_aibot.media_download import MediaDownloadFailure, decrypt_wecom_media, download_incoming_media
from api.apps.services.wecom_aibot.protocol import extract_media_message


def _message(**overrides):
    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-1"},
        "body": {
            "msgid": "msg-1",
            "aibotid": "bot-1",
            "from": {"userid": "user-1"},
            "msgtype": "image",
            "image": {
                "url": "https://example.com/a.png",
                "filename": "a.png",
                "content_type": "image/png",
            },
        },
    }
    payload["body"]["image"].update(overrides)
    return extract_media_message(payload)


@pytest.mark.asyncio
async def test_download_incoming_media_accepts_safe_fixture():
    async def fetcher(media):
        return b"image-bytes", "image/png"

    result = await download_incoming_media(_message(), WeComAIBotConfig(media_max_download_bytes=1024), fetcher=fetcher)

    assert result.data == b"image-bytes"
    assert result.content_type == "image/png"
    assert result.filename == "a.png"
    assert result.size == len(b"image-bytes")


@pytest.mark.asyncio
async def test_download_incoming_media_rejects_unsafe_url_before_fetch():
    message = _message(url="http://127.0.0.1/private.png")

    with pytest.raises(MediaDownloadFailure) as exc_info:
        await download_incoming_media(message, WeComAIBotConfig(media_max_download_bytes=1024))

    assert exc_info.value.reason == "unsafe_url"


@pytest.mark.asyncio
async def test_download_incoming_media_rejects_size_limit():
    async def fetcher(media):
        return b"x" * 4, "image/png"

    with pytest.raises(MediaDownloadFailure) as exc_info:
        await download_incoming_media(_message(), WeComAIBotConfig(media_max_download_bytes=3), fetcher=fetcher)

    assert exc_info.value.reason == "size_limit"


@pytest.mark.asyncio
async def test_download_incoming_media_rejects_type():
    async def fetcher(media):
        return b"html", "text/html"

    with pytest.raises(MediaDownloadFailure) as exc_info:
        await download_incoming_media(_message(filename="a.html", content_type="text/html"), WeComAIBotConfig(), fetcher=fetcher)

    assert exc_info.value.reason == "type_not_allowed"


@pytest.mark.asyncio
async def test_download_incoming_media_propagates_timeout_failure():
    async def fetcher(media):
        raise MediaDownloadFailure("timeout", "Media download timed out.")

    with pytest.raises(MediaDownloadFailure) as exc_info:
        await download_incoming_media(_message(), WeComAIBotConfig(), fetcher=fetcher)

    assert exc_info.value.reason == "timeout"


def test_decrypt_wecom_media_success():
    key = b"k" * 32
    plaintext = b"hello-wecom"
    padder = PKCS7(256).padder()
    padded = padder.update(plaintext) + padder.finalize()
    encryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    encrypted = encryptor.update(padded) + encryptor.finalize()

    assert decrypt_wecom_media(encrypted, key.decode("ascii")) == plaintext


def test_decrypt_wecom_media_failure():
    with pytest.raises(MediaDownloadFailure) as exc_info:
        decrypt_wecom_media(b"bad", "short")

    assert exc_info.value.reason == "decryption_failed"
