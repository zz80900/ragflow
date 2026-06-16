import time

import pytest

from api.apps.services.wecom_aibot.media_upload import MediaUploadFailure, TemporaryMediaCache, WeComTemporaryMediaUploader


@pytest.mark.asyncio
async def test_temporary_media_upload_success_sends_init_chunk_finish():
    sent = []
    uploader = WeComTemporaryMediaUploader(TemporaryMediaCache(ttl_seconds=300))

    async def send(frame):
        sent.append(frame)

    async def wait(req_id):
        frame = sent[-1]
        if frame["cmd"] == "aibot_upload_media_init":
            return {"headers": {"req_id": req_id}, "body": {"upload_id": "upload-1"}, "errcode": 0}
        if frame["cmd"] == "aibot_upload_media_finish":
            return {"headers": {"req_id": req_id}, "body": {"type": "image", "media_id": "media-1", "created_at": int(time.time())}, "errcode": 0}
        return {"headers": {"req_id": req_id}, "errcode": 0}

    result = await uploader.upload(
        bot_id="bot-1",
        media_type="image",
        filename="a.png",
        data=b"image",
        send_frame=send,
        wait_response=wait,
    )

    assert result.media_id == "media-1"
    assert [frame["cmd"] for frame in sent] == [
        "aibot_upload_media_init",
        "aibot_upload_media_chunk",
        "aibot_upload_media_finish",
    ]
    assert sent[0]["body"]["type"] == "image"
    assert sent[1]["body"]["chunk_index"] == 0


@pytest.mark.asyncio
async def test_temporary_media_upload_requires_response_waiter():
    uploader = WeComTemporaryMediaUploader(TemporaryMediaCache(ttl_seconds=300))

    async def send(frame):
        return None

    with pytest.raises(MediaUploadFailure) as exc_info:
        await uploader.upload(
            bot_id="bot-1",
            media_type="image",
            filename="a.png",
            data=b"image",
            send_frame=send,
        )

    assert exc_info.value.reason == "response_waiter_unavailable"


@pytest.mark.asyncio
async def test_temporary_media_cache_is_scoped_by_bot():
    sent = []
    uploader = WeComTemporaryMediaUploader(TemporaryMediaCache(ttl_seconds=300))

    async def send(frame):
        sent.append(frame)

    async def wait(req_id):
        frame = sent[-1]
        if frame["cmd"] == "aibot_upload_media_init":
            return {"headers": {"req_id": req_id}, "body": {"upload_id": f"upload-{len(sent)}"}, "errcode": 0}
        if frame["cmd"] == "aibot_upload_media_finish":
            return {"headers": {"req_id": req_id}, "body": {"type": "image", "media_id": f"media-{len(sent)}", "created_at": int(time.time())}, "errcode": 0}
        return {"headers": {"req_id": req_id}, "errcode": 0}

    first = await uploader.upload(bot_id="bot-1", media_type="image", filename="a.png", data=b"same", send_frame=send, wait_response=wait)
    sent_after_first = len(sent)
    second = await uploader.upload(bot_id="bot-1", media_type="image", filename="a.png", data=b"same", send_frame=send, wait_response=wait)
    third = await uploader.upload(bot_id="bot-2", media_type="image", filename="a.png", data=b"same", send_frame=send, wait_response=wait)

    assert first.media_id == second.media_id
    assert len(sent) == sent_after_first + 3
    assert third.media_id != first.media_id
