from dataclasses import dataclass
import asyncio
import json

import pytest

from api.apps.services.wecom_aibot.agent_bridge import AgentBridgeResult
from api.apps.services.wecom_aibot.config import WeComAIBotConfig
from api.apps.services.wecom_aibot.dedup_store import WeComAIBotDedupStore
from api.apps.services.wecom_aibot.media_download import DownloadedMedia
from api.apps.services.wecom_aibot.media_upload import MediaUploadFailure, TemporaryMedia
from api.apps.services.wecom_aibot.service import WeComAIBotService
import api.apps.services.wecom_aibot.dedup_store as dedup_module
import api.apps.services.wecom_aibot.service as service_module


@dataclass
class FakeBridge:
    def __init__(self):
        self.calls = []

    async def run(self, **kwargs):
        self.calls.append(kwargs)
        yield AgentBridgeResult(session_id="session-1", content="hel")
        yield AgentBridgeResult(session_id="session-1", content="hello")


class SlowBridge:
    async def run(self, **kwargs):
        await service_module.asyncio.Future()
        yield AgentBridgeResult(session_id="session-1", content="late")


class FakeDedup:
    def __init__(self, allowed=True):
        self.allowed = allowed

    def acquire(self, msgid):
        return self.allowed


class FakeSessionStore:
    def __init__(self):
        self.saved = []
        self.resolved = []

    def resolve(self, **kwargs):
        from api.apps.services.wecom_aibot.session_store import WeComConversation

        self.resolved.append(kwargs)
        return WeComConversation(key="conv-key", ragflow_session_id=None, ragflow_user_id=kwargs["userid"], query_prefix="")

    def save(self, conversation_key, ragflow_session_id):
        self.saved.append((conversation_key, ragflow_session_id))


class FakeStorage:
    def __init__(self, presigned_url=""):
        self.objects = {}
        self.presigned_url = presigned_url

    def put(self, bucket, key, data, tenant_id=None):
        self.objects[(bucket, key, tenant_id)] = data

    def get(self, bucket, key, tenant_id=None):
        return self.objects.get((bucket, key, tenant_id), b"stored-image")

    def get_presigned_url(self, bucket, key, ttl_seconds, tenant_id=None):
        return self.presigned_url


class FakeMediaDownloader:
    def __init__(self):
        self.calls = []

    async def __call__(self, message, config):
        self.calls.append(message)
        return DownloadedMedia(data=b"image", content_type="image/png", filename="a.png", size=5)


class ImageBridge:
    def __init__(self, content, image_urls=None):
        self.content = content
        self.image_urls = image_urls or []
        self.calls = []

    async def run(self, **kwargs):
        self.calls.append(kwargs)
        yield AgentBridgeResult(session_id="session-1", content=self.content, image_urls=self.image_urls)


class FakeUploader:
    def __init__(self, fail=False):
        self.fail = fail
        self.calls = []

    async def upload(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail:
            raise MediaUploadFailure("upload_failed", "upload failed")
        return TemporaryMedia(media_id="media-1", media_type=kwargs["media_type"], expires_at=9999999999)


@pytest.mark.asyncio
async def test_handle_payload_sends_final_stream_and_saves_session():
    service = WeComAIBotService(
        config=WeComAIBotConfig(enabled=True, stream_interval_ms=0),
        agent_bridge=FakeBridge(),
    )
    service.dedup_store = FakeDedup()
    session_store = FakeSessionStore()
    service.session_store = session_store
    sent = []

    async def send(frame):
        sent.append(frame)

    await service.handle_payload(
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-1",
                "aibotid": "bot-1",
                "chatid": "user-1",
                "chattype": "single",
                "from": {"userid": "user-1"},
                "msgtype": "text",
                "text": {"content": "hello?"},
            },
        },
        send,
        binding_override={
            "tenant_id": "tenant-1",
            "agent_id": "agent-1",
            "bot_id": "bot-1",
            "enabled": True,
        },
    )

    final_stream = next(frame for frame in sent if (frame.get("body") or {}).get("stream", {}).get("finish") is True)
    assert final_stream["body"]["stream"]["content"] == "hello"
    assert "msgid" not in final_stream["body"]
    assert session_store.saved == [("conv-key", "session-1")]


@pytest.mark.asyncio
async def test_text_only_callback_keeps_stream_frame_sequence():
    service = WeComAIBotService(
        config=WeComAIBotConfig(enabled=True, stream_interval_ms=0, conversation_interval_ms=0),
        agent_bridge=FakeBridge(),
    )
    service.dedup_store = FakeDedup()
    service.session_store = FakeSessionStore()
    sent = []

    async def send(frame):
        sent.append(frame)

    await service.handle_payload(
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-1",
                "aibotid": "bot-1",
                "from": {"userid": "user-1"},
                "msgtype": "text",
                "text": {"content": "hello?"},
            },
        },
        send,
        binding_override={
            "tenant_id": "tenant-1",
            "agent_id": "agent-1",
            "bot_id": "bot-1",
            "enabled": True,
        },
    )

    bodies = [frame["body"] for frame in sent]
    assert [body["msgtype"] for body in bodies] == ["stream", "stream", "stream"]
    assert bodies[-1]["stream"]["finish"] is True
    assert bodies[-1]["stream"]["content"] == "hello"


@pytest.mark.asyncio
async def test_handle_payload_skips_duplicate_message():
    bridge = FakeBridge()
    service = WeComAIBotService(config=WeComAIBotConfig(enabled=True), agent_bridge=bridge)
    service.dedup_store = FakeDedup(allowed=False)
    session_store = FakeSessionStore()
    service.session_store = session_store
    sent = []

    async def send(frame):
        sent.append(frame)

    await service.handle_payload(
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-1",
                "aibotid": "bot-1",
                "from": {"userid": "user-1"},
                "msgtype": "text",
                "text": {"content": "hello?"},
            },
        },
        send,
        binding_override={
            "tenant_id": "tenant-1",
            "agent_id": "agent-1",
            "bot_id": "bot-1",
            "enabled": True,
        },
    )

    assert sent == []
    assert bridge.calls == []
    assert session_store.resolved == []


@pytest.mark.asyncio
async def test_handle_payload_skips_duplicate_media_before_download():
    downloader = FakeMediaDownloader()
    bridge = FakeBridge()
    service = WeComAIBotService(config=WeComAIBotConfig(enabled=True), agent_bridge=bridge, media_downloader=downloader, storage=FakeStorage())
    service.dedup_store = FakeDedup(allowed=False)
    service.session_store = FakeSessionStore()
    sent = []

    async def send(frame):
        sent.append(frame)

    await service.handle_payload(
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-1",
                "aibotid": "bot-1",
                "from": {"userid": "user-1"},
                "msgtype": "image",
                "image": {"url": "https://example.com/a.png", "filename": "a.png", "content_type": "image/png"},
            },
        },
        send,
        binding_override={
            "tenant_id": "tenant-1",
            "agent_id": "agent-1",
            "bot_id": "bot-1",
            "enabled": True,
        },
    )

    assert sent == []
    assert downloader.calls == []
    assert bridge.calls == []


@pytest.mark.asyncio
async def test_handle_payload_builds_media_query_and_stores_media():
    downloader = FakeMediaDownloader()
    bridge = FakeBridge()
    storage = FakeStorage()
    service = WeComAIBotService(
        config=WeComAIBotConfig(enabled=True, stream_interval_ms=0, conversation_interval_ms=0),
        agent_bridge=bridge,
        media_downloader=downloader,
        storage=storage,
    )
    service.dedup_store = FakeDedup()
    service.session_store = FakeSessionStore()
    sent = []

    async def send(frame):
        sent.append(frame)

    latest = await service.handle_payload(
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-1",
                "aibotid": "bot-1",
                "from": {"userid": "user-1"},
                "msgtype": "image",
                "image": {"url": "https://example.com/a.png", "filename": "a.png", "content_type": "image/png"},
            },
        },
        send,
        binding_override={
            "tenant_id": "tenant-1",
            "agent_id": "agent-1",
            "bot_id": "bot-1",
            "enabled": True,
        },
    )

    query = bridge.calls[0]["query"]
    assert "[企业微信图片]" in query
    assert "文件名: a.png" in query
    assert "引用: wecom-aibot-media-tenant-1/agent-1/bot-1/msg-1/a.png" in query
    assert latest.stored_media == ["wecom-aibot-media-tenant-1/agent-1/bot-1/msg-1/a.png"]
    assert ("wecom-aibot-media", "tenant-1/agent-1/bot-1/msg-1/a.png", "tenant-1") in storage.objects


@pytest.mark.asyncio
async def test_media_reply_follows_final_stream():
    bridge = ImageBridge("hello ![x](https://example.com/a.png)", ["https://example.com/a.png"])
    service = WeComAIBotService(
        config=WeComAIBotConfig(enabled=True, stream_interval_ms=0, conversation_interval_ms=0),
        agent_bridge=bridge,
    )
    service.dedup_store = FakeDedup()
    service.session_store = FakeSessionStore()
    sent = []

    async def send(frame):
        sent.append(frame)

    await service.handle_payload(
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-1",
                "aibotid": "bot-1",
                "from": {"userid": "user-1"},
                "msgtype": "text",
                "text": {"content": "hello?"},
            },
        },
        send,
        binding_override={
            "tenant_id": "tenant-1",
            "agent_id": "agent-1",
            "bot_id": "bot-1",
            "enabled": True,
        },
    )

    final_index = next(i for i, frame in enumerate(sent) if (frame.get("body") or {}).get("stream", {}).get("finish") is True)
    markdown_index = next(i for i, frame in enumerate(sent) if (frame.get("body") or {}).get("msgtype") == "markdown")
    assert final_index < markdown_index
    assert sent[markdown_index]["body"]["markdown"]["content"] == "![图片](https://example.com/a.png)"


@pytest.mark.asyncio
async def test_upload_reply_uses_media_id_after_final_stream():
    bridge = ImageBridge("hello ![x](kb1-image.png)")
    uploader = FakeUploader()
    storage = FakeStorage()
    service = WeComAIBotService(
        config=WeComAIBotConfig(enabled=True, stream_interval_ms=0, conversation_interval_ms=0, media_reply_mode="upload"),
        agent_bridge=bridge,
        media_uploader=uploader,
        storage=storage,
    )
    service.dedup_store = FakeDedup()
    service.session_store = FakeSessionStore()
    sent = []

    async def send(frame):
        sent.append(frame)

    await service.handle_payload(
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-1",
                "aibotid": "bot-1",
                "from": {"userid": "user-1"},
                "msgtype": "text",
                "text": {"content": "hello?"},
            },
        },
        send,
        binding_override={
            "tenant_id": "tenant-1",
            "agent_id": "agent-1",
            "bot_id": "bot-1",
            "enabled": True,
        },
    )

    final_index = next(i for i, frame in enumerate(sent) if (frame.get("body") or {}).get("stream", {}).get("finish") is True)
    image_index = next(i for i, frame in enumerate(sent) if (frame.get("body") or {}).get("msgtype") == "image")
    assert final_index < image_index
    assert sent[image_index]["body"]["image"]["media_id"] == "media-1"
    assert uploader.calls[0]["bot_id"] == "bot-1"


@pytest.mark.asyncio
async def test_upload_failure_falls_back_to_public_url():
    bridge = ImageBridge("hello ![x](kb1-image.png)")
    uploader = FakeUploader(fail=True)
    storage = FakeStorage(presigned_url="https://storage.example.com/image.png")
    service = WeComAIBotService(
        config=WeComAIBotConfig(enabled=True, stream_interval_ms=0, conversation_interval_ms=0, media_reply_mode="upload"),
        agent_bridge=bridge,
        media_uploader=uploader,
        storage=storage,
    )
    service.dedup_store = FakeDedup()
    service.session_store = FakeSessionStore()
    sent = []

    async def send(frame):
        sent.append(frame)

    await service.handle_payload(
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-1",
                "aibotid": "bot-1",
                "from": {"userid": "user-1"},
                "msgtype": "text",
                "text": {"content": "hello?"},
            },
        },
        send,
        binding_override={
            "tenant_id": "tenant-1",
            "agent_id": "agent-1",
            "bot_id": "bot-1",
            "enabled": True,
        },
    )

    markdown = sent[-1]["body"]["markdown"]["content"]
    assert markdown == "![图片](https://storage.example.com/image.png)"


@pytest.mark.asyncio
async def test_upload_failure_without_fallback_sends_no_media_frame():
    bridge = ImageBridge("hello ![x](kb1-image.png)")
    uploader = FakeUploader(fail=True)
    storage = FakeStorage()
    service = WeComAIBotService(
        config=WeComAIBotConfig(enabled=True, stream_interval_ms=0, conversation_interval_ms=0, media_reply_mode="upload"),
        agent_bridge=bridge,
        media_uploader=uploader,
        storage=storage,
    )
    service.dedup_store = FakeDedup()
    service.session_store = FakeSessionStore()
    sent = []

    async def send(frame):
        sent.append(frame)

    latest = await service.handle_payload(
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-1",
                "aibotid": "bot-1",
                "from": {"userid": "user-1"},
                "msgtype": "text",
                "text": {"content": "hello?"},
            },
        },
        send,
        binding_override={
            "tenant_id": "tenant-1",
            "agent_id": "agent-1",
            "bot_id": "bot-1",
            "enabled": True,
        },
    )

    assert all((frame.get("body") or {}).get("msgtype") == "stream" for frame in sent)
    assert latest.media_reply_debug.failures[-1] == "upload_failed"


@pytest.mark.asyncio
async def test_handle_payload_replies_welcome_for_enter_event():
    service = WeComAIBotService(
        config=WeComAIBotConfig(enabled=True, welcome_message="welcome"),
        agent_bridge=FakeBridge(),
    )
    sent = []

    async def send(frame):
        sent.append(frame)

    await service.handle_payload(
        {
            "cmd": "aibot_event_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "event_type": "enter_conversation",
                "aibotid": "bot-1",
                "from": {"userid": "user-1"},
            },
        },
        send,
        binding_override={
            "tenant_id": "tenant-1",
            "agent_id": "agent-1",
            "bot_id": "bot-1",
            "enabled": True,
        },
    )

    assert sent == [
        {
            "cmd": "aibot_respond_welcome_msg",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgtype": "markdown",
                "markdown": {"content": "welcome"},
            },
        }
    ]


def test_dedup_store_fail_closed_on_redis_error(monkeypatch):
    class BrokenRedis:
        def set(self, *args, **kwargs):
            raise RuntimeError("redis unavailable")

    class FakeRedisConn:
        REDIS = BrokenRedis()

        @staticmethod
        def is_alive():
            return True

    monkeypatch.setattr(dedup_module, "REDIS_CONN", FakeRedisConn())

    assert WeComAIBotDedupStore(ttl_seconds=60).acquire("msg-1") is False


def test_renew_lock_uses_atomic_lua_compare_and_expire(monkeypatch):
    calls = []

    class FakeRedis:
        def eval(self, script, key_count, key, value, ttl):
            calls.append((script, key_count, key, value, ttl))
            return 1

    class FakeRedisConn:
        REDIS = FakeRedis()

        @staticmethod
        def is_alive():
            return True

    monkeypatch.setattr(service_module, "REDIS_CONN", FakeRedisConn())

    service = WeComAIBotService(config=WeComAIBotConfig(enabled=True, lock_ttl_seconds=60), agent_bridge=FakeBridge())

    assert service._renew_lock("lock-key", "lock-value") is True
    assert "expire" in calls[0][0]
    assert calls[0][1:] == (1, "lock-key", "lock-value", 60)


def test_renew_lock_fails_when_lua_compare_fails(monkeypatch):
    class FakeRedis:
        def eval(self, *args):
            return 0

    class FakeRedisConn:
        REDIS = FakeRedis()

        @staticmethod
        def is_alive():
            return True

    monkeypatch.setattr(service_module, "REDIS_CONN", FakeRedisConn())

    service = WeComAIBotService(config=WeComAIBotConfig(enabled=True), agent_bridge=FakeBridge())

    assert service._renew_lock("lock-key", "other-value") is False


@pytest.mark.asyncio
async def test_heartbeat_raises_when_lock_cannot_be_renewed():
    async def send(frame):
        return None

    service = WeComAIBotService(
        config=WeComAIBotConfig(enabled=True, heartbeat_seconds=0),
        agent_bridge=FakeBridge(),
    )
    service._renew_lock = lambda *args: False

    with pytest.raises(RuntimeError, match="lock renew failed"):
        await service._heartbeat(send, "lock", "value")


@pytest.mark.asyncio
async def test_conversation_rate_limit_falls_back_to_local_limiter(monkeypatch):
    sleeps = []
    now = [10.0]

    class FakeRedisConn:
        @staticmethod
        def is_alive():
            return False

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(service_module, "REDIS_CONN", FakeRedisConn())
    monkeypatch.setattr(service_module.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(service_module.asyncio, "sleep", fake_sleep)

    service = WeComAIBotService(
        config=WeComAIBotConfig(enabled=True, conversation_interval_ms=1000),
        agent_bridge=FakeBridge(),
    )

    await service._wait_for_conversation_interval("conv-1")
    await service._wait_for_conversation_interval("conv-1")

    assert sleeps == [pytest.approx(1.0)]


@pytest.mark.asyncio
async def test_conversation_rate_limit_uses_redis_reservation(monkeypatch):
    sleeps = []
    calls = []

    class FakeRedis:
        def eval(self, script, key_count, key, now_ms, interval_ms, ttl_ms):
            calls.append((script, key_count, key, now_ms, interval_ms, ttl_ms))
            return 750

    class FakeRedisConn:
        REDIS = FakeRedis()

        @staticmethod
        def is_alive():
            return True

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(service_module, "REDIS_CONN", FakeRedisConn())
    monkeypatch.setattr(service_module.time, "time", lambda: 10.0)
    monkeypatch.setattr(service_module.asyncio, "sleep", fake_sleep)

    service = WeComAIBotService(
        config=WeComAIBotConfig(enabled=True, conversation_interval_ms=1000),
        agent_bridge=FakeBridge(),
    )

    await service._wait_for_conversation_interval("conv-1")

    assert calls[0][1:] == (1, "wecom:aibot:rate:conv-1", 10000, 1000, 4000)
    assert sleeps == [pytest.approx(0.75)]


@pytest.mark.asyncio
async def test_agent_stream_timeout_sends_final_error(monkeypatch):
    service = WeComAIBotService(
        config=WeComAIBotConfig(enabled=True, stream_interval_ms=0, conversation_interval_ms=0, max_stream_seconds=0.01),
        agent_bridge=SlowBridge(),
    )
    service.dedup_store = FakeDedup()
    service.session_store = FakeSessionStore()
    sent = []

    async def send(frame):
        sent.append(frame)

    await service.handle_payload(
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-1",
                "aibotid": "bot-1",
                "chatid": "user-1",
                "chattype": "single",
                "from": {"userid": "user-1"},
                "msgtype": "text",
                "text": {"content": "hello?"},
            },
        },
        send,
        binding_override={
            "tenant_id": "tenant-1",
            "agent_id": "agent-1",
            "bot_id": "bot-1",
            "enabled": True,
        },
    )

    final_stream = sent[-1]["body"]["stream"]
    assert final_stream["finish"] is True
    assert "exceeded 0.01 seconds" in final_stream["content"]


@pytest.mark.asyncio
async def test_test_connection_returns_sanitized_success(monkeypatch):
    class FakeClient:
        sent_frames = []

        def __init__(self, url):
            self.url = url

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def send(self, frame):
            self.sent_frames.append(frame)

        async def receive(self):
            yield json.dumps({"headers": {"req_id": self.sent_frames[-1]["headers"]["req_id"]}, "errcode": 0, "errmsg": "ok"})

    statuses = []
    monkeypatch.setattr(service_module, "WeComAIBotWebSocketClient", FakeClient)
    monkeypatch.setattr(
        service_module.WeComAIBotBindingService,
        "update_status",
        lambda *args, **kwargs: statuses.append((args, kwargs)),
    )

    service = WeComAIBotService(config=WeComAIBotConfig(enabled=True), agent_bridge=FakeBridge())
    result = await service.test_connection({"bot_id": "bot-1", "secret": "sensitive"})

    assert result["ok"] is True
    assert result["response_code"] == 0
    assert "sensitive" not in json.dumps(result)
    assert FakeClient.sent_frames[0]["body"]["secret"] == "sensitive"
    assert statuses[-1][0][:2] == ("bot-1", "tested")
    assert statuses[-1][1]["connected"] is True


@pytest.mark.asyncio
async def test_test_connection_reports_subscribe_error(monkeypatch):
    class FakeClient:
        def __init__(self, url):
            self.url = url

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def send(self, frame):
            self.sent_frame = frame

        async def receive(self):
            yield json.dumps({"headers": {"req_id": self.sent_frame["headers"]["req_id"]}, "errcode": 40014, "errmsg": "invalid credential"})

    statuses = []
    monkeypatch.setattr(service_module, "WeComAIBotWebSocketClient", FakeClient)
    monkeypatch.setattr(
        service_module.WeComAIBotBindingService,
        "update_status",
        lambda *args, **kwargs: statuses.append((args, kwargs)),
    )

    service = WeComAIBotService(config=WeComAIBotConfig(enabled=True), agent_bridge=FakeBridge())
    result = await service.test_connection({"bot_id": "bot-1", "secret": "sensitive"})

    assert result["ok"] is False
    assert result["response_code"] == 40014
    assert result["message"] == "invalid credential"
    assert statuses[-1][0] == ("bot-1", "error", "invalid credential")


@pytest.mark.asyncio
async def test_test_connection_sanitizes_connection_exception(monkeypatch):
    class FakeClient:
        def __init__(self, url):
            self.url = url

        async def __aenter__(self):
            raise RuntimeError("bad sensitive")

        async def __aexit__(self, exc_type, exc, tb):
            return None

    statuses = []
    monkeypatch.setattr(service_module, "WeComAIBotWebSocketClient", FakeClient)
    monkeypatch.setattr(
        service_module.WeComAIBotBindingService,
        "update_status",
        lambda *args, **kwargs: statuses.append((args, kwargs)),
    )

    service = WeComAIBotService(config=WeComAIBotConfig(enabled=True), agent_bridge=FakeBridge())
    result = await service.test_connection({"bot_id": "bot-1", "secret": "sensitive"})

    assert result["ok"] is False
    assert result["message"] == "bad ********"
    assert statuses[-1][0] == ("bot-1", "error", "bad ********")


@pytest.mark.asyncio
async def test_wait_subscribe_response_rejects_unexpected_callback():
    async def receive():
        yield json.dumps({"cmd": "aibot_msg_callback", "headers": {"req_id": "other"}, "body": {"msgid": "msg-1"}})

    service = WeComAIBotService(config=WeComAIBotConfig(enabled=True, test_connection_timeout_seconds=0), agent_bridge=FakeBridge())

    with pytest.raises(TimeoutError):
        await service._wait_subscribe_response(receive(), {"bot_id": "bot-1", "secret": "sensitive"}, "req-1")


@pytest.mark.asyncio
async def test_wait_subscribe_response_skips_unrelated_frames():
    async def receive():
        yield json.dumps({"cmd": "aibot_msg_callback", "headers": {"req_id": "other"}, "body": {"msgid": "msg-1"}})
        yield json.dumps({"cmd": "aibot_subscribe", "headers": {"req_id": "req-1"}, "errcode": 0, "errmsg": "ok"})

    service = WeComAIBotService(config=WeComAIBotConfig(enabled=True), agent_bridge=FakeBridge())
    result = await service._wait_subscribe_response(receive(), {"bot_id": "bot-1", "secret": "sensitive"}, "req-1")

    assert result["ok"] is True
    assert result["response_code"] == 0


@pytest.mark.asyncio
async def test_wait_subscribe_response_accepts_missing_cmd():
    async def receive():
        yield json.dumps({"headers": {"req_id": "req-1"}, "errcode": 0, "errmsg": "ok"})

    service = WeComAIBotService(config=WeComAIBotConfig(enabled=True), agent_bridge=FakeBridge())
    result = await service._wait_subscribe_response(receive(), {"bot_id": "bot-1", "secret": "sensitive"}, "req-1")

    assert result["ok"] is True
    assert result["response_code"] == 0


@pytest.mark.asyncio
async def test_route_received_payloads_splits_upload_response_from_callbacks():
    async def receive():
        yield json.dumps({"cmd": "aibot_upload_media_init", "headers": {"req_id": "upload-1"}, "body": {"errcode": 0}})
        yield json.dumps({"cmd": "aibot_msg_callback", "headers": {"req_id": "msg-1"}, "body": {"msgid": "msg-1"}})

    callback_queue = asyncio.Queue()
    future = asyncio.get_running_loop().create_future()
    pending = {"upload-1": future}
    response_buffer = {}

    await WeComAIBotService._route_received_payloads(receive(), callback_queue, pending, response_buffer)

    assert future.result()["cmd"] == "aibot_upload_media_init"
    assert (await callback_queue.get())["cmd"] == "aibot_msg_callback"
    assert response_buffer == {}


@pytest.mark.asyncio
async def test_simulate_text_message_returns_debug_stream_result():
    service = WeComAIBotService(
        config=WeComAIBotConfig(enabled=True, stream_interval_ms=0),
        agent_bridge=FakeBridge(),
    )
    service.dedup_store = FakeDedup()
    service.session_store = FakeSessionStore()

    result = await service.simulate_text_message(
        binding={
            "tenant_id": "tenant-1",
            "agent_id": "agent-1",
            "bot_id": "bot-1",
            "enabled": True,
        },
        userid="user-1",
        chatid="chat-1",
        chattype="single",
        content="hello?",
    )

    assert result["reply"] == "hello"
    assert result["frame_count"] > 0
    assert result["session_id"] == "session-1"
    assert result["finish"] is True
    assert result["stream_id"].startswith("wecom-debug-")
    assert result["streams"][-1]["finish"] is True
    assert result["streams"][-1]["content"] == "hello"


@pytest.mark.asyncio
async def test_simulate_media_message_returns_debug_media_shape():
    service = WeComAIBotService(
        config=WeComAIBotConfig(enabled=True, stream_interval_ms=0, conversation_interval_ms=0),
        agent_bridge=FakeBridge(),
        storage=FakeStorage(),
    )
    service.dedup_store = FakeDedup()
    service.session_store = FakeSessionStore()

    result = await service.simulate_media_message(
        binding={
            "tenant_id": "tenant-1",
            "agent_id": "agent-1",
            "bot_id": "bot-1",
            "enabled": True,
        },
        userid="user-1",
        chatid="chat-1",
        chattype="single",
        content="look",
        media={
            "type": "image",
            "filename": "a.png",
            "content_type": "image/png",
            "data_base64": "aW1hZ2U=",
        },
    )

    assert result["reply"] == "hello"
    assert result["finish"] is True
    assert result["stored_references"]
    assert result["rejected_media_reason"] == ""


def test_binding_fingerprint_changes_when_secret_changes():
    left = {
        "tenant_id": "tenant-1",
        "agent_id": "agent-1",
        "bot_id": "bot-1",
        "secret": "secret-1",
        "enabled": True,
    }
    right = {**left, "secret": "secret-2"}

    assert WeComAIBotService._binding_fingerprint(left) != WeComAIBotService._binding_fingerprint(right)
