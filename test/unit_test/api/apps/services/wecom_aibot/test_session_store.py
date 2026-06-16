import api.apps.services.wecom_aibot.session_store as session_store_module
from api.apps.services.wecom_aibot.session_store import WeComAIBotSessionStore


class FakeRedisConn:
    def __init__(self, value=None):
        self.value = value
        self.set_calls = []

    def is_alive(self):
        return True

    def get(self, key):
        return self.value

    def set(self, key, value, exp=3600):
        self.set_calls.append((key, value, exp))
        return True


def test_group_shared_session_uses_chat_as_user_and_prefixes_sender(monkeypatch):
    monkeypatch.setattr(session_store_module, "REDIS_CONN", FakeRedisConn())
    store = WeComAIBotSessionStore(ttl_seconds=60, group_context_mode="shared")

    conversation = store.resolve(
        agent_id="agent-1",
        bot_id="bot-1",
        chattype="group",
        chatid="chat-1",
        userid="user-1",
    )

    assert conversation.ragflow_user_id == "chat-1"
    assert conversation.query_prefix == "[来自 user-1] "


def test_single_session_uses_sender_user(monkeypatch):
    monkeypatch.setattr(session_store_module, "REDIS_CONN", FakeRedisConn())
    store = WeComAIBotSessionStore(ttl_seconds=60)

    conversation = store.resolve(
        agent_id="agent-1",
        bot_id="bot-1",
        chattype="single",
        chatid="chat-1",
        userid="user-1",
    )

    assert conversation.ragflow_user_id == "user-1"
    assert conversation.query_prefix == ""


def test_resolve_reads_existing_session_from_redis(monkeypatch):
    monkeypatch.setattr(session_store_module, "REDIS_CONN", FakeRedisConn("session-1"))
    store = WeComAIBotSessionStore(ttl_seconds=60)

    conversation = store.resolve(
        agent_id="agent-1",
        bot_id="bot-1",
        chattype="single",
        chatid="chat-1",
        userid="user-1",
    )

    assert conversation.ragflow_session_id == "session-1"


def test_save_writes_session_with_ttl(monkeypatch):
    redis = FakeRedisConn()
    monkeypatch.setattr(session_store_module, "REDIS_CONN", redis)
    store = WeComAIBotSessionStore(ttl_seconds=60)

    store.save("conversation-key", "session-1")
    store.save("conversation-key", None)

    assert redis.set_calls == [("wecom:aibot:session:conversation-key", "session-1", 60)]
