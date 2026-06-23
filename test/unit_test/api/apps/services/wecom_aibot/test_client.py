import asyncio
import sys
import types

import pytest

from api.apps.services.wecom_aibot.client import WeComAIBotWebSocketClient
from api.apps.services.wecom_aibot.protocol import dumps_frame


class FakeWebSocket:
    def __init__(self):
        self.sent = []

    async def send(self, payload):
        await asyncio.sleep(0)
        self.sent.append(payload)

    async def close(self):
        return None


@pytest.mark.asyncio
async def test_connect_disables_protocol_ping(monkeypatch):
    captured = {}

    async def fake_connect(url, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return FakeWebSocket()

    monkeypatch.setitem(sys.modules, "websockets", types.SimpleNamespace(connect=fake_connect))

    async with WeComAIBotWebSocketClient("wss://example.test/ws"):
        pass

    assert captured == {
        "url": "wss://example.test/ws",
        "kwargs": {
            "ping_interval": None,
            "ping_timeout": None,
            "close_timeout": 5,
        },
    }


@pytest.mark.asyncio
async def test_queue_send_serializes_frames_in_order():
    client = WeComAIBotWebSocketClient("ws://example.test")
    client._websocket = FakeWebSocket()
    await client.start_writer()

    await asyncio.gather(
        client.queue_send({"n": 1}),
        client.queue_send({"n": 2}),
        client.queue_send({"n": 3}),
    )

    await client.stop_writer()

    assert client._websocket.sent == [dumps_frame({"n": 1}), dumps_frame({"n": 2}), dumps_frame({"n": 3})]


@pytest.mark.asyncio
async def test_queue_send_requires_started_writer():
    client = WeComAIBotWebSocketClient("ws://example.test")
    client._websocket = FakeWebSocket()

    with pytest.raises(RuntimeError, match="writer is not running"):
        await client.queue_send({"n": 1})
