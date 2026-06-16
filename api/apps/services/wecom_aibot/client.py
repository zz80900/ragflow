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

import asyncio
from collections.abc import AsyncIterator

from api.apps.services.wecom_aibot.protocol import dumps_frame


class WeComAIBotWebSocketClient:
    def __init__(self, url: str):
        self.url = url
        self._websocket = None

    async def __aenter__(self) -> "WeComAIBotWebSocketClient":
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("Python package 'websockets' is required for WeCom AIBot runner.") from exc

        self._websocket = await websockets.connect(self.url)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._websocket:
            await self._websocket.close()
            self._websocket = None

    async def send(self, frame: dict) -> None:
        if not self._websocket:
            raise RuntimeError("WeCom AIBot WebSocket is not connected.")
        await self._websocket.send(dumps_frame(frame))

    async def receive(self) -> AsyncIterator[str]:
        if not self._websocket:
            raise RuntimeError("WeCom AIBot WebSocket is not connected.")
        while True:
            try:
                yield await self._websocket.recv()
            except asyncio.CancelledError:
                raise
