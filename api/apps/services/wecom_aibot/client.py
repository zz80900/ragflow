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
    def __init__(self, url: str, *, send_timeout_seconds: int = 10, receive_timeout_seconds: int = 90):
        self.url = url
        self._websocket = None
        self.send_timeout_seconds = max(int(send_timeout_seconds), 1)
        self.receive_timeout_seconds = max(int(receive_timeout_seconds), 1)

    async def __aenter__(self) -> "WeComAIBotWebSocketClient":
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("Python package 'websockets' is required for WeCom AIBot runner.") from exc

        self._websocket = await websockets.connect(
            self.url,
            ping_interval=None,
            ping_timeout=None,
            close_timeout=5,
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop_writer()
        if self._websocket:
            await self._websocket.close()
            self._websocket = None

    async def send(self, frame: dict) -> None:
        if not self._websocket:
            raise RuntimeError("WeCom AIBot WebSocket is not connected.")
        await asyncio.wait_for(self._websocket.send(dumps_frame(frame)), timeout=self.send_timeout_seconds)

    async def start_writer(self) -> asyncio.Queue[tuple[dict | None, asyncio.Future | None]]:
        if not self._websocket:
            raise RuntimeError("WeCom AIBot WebSocket is not connected.")
        if getattr(self, "_writer_task", None) and not self._writer_task.done():
            return self._writer_queue
        self._writer_queue: asyncio.Queue[tuple[dict | None, asyncio.Future | None]] = asyncio.Queue()
        self._writer_task = asyncio.create_task(self._writer_loop())
        return self._writer_queue

    async def stop_writer(self) -> None:
        writer_task = getattr(self, "_writer_task", None)
        writer_queue = getattr(self, "_writer_queue", None)
        if writer_queue is not None and writer_task and not writer_task.done():
            await writer_queue.put((None, None))
            await asyncio.gather(writer_task, return_exceptions=True)
        self._writer_task = None
        self._writer_queue = None

    async def queue_send(self, frame: dict) -> None:
        writer_queue = getattr(self, "_writer_queue", None)
        writer_task = getattr(self, "_writer_task", None)
        if writer_queue is None or writer_task is None or writer_task.done():
            raise RuntimeError("WeCom AIBot WebSocket writer is not running.")
        completion = asyncio.get_running_loop().create_future()
        await writer_queue.put((frame, completion))
        await completion

    async def receive(self) -> AsyncIterator[str]:
        if not self._websocket:
            raise RuntimeError("WeCom AIBot WebSocket is not connected.")
        while True:
            try:
                yield await asyncio.wait_for(self._websocket.recv(), timeout=self.receive_timeout_seconds)
            except TimeoutError as exc:
                raise TimeoutError("WeCom AIBot WebSocket receive timed out.") from exc
            except asyncio.CancelledError:
                raise

    async def _writer_loop(self) -> None:
        while True:
            frame, completion = await self._writer_queue.get()
            if frame is None:
                if completion and not completion.done():
                    completion.set_result(None)
                return
            try:
                await self.send(frame)
            except Exception as exc:
                if completion and not completion.done():
                    completion.set_exception(exc)
                raise
            else:
                if completion and not completion.done():
                    completion.set_result(None)
