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
import logging
import time
from collections.abc import Awaitable, Callable

from rag.utils.redis_conn import REDIS_CONN

from api.apps.services.wecom_aibot.agent_bridge import AgentBridgeResult, WeComAgentBridge
from api.apps.services.wecom_aibot.binding_store import WeComAIBotBindingService
from api.apps.services.wecom_aibot.client import WeComAIBotWebSocketClient
from api.apps.services.wecom_aibot.config import WeComAIBotConfig
from api.apps.services.wecom_aibot.dedup_store import WeComAIBotDedupStore
from api.apps.services.wecom_aibot.media import (
    build_media_query,
    build_reply_media_frames,
    extract_outgoing_media,
    store_inbound_media,
)
from api.apps.services.wecom_aibot.media_download import MediaDownloadFailure, download_incoming_media
from api.apps.services.wecom_aibot.media_upload import TemporaryMediaCache, WeComTemporaryMediaUploader
from api.apps.services.wecom_aibot.protocol import (
    CMD_EVENT_CALLBACK,
    CMD_MSG_CALLBACK,
    CMD_SUBSCRIBE,
    WeComIncomingMessage,
    build_ping_frame,
    build_stream_frame,
    build_subscribe_frame,
    build_welcome_frame,
    extract_event,
    extract_markdown_image_urls,
    extract_media_message,
    extract_text_message,
    parse_payload,
)
from api.apps.services.wecom_aibot.session_store import WeComAIBotSessionStore
from common.misc_utils import thread_pool_exec


FrameSender = Callable[[dict], Awaitable[None]]

LOCK_RENEW_SCRIPT = """
    local current_value = redis.call('get', KEYS[1])
    if current_value and current_value == ARGV[1] then
        redis.call('expire', KEYS[1], tonumber(ARGV[2]))
        return 1
    end
    return 0
"""

RATE_LIMIT_SCRIPT = """
    local key = KEYS[1]
    local now = tonumber(ARGV[1])
    local interval = tonumber(ARGV[2])
    local ttl = tonumber(ARGV[3])
    local last_reserved = tonumber(redis.call('get', key) or '0')
    local scheduled = now
    if last_reserved and last_reserved > now then
        scheduled = last_reserved
    end
    redis.call('psetex', key, ttl, scheduled + interval)
    return scheduled - now
"""


class WeComAIBotService:
    def __init__(
        self,
        config: WeComAIBotConfig | None = None,
        agent_bridge: WeComAgentBridge | None = None,
        storage=None,
        media_downloader=None,
        media_uploader=None,
        wait_response=None,
    ):
        self.config = config or WeComAIBotConfig.from_env()
        self.agent_bridge = agent_bridge or WeComAgentBridge()
        self.dedup_store = WeComAIBotDedupStore(self.config.dedup_ttl_seconds)
        self.session_store = WeComAIBotSessionStore(self.config.session_ttl_seconds, self.config.group_context_mode)
        self.storage = storage
        self.media_downloader = media_downloader or download_incoming_media
        self.media_uploader = media_uploader or WeComTemporaryMediaUploader(TemporaryMediaCache(self.config.media_temp_cache_seconds))
        self.wait_response = wait_response
        self._stop_event = asyncio.Event()
        self._conversation_next_send_at: dict[str, float] = {}
        self._conversation_rate_locks: dict[str, asyncio.Lock] = {}

    def stop(self) -> None:
        self._stop_event.set()

    async def run_forever(self) -> None:
        if not self.config.enabled:
            logging.warning("WeCom AIBot runner is disabled by WECOM_AIBOT_ENABLED.")
            return

        bindings = WeComAIBotBindingService.list_enabled(include_secret=True)
        if not bindings:
            logging.warning("No enabled WeCom AIBot bindings found. Keep polling for changes.")

        await self._supervise_bindings(bindings)

    async def test_connection(self, binding: dict) -> dict:
        frame = build_subscribe_frame(binding["bot_id"], binding.get("secret") or "")
        result = {"ok": False, "cmd": frame["cmd"], "bot_id": binding["bot_id"], "message": ""}
        try:
            async with WeComAIBotWebSocketClient(self.config.ws_url) as client:
                await client.send(frame)
                response = await self._wait_subscribe_response(client.receive(), binding, frame["headers"]["req_id"])
                result.update(response)
                ok = response["ok"]
                response_message = response["message"]
                if ok:
                    WeComAIBotBindingService.update_status(binding["bot_id"], "tested", connected=True)
                else:
                    WeComAIBotBindingService.update_status(binding["bot_id"], "error", response_message)
        except Exception as exc:
            message = self._sanitize_error_message(str(exc), binding.get("secret") or "")
            result["message"] = message
            WeComAIBotBindingService.update_status(binding["bot_id"], "error", message)
        return result

    async def simulate_text_message(
        self,
        binding: dict,
        userid: str,
        chatid: str,
        chattype: str,
        content: str,
    ) -> dict:
        req_id = f"debug-{int(time.time() * 1000)}"
        payload = {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": req_id},
            "body": {
                "msgid": req_id,
                "aibotid": binding["bot_id"],
                "chatid": chatid or userid,
                "chattype": chattype or "single",
                "from": {"userid": userid},
                "msgtype": "text",
                "text": {"content": content},
            },
        }
        sent_frames: list[dict] = []

        async def collect(frame: dict) -> None:
            sent_frames.append(frame)

        latest = await self.handle_payload(payload, collect, binding_override=binding)
        return self._build_simulation_result(sent_frames, latest)

    async def simulate_media_message(
        self,
        binding: dict,
        userid: str,
        chatid: str,
        chattype: str,
        media: dict,
        content: str = "",
    ) -> dict:
        media_type = (media.get("type") or media.get("msgtype") or "image").lower()
        req_id = f"debug-{int(time.time() * 1000)}"
        payload = {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": req_id},
            "body": {
                "msgid": req_id,
                "aibotid": binding["bot_id"],
                "chatid": chatid or userid,
                "chattype": chattype or "single",
                "from": {"userid": userid},
                "msgtype": media_type,
                media_type: {
                    "url": media.get("url") or media.get("download_url") or "",
                    "aeskey": media.get("aeskey") or "",
                    "filename": media.get("filename") or f"debug.{ 'png' if media_type == 'image' else 'bin' }",
                    "content_type": media.get("content_type") or "",
                    "size": media.get("size"),
                    "data_base64": media.get("data_base64") or media.get("content_base64") or "",
                },
                "text": {"content": content},
            },
        }
        sent_frames: list[dict] = []

        async def collect(frame: dict) -> None:
            sent_frames.append(frame)

        latest = await self.handle_payload(payload, collect, binding_override=binding)
        return self._build_simulation_result(sent_frames, latest)

    @staticmethod
    def _build_simulation_result(sent_frames: list[dict], latest: AgentBridgeResult | None) -> dict:
        final_content = ""
        final_stream: dict | None = None
        stream_frames: list[dict] = []
        image_urls: list[str] = []
        for frame in sent_frames:
            body = frame.get("body") or {}
            stream = body.get("stream") or {}
            if stream:
                stream_frames.append(
                    {
                        "id": stream.get("id") or "",
                        "finish": bool(stream.get("finish")),
                        "content": stream.get("content") or "",
                    }
                )
            markdown = (body.get("markdown") or {}).get("content") or ""
            image_urls.extend(extract_markdown_image_urls(markdown))
            if stream.get("finish"):
                final_stream = stream
                final_content = stream.get("content") or ""

        media_debug = getattr(latest, "media_reply_debug", None) if latest else None
        stored_media = getattr(latest, "stored_media", []) if latest else []
        rejected_media_reason = getattr(latest, "rejected_media_reason", "") if latest else ""
        public_urls = list(media_debug.public_urls) if media_debug else []
        uploaded_media_ids = list(media_debug.uploaded_media_ids) if media_debug else []
        media_failures = list(media_debug.failures) if media_debug else []

        return {
            "reply": final_content,
            "frame_count": len(sent_frames),
            "session_id": latest.session_id if latest else None,
            "finish": bool(final_stream and final_stream.get("finish")),
            "stream_id": (final_stream or {}).get("id") or "",
            "streams": stream_frames,
            "image_urls": list(dict.fromkeys([*(latest.image_urls if latest else []), *image_urls, *public_urls])),
            "stored_references": stored_media,
            "public_urls": public_urls,
            "uploaded_media_ids": uploaded_media_ids,
            "rejected_media_reason": rejected_media_reason,
            "media_failures": media_failures,
            "media_frames": list(media_debug.frames) if media_debug else [],
        }

    async def handle_payload(
        self,
        payload: dict,
        send_frame: FrameSender,
        binding_override: dict | None = None,
        wait_response=None,
    ) -> AgentBridgeResult | None:
        event = extract_event(payload)
        if event:
            binding = binding_override or WeComAIBotBindingService.get_by_bot_id(event.aibotid, include_secret=False)
            if binding and binding.get("enabled", True) and event.is_enter_conversation and self.config.welcome_message:
                await send_frame(build_welcome_frame(event.req_id, self.config.welcome_message))
            return None

        message = extract_text_message(payload)
        media_message = None if message else extract_media_message(payload)
        if not message and not media_message:
            return None

        incoming = message or media_message
        binding = binding_override or WeComAIBotBindingService.get_by_bot_id(incoming.aibotid, include_secret=False)
        if not binding or not binding.get("enabled", True):
            logging.warning("No enabled WeCom AIBot binding found for bot_id=%s", incoming.aibotid)
            return None

        if not self.dedup_store.acquire(incoming.msgid):
            logging.info("Skip duplicated WeCom AIBot message: %s", incoming.msgid)
            return None

        if media_message:
            return await self._reply_to_media_message(binding, media_message, send_frame, wait_response=wait_response)
        return await self._reply_to_message(binding, message, send_frame, wait_response=wait_response)

    async def _reply_to_media_message(self, binding: dict, message, send_frame: FrameSender, wait_response=None) -> AgentBridgeResult | None:
        try:
            downloaded = await self.media_downloader(message, self.config)
            stored = await thread_pool_exec(
                store_inbound_media,
                self._storage(),
                binding["tenant_id"],
                binding["agent_id"],
                binding["bot_id"],
                message,
                downloaded,
            )
        except MediaDownloadFailure as exc:
            logging.warning("Reject WeCom AIBot media message msgid=%s reason=%s", message.msgid, exc.reason)
            stream_id = f"wecom-{message.msgid or int(time.time() * 1000)}"
            content = f"媒体消息已拒绝: {exc.reason}"
            await send_frame(build_stream_frame(message.req_id, stream_id, content, True))
            result = AgentBridgeResult(content=content)
            result.rejected_media_reason = exc.reason
            return result

        media_query = build_media_query(message, stored)
        text_message = WeComIncomingMessage(
            req_id=message.req_id,
            msgid=message.msgid,
            aibotid=message.aibotid,
            userid=message.userid,
            chattype=message.chattype,
            chatid=message.chatid,
            msgtype=message.msgtype,
            content=media_query,
            raw=message.raw,
        )
        latest = await self._reply_to_message(binding, text_message, send_frame, wait_response=wait_response)
        latest.stored_media = [stored.reference]
        return latest

    async def _reply_to_message(self, binding: dict, message: WeComIncomingMessage, send_frame: FrameSender, wait_response=None) -> AgentBridgeResult:
        conversation = self.session_store.resolve(
            agent_id=binding["agent_id"],
            bot_id=binding["bot_id"],
            chattype=message.chattype,
            chatid=message.chatid,
            userid=message.userid,
        )
        query = conversation.query_prefix + message.content
        stream_id = f"wecom-{message.msgid or int(time.time() * 1000)}"
        last_sent_at = 0.0
        latest = AgentBridgeResult(session_id=conversation.ragflow_session_id, content="")
        image_urls: list[str] = []

        try:
            async for result in self._run_agent_with_timeout(
                tenant_id=binding["tenant_id"],
                agent_id=binding["agent_id"],
                query=query,
                user_id=conversation.ragflow_user_id,
                session_id=conversation.ragflow_session_id,
            ):
                latest = result
                image_urls = result.image_urls
                now = time.monotonic()
                if now - last_sent_at >= self.config.stream_interval_ms / 1000:
                    await self._send_rate_limited(
                        send_frame,
                        conversation.key,
                        build_stream_frame(message.req_id, stream_id, result.content, False),
                    )
                    last_sent_at = now

            self.session_store.save(conversation.key, latest.session_id)
            await self._send_rate_limited(
                send_frame,
                conversation.key,
                build_stream_frame(message.req_id, stream_id, latest.content, True),
            )
            outgoing_media = extract_outgoing_media(latest.content, image_urls)
            if outgoing_media:
                storage = self._storage() if any(self._requires_storage(item.source) for item in outgoing_media) else self.storage
                media_debug = await build_reply_media_frames(
                    req_id=message.req_id,
                    outgoing=outgoing_media,
                    binding=binding,
                    config=self.config,
                    storage=storage,
                    send_frame=send_frame,
                    uploader=self.media_uploader,
                    wait_response=wait_response or self.wait_response,
                )
                for frame in media_debug.frames:
                    await self._send_rate_limited(
                        send_frame,
                        conversation.key,
                        frame,
                    )
                latest.media_reply_debug = media_debug
            return latest
        except Exception as exc:
            error_message = self._sanitize_error_message(str(exc), binding.get("secret") or "")
            logging.error("WeCom AIBot message handling failed for bot_id=%s: %s", binding["bot_id"], error_message)
            WeComAIBotBindingService.update_status(binding["bot_id"], "error", error_message)
            await self._send_rate_limited(
                send_frame,
                conversation.key,
                build_stream_frame(message.req_id, stream_id, f"**ERROR**: {error_message}", True),
            )
            return AgentBridgeResult(session_id=latest.session_id, content=f"**ERROR**: {error_message}")

    async def _run_agent_with_timeout(self, **kwargs):
        iterator = self.agent_bridge.run(**kwargs)
        timeout_seconds = self.config.max_stream_seconds
        deadline = time.monotonic() + timeout_seconds if timeout_seconds > 0 else None
        try:
            while True:
                try:
                    if deadline is None:
                        result = await iterator.__anext__()
                    else:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise TimeoutError(f"WeCom AIBot stream exceeded {timeout_seconds} seconds.")
                        try:
                            result = await asyncio.wait_for(iterator.__anext__(), timeout=remaining)
                        except TimeoutError as exc:
                            raise TimeoutError(f"WeCom AIBot stream exceeded {timeout_seconds} seconds.") from exc
                except StopAsyncIteration:
                    return
                yield result
        except TimeoutError:
            close = getattr(iterator, "aclose", None)
            if close:
                await close()
            raise

    def _storage(self):
        if self.storage is None:
            from common import settings

            self.storage = settings.STORAGE_IMPL
        return self.storage

    @staticmethod
    def _requires_storage(source: str) -> bool:
        return not source.startswith("https://") or "/api/v1/documents/images/" in source

    async def _run_binding(self, binding: dict) -> None:
        backoff = self.config.reconnect_initial_seconds
        while not self._stop_event.is_set():
            lock_value = f"{binding['bot_id']}:{id(self)}"
            lock_key = f"wecom:aibot:lock:{binding['bot_id']}"
            if not self._acquire_lock(lock_key, lock_value):
                await asyncio.sleep(min(self.config.lock_ttl_seconds, self.config.reconnect_max_seconds))
                continue

            try:
                async with WeComAIBotWebSocketClient(self.config.ws_url) as client:
                    frame = build_subscribe_frame(binding["bot_id"], binding.get("secret") or "")
                    await client.send(frame)
                    response = await self._wait_subscribe_response(client.receive(), binding, frame["headers"]["req_id"])
                    if not response["ok"]:
                        raise RuntimeError(response["message"])
                    WeComAIBotBindingService.update_status(binding["bot_id"], "subscribed", connected=True)
                    receiver = client.receive()
                    callback_queue: asyncio.Queue[dict] = asyncio.Queue()
                    pending_responses: dict[str, asyncio.Future] = {}
                    response_buffer: dict[str, dict] = {}
                    heartbeat_task = asyncio.create_task(self._heartbeat(client.send, lock_key, lock_value))
                    receiver_task = asyncio.create_task(self._route_received_payloads(receiver, callback_queue, pending_responses, response_buffer))
                    callback_task: asyncio.Task | None = None

                    async def wait_response(req_id: str) -> dict:
                        buffered = response_buffer.pop(req_id, None)
                        if buffered is not None:
                            return buffered
                        future = asyncio.get_running_loop().create_future()
                        pending_responses[req_id] = future
                        try:
                            return await asyncio.wait_for(future, timeout=max(self.config.test_connection_timeout_seconds, 1))
                        finally:
                            pending_responses.pop(req_id, None)

                    try:
                        while True:
                            callback_task = asyncio.create_task(callback_queue.get())
                            done, _ = await asyncio.wait(
                                {callback_task, heartbeat_task, receiver_task},
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            if heartbeat_task in done:
                                callback_task.cancel()
                                await asyncio.gather(callback_task, return_exceptions=True)
                                heartbeat_task.result()
                            if receiver_task in done:
                                callback_task.cancel()
                                await asyncio.gather(callback_task, return_exceptions=True)
                                receiver_task.result()
                                break
                            payload = callback_task.result()
                            await self.handle_payload(payload, client.send, binding_override=binding, wait_response=wait_response)
                    finally:
                        for future in pending_responses.values():
                            if not future.done():
                                future.cancel()
                        pending_responses.clear()
                        response_buffer.clear()
                        if callback_task and not callback_task.done():
                            callback_task.cancel()
                        heartbeat_task.cancel()
                        receiver_task.cancel()
                        await asyncio.gather(
                            *(task for task in (callback_task, heartbeat_task, receiver_task) if task),
                            return_exceptions=True,
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error_message = self._sanitize_error_message(str(exc), binding.get("secret") or "")
                logging.error("WeCom AIBot connection failed for bot_id=%s: %s", binding["bot_id"], error_message)
                WeComAIBotBindingService.update_status(
                    binding["bot_id"],
                    "error",
                    error_message,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.config.reconnect_max_seconds)
            finally:
                self._release_lock(lock_key, lock_value)

    async def _heartbeat(self, send_frame: FrameSender, lock_key: str, lock_value: str) -> None:
        while True:
            await asyncio.sleep(self.config.heartbeat_seconds)
            if not self._renew_lock(lock_key, lock_value):
                raise RuntimeError("WeCom AIBot lock renew failed.")
            await send_frame(build_ping_frame())

    @staticmethod
    async def _route_received_payloads(
        receiver,
        callback_queue: asyncio.Queue,
        pending_responses: dict[str, asyncio.Future],
        response_buffer: dict[str, dict],
    ) -> None:
        async for raw in receiver:
            payload = parse_payload(raw)
            req_id = ((payload.get("headers") or {}).get("req_id") or "").strip()
            future = pending_responses.get(req_id) if req_id else None
            if future is not None:
                if not future.done():
                    future.set_result(payload)
                continue
            if req_id and payload.get("cmd") not in {CMD_MSG_CALLBACK, CMD_EVENT_CALLBACK}:
                response_buffer[req_id] = payload
                if len(response_buffer) > 128:
                    response_buffer.pop(next(iter(response_buffer)))
                continue
            await callback_queue.put(payload)

    async def _send_rate_limited(self, send_frame: FrameSender, conversation_key: str, frame: dict) -> None:
        await self._wait_for_conversation_interval(conversation_key)
        await send_frame(frame)

    async def _wait_for_conversation_interval(self, conversation_key: str) -> None:
        interval_seconds = self.config.conversation_interval_ms / 1000
        if interval_seconds <= 0:
            return

        redis_key = f"wecom:aibot:rate:{conversation_key}"
        if REDIS_CONN.is_alive():
            try:
                delay_ms = REDIS_CONN.REDIS.eval(
                    RATE_LIMIT_SCRIPT,
                    1,
                    redis_key,
                    int(time.time() * 1000),
                    int(self.config.conversation_interval_ms),
                    max(int(self.config.conversation_interval_ms) * 4, 1000),
                )
                delay_seconds = max(float(delay_ms or 0) / 1000, 0)
                if delay_seconds > 0:
                    await asyncio.sleep(delay_seconds)
                return
            except Exception:
                logging.exception("WeCom AIBot conversation rate limit failed; falling back to local limiter.")

        lock = self._conversation_rate_locks.setdefault(conversation_key, asyncio.Lock())
        async with lock:
            now = time.monotonic()
            next_send_at = self._conversation_next_send_at.get(conversation_key, 0.0)
            if next_send_at > now:
                await asyncio.sleep(next_send_at - now)
                now = time.monotonic()
            self._conversation_next_send_at[conversation_key] = now + interval_seconds

    async def _supervise_bindings(self, initial_bindings: list[dict] | None = None) -> None:
        active: dict[str, tuple[str, asyncio.Task]] = {}
        bindings = initial_bindings or []
        while not self._stop_event.is_set():
            desired = {binding["bot_id"]: binding for binding in bindings}
            for bot_id, (fingerprint, task) in list(active.items()):
                binding = desired.get(bot_id)
                if not binding or self._binding_fingerprint(binding) != fingerprint or task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    active.pop(bot_id, None)

            for bot_id, binding in desired.items():
                if bot_id not in active:
                    task = asyncio.create_task(self._run_binding(binding))
                    active[bot_id] = (self._binding_fingerprint(binding), task)

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.config.binding_refresh_seconds)
            except TimeoutError:
                bindings = WeComAIBotBindingService.list_enabled(include_secret=True)

        for _, task in active.values():
            task.cancel()
        await asyncio.gather(*(task for _, task in active.values()), return_exceptions=True)

    @staticmethod
    def _binding_fingerprint(binding: dict) -> str:
        return "|".join(
            [
                binding.get("tenant_id") or "",
                binding.get("agent_id") or "",
                binding.get("bot_id") or "",
                binding.get("secret") or "",
                str(bool(binding.get("enabled", True))),
            ]
        )

    def _acquire_lock(self, key: str, value: str) -> bool:
        if not REDIS_CONN.is_alive():
            logging.error("WeCom AIBot single-connection lock requires Redis.")
            return False
        try:
            return bool(REDIS_CONN.REDIS.set(key, value, ex=self.config.lock_ttl_seconds, nx=True))
        except Exception:
            logging.exception("WeCom AIBot lock acquire failed.")
            return False

    @staticmethod
    def _release_lock(key: str, value: str) -> None:
        if REDIS_CONN.is_alive():
            REDIS_CONN.delete_if_equal(key, value)

    def _renew_lock(self, key: str, value: str) -> bool:
        if not REDIS_CONN.is_alive():
            logging.error("WeCom AIBot single-connection lock requires Redis.")
            return False
        try:
            renewed = REDIS_CONN.REDIS.eval(LOCK_RENEW_SCRIPT, 1, key, value, int(self.config.lock_ttl_seconds))
            if not renewed:
                logging.error("WeCom AIBot lock value changed before renewal.")
            return bool(renewed)
        except Exception:
            logging.exception("WeCom AIBot lock renew failed.")
            return False

    async def _wait_subscribe_response(self, receiver, binding: dict, req_id: str) -> dict:
        deadline = time.monotonic() + self.config.test_connection_timeout_seconds
        payload = {}
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Timed out waiting for WeCom AIBot subscribe response.")
            raw = await asyncio.wait_for(receiver.__anext__(), timeout=remaining)
            payload = parse_payload(raw)
            headers = payload.get("headers") or {}
            response_cmd = payload.get("cmd")
            if headers.get("req_id") == req_id and response_cmd in {CMD_SUBSCRIBE, "", None}:
                break

        response_cmd = payload.get("cmd")
        response_code, response_message = self._extract_response_error(payload)
        response_message = self._sanitize_error_message(response_message, binding.get("secret") or "")
        ok = response_code == 0
        if response_code is None:
            response_message = "Subscribe response did not include errcode."
        elif not response_message:
            response_message = f"Subscribe failed with errcode {response_code}."
        return {
            "ok": ok,
            "message": "Subscribe succeeded." if ok else response_message,
            "response_cmd": response_cmd,
            "response_code": response_code,
            "response_message": response_message,
        }

    @staticmethod
    def _extract_response_error(payload: dict) -> tuple[int | None, str]:
        body = payload.get("body") or {}
        errcode = payload.get("errcode", body.get("errcode"))
        errmsg = payload.get("errmsg", body.get("errmsg")) or ""
        try:
            errcode = int(errcode)
        except (TypeError, ValueError):
            errcode = None
        return errcode, errmsg

    @staticmethod
    def _sanitize_error_message(message: str, secret: str) -> str:
        if secret:
            return message.replace(secret, "********")
        return message
