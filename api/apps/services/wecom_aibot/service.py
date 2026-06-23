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

import asyncio
import heapq
import hashlib
import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from rag.utils.redis_conn import REDIS_CONN

from api.apps.services.wecom_aibot.agent_bridge import AgentBridgeResult, WeComAgentBridge
from api.apps.services.wecom_aibot.binding_store import WeComAIBotBindingService
from api.apps.services.wecom_aibot.client import WeComAIBotWebSocketClient
from api.apps.services.wecom_aibot.config import WeComAIBotConfig
from api.apps.services.wecom_aibot.dedup_store import WeComAIBotDedupStore
from api.apps.services.wecom_aibot.media import (
    MediaReplyDebug,
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
    WeComIncomingEvent,
    WeComIncomingMedia,
    WeComIncomingMessage,
    build_markdown_frame,
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
ResponseWaiter = Callable[[str], Awaitable[dict]]

PRIORITY_RECOVERY = 0
PRIORITY_CONTROL = 5
PRIORITY_STREAM_FINAL = 10
PRIORITY_STREAM_UPDATE = 20
PRIORITY_MEDIA = 30
PRIORITY_STOP = 99

EMPTY_RESPONSE_MESSAGE = "抱歉，我暂时无法回答这个问题。"
BUSY_MESSAGE = "系统繁忙，请稍后再试。"
QUEUE_TIMEOUT_MESSAGE = "消息排队超时，请稍后再试。"
GENERIC_ERROR_MESSAGE = "抱歉，处理您的问题时出现了异常，请稍后再试。"
TIMEOUT_ERROR_MESSAGE = "抱歉，处理超时，请稍后再试。"
DATASET_NOT_SELECTED_MESSAGE = "当前机器人未配置知识库，请联系管理员检查知识库设置。"
UNAUTHORIZED_TOOL_MESSAGE = "外部工具认证失败，请联系管理员检查相关连接配置。"
NEW_SESSION_COMMAND = "/new"
NEW_SESSION_MESSAGE = "已清空当前会话缓存，并开始新的对话。"

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


@dataclass
class PreparedPayload:
    payload: dict[str, Any]
    binding: dict[str, Any]
    event: WeComIncomingEvent | None = None
    message: WeComIncomingMessage | None = None
    media_message: WeComIncomingMedia | None = None
    conversation_key: str = ""
    stream_id: str = ""
    accepted_at: float = field(default_factory=time.monotonic)

    @property
    def req_id(self) -> str:
        incoming = self.message or self.media_message or self.event
        return incoming.req_id if incoming else ""

    @property
    def msgid(self) -> str:
        incoming = self.message or self.media_message
        return incoming.msgid if incoming else ""

    @property
    def is_event(self) -> bool:
        return self.event is not None


@dataclass
class ActiveStreamState:
    req_id: str
    conversation_key: str
    stream_id: str
    latest_content: str = ""
    finished: bool = False
    final_sent: bool = False
    last_sent_at: float = 0.0
    last_sent_content: str = ""
    revision: int = 0
    flushed_revision: int = 0
    pending_media_frames: list[dict[str, Any]] = field(default_factory=list)
    media_enqueued: bool = False


@dataclass
class OutboundEnvelope:
    kind: str
    priority: int
    sequence: int
    frame: dict[str, Any] | None = None
    conversation_key: str = ""
    stream_id: str = ""
    revision: int = 0
    apply_pacing: bool = False
    retryable: bool = False
    attempts: int = 0
    force_send: bool = False


@dataclass
class BindingRuntime:
    binding: dict[str, Any]
    inbound_queue: asyncio.Queue[PreparedPayload | None]
    outbound_queue: asyncio.PriorityQueue[tuple[int, int, OutboundEnvelope]]
    active_streams: dict[str, ActiveStreamState] = field(default_factory=dict)
    conversation_inflight: dict[str, int] = field(default_factory=dict)
    pending_responses: dict[str, asyncio.Future] = field(default_factory=dict)
    response_buffer: dict[str, dict[str, Any]] = field(default_factory=dict)
    connection_ready: asyncio.Event = field(default_factory=asyncio.Event)
    connection_failure: asyncio.Future | None = None
    send_frame: FrameSender | None = None
    wait_response: ResponseWaiter | None = None
    workers: list[asyncio.Task] = field(default_factory=list)
    writer_task: asyncio.Task | None = None
    sequence: int = 0
    stopped: bool = False


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
                if response["ok"]:
                    WeComAIBotBindingService.update_status(binding["bot_id"], "tested", connected=True)
                else:
                    WeComAIBotBindingService.update_status(binding["bot_id"], "error", response["message"])
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
                    "filename": media.get("filename") or f"debug.{'png' if media_type == 'image' else 'bin'}",
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
        last_markdown = ""
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
            if markdown:
                last_markdown = markdown
            image_urls.extend(extract_markdown_image_urls(markdown))
            if stream.get("finish"):
                final_stream = stream
                final_content = stream.get("content") or ""
        if not final_content and last_markdown:
            final_content = last_markdown

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
        prepared = self._prepare_payload(payload, binding_override=binding_override, acquire_dedup=True)
        if prepared is None:
            return None
        return await self._process_prepared_payload(
            prepared,
            runtime=None,
            send_frame=send_frame,
            wait_response=wait_response or self.wait_response,
        )

    async def _process_prepared_payload(
        self,
        prepared: PreparedPayload,
        runtime: BindingRuntime | None,
        send_frame: FrameSender | None,
        wait_response: ResponseWaiter | None,
    ) -> AgentBridgeResult | None:
        if prepared.event:
            if prepared.binding.get("enabled", True) and prepared.event.is_enter_conversation and self.config.welcome_message:
                await self._send_plain_frame(
                    runtime=runtime,
                    send_frame=send_frame,
                    frame=build_welcome_frame(prepared.event.req_id, self.config.welcome_message),
                    conversation_key=prepared.conversation_key,
                    apply_pacing=False,
                    priority=PRIORITY_CONTROL,
                    retryable=True,
                )
            return None

        incoming = prepared.message or prepared.media_message
        is_immediate_command = bool(prepared.message and self._is_new_session_command(prepared.message.content))
        if incoming and prepared.req_id and prepared.stream_id and not is_immediate_command:
            await self._send_plain_frame(
                runtime=runtime,
                send_frame=send_frame,
                frame=build_stream_frame(prepared.req_id, prepared.stream_id, "", False),
                conversation_key=prepared.conversation_key,
                apply_pacing=False,
                priority=PRIORITY_CONTROL,
                retryable=True,
            )

        try:
            if prepared.media_message:
                return await self._reply_to_media_message(
                    prepared.binding,
                    prepared.media_message,
                    prepared.stream_id,
                    prepared.conversation_key,
                    runtime=runtime,
                    send_frame=send_frame,
                    wait_response=wait_response,
                )

            if prepared.message:
                return await self._reply_to_message(
                    prepared.binding,
                    prepared.message,
                    prepared.stream_id,
                    prepared.conversation_key,
                    runtime=runtime,
                    send_frame=send_frame,
                    wait_response=wait_response,
                )
        except Exception as exc:
            return await self._handle_message_error(
                binding=prepared.binding,
                runtime=runtime,
                send_frame=send_frame,
                conversation_key=prepared.conversation_key,
                req_id=prepared.req_id,
                stream_id=prepared.stream_id,
                session_id=None,
                exc=exc,
            )
        return None

    async def _reply_to_media_message(
        self,
        binding: dict,
        message: WeComIncomingMedia,
        stream_id: str,
        conversation_key: str,
        *,
        runtime: BindingRuntime | None,
        send_frame: FrameSender | None,
        wait_response: ResponseWaiter | None,
    ) -> AgentBridgeResult | None:
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
            content = f"媒体消息已拒绝: {exc.reason}"
            await self._publish_stream_update(
                runtime=runtime,
                send_frame=send_frame,
                conversation_key=conversation_key,
                req_id=message.req_id,
                stream_id=stream_id,
                content=content,
                finish=True,
            )
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
        latest = await self._reply_to_message(
            binding,
            text_message,
            stream_id,
            conversation_key,
            runtime=runtime,
            send_frame=send_frame,
            wait_response=wait_response,
        )
        latest = latest or AgentBridgeResult(content="")
        latest.stored_media = [stored.reference]
        return latest

    async def _reply_to_message(
        self,
        binding: dict,
        message: WeComIncomingMessage,
        stream_id: str,
        conversation_key: str,
        *,
        runtime: BindingRuntime | None,
        send_frame: FrameSender | None,
        wait_response: ResponseWaiter | None,
    ) -> AgentBridgeResult:
        latest = AgentBridgeResult(session_id=None, content="")
        image_urls: list[str] = []
        last_aggregated_at = 0.0
        active_conversation_key = conversation_key

        try:
            if self._is_new_session_command(message.content):
                return await self._reset_conversation(
                    binding=binding,
                    message=message,
                    stream_id=stream_id,
                    conversation_key=conversation_key,
                    runtime=runtime,
                    send_frame=send_frame,
                )

            conversation = self.session_store.resolve(
                agent_id=binding["agent_id"],
                bot_id=binding["bot_id"],
                chattype=message.chattype,
                chatid=message.chatid,
                userid=message.userid,
            )
            active_conversation_key = conversation.key
            latest.session_id = conversation.ragflow_session_id
            query = conversation.query_prefix + message.content
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
                if now - last_aggregated_at >= self.config.effective_aggregation_interval_ms / 1000:
                    await self._publish_stream_update(
                        runtime=runtime,
                        send_frame=send_frame,
                        conversation_key=active_conversation_key,
                        req_id=message.req_id,
                        stream_id=stream_id,
                        content=result.content,
                        finish=False,
                    )
                    last_aggregated_at = now

            if not (latest.content or "").strip() and not image_urls:
                latest.content = EMPTY_RESPONSE_MESSAGE

            self.session_store.save(active_conversation_key, latest.session_id)
            await self._publish_stream_update(
                runtime=runtime,
                send_frame=send_frame,
                conversation_key=active_conversation_key,
                req_id=message.req_id,
                stream_id=stream_id,
                content=latest.content,
                finish=True,
            )
            media_debug = await self._build_media_reply_frames(
                latest=latest,
                image_urls=image_urls,
                binding=binding,
                runtime=runtime,
                conversation_key=active_conversation_key,
                send_frame=send_frame,
                wait_response=wait_response,
                req_id=message.req_id,
            )
            if media_debug:
                await self._send_media_frames(
                    runtime=runtime,
                    send_frame=send_frame,
                    conversation_key=active_conversation_key,
                    frames=media_debug.frames,
                )
                latest.media_reply_debug = media_debug
            return latest
        except Exception as exc:
            return await self._handle_message_error(
                binding=binding,
                runtime=runtime,
                send_frame=send_frame,
                conversation_key=active_conversation_key,
                req_id=message.req_id,
                stream_id=stream_id,
                session_id=latest.session_id,
                exc=exc,
            )

    @staticmethod
    def _is_new_session_command(content: str) -> bool:
        return (content or "").strip().lower() == NEW_SESSION_COMMAND

    async def _reset_conversation(
        self,
        *,
        binding: dict,
        message: WeComIncomingMessage,
        stream_id: str,
        conversation_key: str,
        runtime: BindingRuntime | None,
        send_frame: FrameSender | None,
    ) -> AgentBridgeResult:
        self.session_store.clear_for_conversation(
            agent_id=binding["agent_id"],
            bot_id=binding["bot_id"],
            chattype=message.chattype,
            chatid=message.chatid,
            userid=message.userid,
        )
        await self._send_plain_frame(
            runtime=runtime,
            send_frame=send_frame,
            frame=build_markdown_frame(message.req_id, NEW_SESSION_MESSAGE),
            conversation_key=conversation_key,
            apply_pacing=False,
            priority=PRIORITY_CONTROL,
            retryable=True,
        )
        return AgentBridgeResult(session_id=None, content=NEW_SESSION_MESSAGE)

    async def _send_media_frames(
        self,
        *,
        runtime: BindingRuntime | None,
        send_frame: FrameSender | None,
        conversation_key: str,
        frames: list[dict[str, Any]],
    ) -> None:
        for frame in frames:
            if runtime is None:
                await self._send_rate_limited(send_frame, conversation_key, frame)
            else:
                await self._enqueue_frame(
                    runtime,
                    frame,
                    conversation_key=conversation_key,
                    apply_pacing=True,
                    priority=PRIORITY_MEDIA,
                    retryable=True,
                )

    async def _build_media_reply_frames(
        self,
        *,
        latest: AgentBridgeResult,
        image_urls: list[str],
        binding: dict,
        runtime: BindingRuntime | None,
        conversation_key: str,
        send_frame: FrameSender | None,
        wait_response: ResponseWaiter | None,
        req_id: str,
    ) -> MediaReplyDebug | None:
        outgoing_media = extract_outgoing_media(latest.content, image_urls)
        if not outgoing_media:
            return None
        try:
            storage = self._storage() if any(self._requires_storage(item.source) for item in outgoing_media) else self.storage
            return await build_reply_media_frames(
                req_id=req_id,
                outgoing=outgoing_media,
                binding=binding,
                config=self.config,
                storage=storage,
                send_frame=self._runtime_media_sender(runtime, conversation_key) if runtime else send_frame,
                uploader=self.media_uploader,
                wait_response=wait_response or (self._runtime_wait_response(runtime) if runtime else self.wait_response),
            )
        except Exception:
            logging.exception(
                "WeCom AIBot media reply build failed for bot_id=%s conversation_key=%s",
                binding["bot_id"],
                conversation_key,
            )
            return MediaReplyDebug(failures=["media_reply_failed"])

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
        runtime = self._create_binding_runtime(binding)
        await self._start_binding_runtime(runtime)
        backoff = self.config.reconnect_initial_seconds
        lock_key = f"wecom:aibot:lock:{binding['bot_id']}"
        lock_value = f"{binding['bot_id']}:{id(self)}"
        try:
            while not self._stop_event.is_set():
                if not self._acquire_lock(lock_key, lock_value):
                    await asyncio.sleep(min(self.config.lock_ttl_seconds, self.config.reconnect_max_seconds))
                    continue

                try:
                    async with WeComAIBotWebSocketClient(
                        self.config.ws_url,
                        send_timeout_seconds=self.config.ws_send_timeout_seconds,
                        receive_timeout_seconds=self.config.ws_receive_timeout_seconds,
                    ) as client:
                        frame = build_subscribe_frame(binding["bot_id"], binding.get("secret") or "")
                        await client.send(frame)
                        response = await self._wait_subscribe_response(client.receive(), binding, frame["headers"]["req_id"])
                        if not response["ok"]:
                            raise RuntimeError(response["message"])

                        await client.start_writer()
                        WeComAIBotBindingService.update_status(binding["bot_id"], "subscribed", connected=True)
                        self._attach_connection(runtime, client)
                        heartbeat_task = asyncio.create_task(self._heartbeat_runtime(runtime, lock_key, lock_value))
                        receiver_task = asyncio.create_task(self._receive_binding_payloads(client.receive(), runtime))
                        await self._enqueue_stream_recovery(runtime)

                        try:
                            wait_targets = {heartbeat_task, receiver_task}
                            if runtime.connection_failure is not None:
                                wait_targets.add(runtime.connection_failure)
                            done, _ = await asyncio.wait(wait_targets, return_when=asyncio.FIRST_COMPLETED)
                            if runtime.connection_failure in done:
                                runtime.connection_failure.result()
                            if heartbeat_task in done:
                                heartbeat_task.result()
                            if receiver_task in done:
                                receiver_task.result()
                        finally:
                            self._detach_connection(runtime)
                            heartbeat_task.cancel()
                            receiver_task.cancel()
                            await asyncio.gather(heartbeat_task, receiver_task, return_exceptions=True)
                            await client.stop_writer()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    error_message = self._sanitize_error_message(str(exc), binding.get("secret") or "")
                    logging.error("WeCom AIBot connection failed for bot_id=%s: %s", binding["bot_id"], error_message)
                    WeComAIBotBindingService.update_status(binding["bot_id"], "error", error_message)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, self.config.reconnect_max_seconds)
                else:
                    backoff = self.config.reconnect_initial_seconds
                finally:
                    self._release_lock(lock_key, lock_value)
        finally:
            await self._stop_binding_runtime(runtime)

    def _create_binding_runtime(self, binding: dict) -> BindingRuntime:
        outbound_queue_size = max(self.config.inbound_queue_size * 4, self.config.worker_count * 4, 16)
        return BindingRuntime(
            binding=binding,
            inbound_queue=asyncio.Queue(max(max(self.config.inbound_queue_size, 1), 1)),
            outbound_queue=asyncio.PriorityQueue(maxsize=outbound_queue_size),
        )

    async def _start_binding_runtime(self, runtime: BindingRuntime) -> None:
        runtime.writer_task = asyncio.create_task(self._outbound_writer(runtime))
        worker_count = max(self.config.worker_count, 1)
        runtime.workers = [
            asyncio.create_task(self._inbound_worker(runtime, worker_id))
            for worker_id in range(worker_count)
        ]

    async def _stop_binding_runtime(self, runtime: BindingRuntime) -> None:
        runtime.stopped = True
        runtime.connection_ready.set()
        for _ in runtime.workers:
            try:
                runtime.inbound_queue.put_nowait(None)
            except asyncio.QueueFull:
                break
        if runtime.writer_task:
            stop_enqueued = await self._put_outbound_envelope(
                runtime,
                OutboundEnvelope(kind="stop", priority=PRIORITY_STOP, sequence=0),
            )
            if not stop_enqueued:
                runtime.writer_task.cancel()
        if runtime.connection_failure and not runtime.connection_failure.done():
            runtime.connection_failure.cancel()
        for future in runtime.pending_responses.values():
            if not future.done():
                future.cancel()
        await asyncio.gather(*(task for task in runtime.workers if task), return_exceptions=True)
        if runtime.writer_task:
            await asyncio.gather(runtime.writer_task, return_exceptions=True)

    def _attach_connection(self, runtime: BindingRuntime, client: WeComAIBotWebSocketClient) -> None:
        async def send_via_client(frame: dict) -> None:
            await client.queue_send(frame)

        async def wait_response(req_id: str) -> dict:
            buffered = runtime.response_buffer.pop(req_id, None)
            if buffered is not None:
                return buffered
            future = asyncio.get_running_loop().create_future()
            runtime.pending_responses[req_id] = future
            try:
                return await asyncio.wait_for(future, timeout=max(self.config.test_connection_timeout_seconds, 1))
            finally:
                runtime.pending_responses.pop(req_id, None)

        runtime.send_frame = send_via_client
        runtime.wait_response = wait_response
        runtime.connection_failure = asyncio.get_running_loop().create_future()
        runtime.connection_ready.set()

    def _detach_connection(self, runtime: BindingRuntime) -> None:
        runtime.connection_ready.clear()
        runtime.send_frame = None
        runtime.wait_response = None
        for future in runtime.pending_responses.values():
            if not future.done():
                future.cancel()
        runtime.pending_responses.clear()
        runtime.response_buffer.clear()

    async def _heartbeat_runtime(self, runtime: BindingRuntime, lock_key: str, lock_value: str) -> None:
        async def send(frame: dict) -> None:
            await self._send_plain_frame(
                runtime=runtime,
                send_frame=None,
                frame=frame,
                conversation_key="",
                apply_pacing=False,
                priority=PRIORITY_CONTROL,
                retryable=False,
            )

        await self._heartbeat(send, lock_key, lock_value)

    async def _receive_binding_payloads(self, receiver, runtime: BindingRuntime) -> None:
        async for raw in receiver:
            payload = parse_payload(raw)
            req_id = ((payload.get("headers") or {}).get("req_id") or "").strip()
            future = runtime.pending_responses.get(req_id) if req_id else None
            if future is not None:
                if not future.done():
                    future.set_result(payload)
                continue
            if req_id and payload.get("cmd") not in {CMD_MSG_CALLBACK, CMD_EVENT_CALLBACK}:
                runtime.response_buffer[req_id] = payload
                if len(runtime.response_buffer) > 128:
                    runtime.response_buffer.pop(next(iter(runtime.response_buffer)))
                continue
            await self._enqueue_callback(runtime, payload)

    async def _enqueue_callback(self, runtime: BindingRuntime, payload: dict[str, Any]) -> None:
        prepared = self._prepare_payload(payload, binding_override=runtime.binding, acquire_dedup=True)
        if prepared is None:
            return

        if prepared.conversation_key:
            inflight = runtime.conversation_inflight.get(prepared.conversation_key, 0)
            if self.config.per_conversation_max_inflight > 0 and inflight >= self.config.per_conversation_max_inflight:
                logging.warning(
                    "WeCom AIBot reject callback for bot_id=%s conversation_key=%s reason=per_conversation_limit current=%s",
                    runtime.binding["bot_id"],
                    prepared.conversation_key,
                    inflight,
                )
                await self._send_terminal_response(runtime, prepared, BUSY_MESSAGE)
                return

        if runtime.inbound_queue.full():
            logging.warning(
                "WeCom AIBot reject callback for bot_id=%s reason=inbound_queue_full depth=%s",
                runtime.binding["bot_id"],
                runtime.inbound_queue.qsize(),
            )
            await self._send_terminal_response(runtime, prepared, BUSY_MESSAGE)
            return

        if prepared.conversation_key:
            runtime.conversation_inflight[prepared.conversation_key] = runtime.conversation_inflight.get(prepared.conversation_key, 0) + 1
        await runtime.inbound_queue.put(prepared)
        logging.info(
            "WeCom AIBot queued callback bot_id=%s queue_depth=%s conversation_key=%s inflight=%s",
            runtime.binding["bot_id"],
            runtime.inbound_queue.qsize(),
            prepared.conversation_key,
            runtime.conversation_inflight.get(prepared.conversation_key, 0),
        )

    async def _inbound_worker(self, runtime: BindingRuntime, worker_id: int) -> None:
        queue_timeout_seconds = max(self.config.queue_wait_timeout_seconds, 0)
        while True:
            prepared = await runtime.inbound_queue.get()
            if prepared is None:
                return
            wait_seconds = time.monotonic() - prepared.accepted_at
            try:
                if queue_timeout_seconds and wait_seconds > queue_timeout_seconds:
                    logging.warning(
                        "WeCom AIBot queue timeout bot_id=%s worker=%s conversation_key=%s wait_seconds=%.3f",
                        runtime.binding["bot_id"],
                        worker_id,
                        prepared.conversation_key,
                        wait_seconds,
                    )
                    await self._send_terminal_response(runtime, prepared, QUEUE_TIMEOUT_MESSAGE)
                    continue

                logging.info(
                    "WeCom AIBot worker start bot_id=%s worker=%s queue_depth=%s conversation_key=%s",
                    runtime.binding["bot_id"],
                    worker_id,
                    runtime.inbound_queue.qsize(),
                    prepared.conversation_key,
                )
                await self._process_prepared_payload(
                    prepared,
                    runtime=runtime,
                    send_frame=None,
                    wait_response=self._runtime_wait_response(runtime),
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception(
                    "WeCom AIBot worker failed bot_id=%s worker=%s conversation_key=%s",
                    runtime.binding["bot_id"],
                    worker_id,
                    prepared.conversation_key,
                )
            finally:
                if prepared.conversation_key:
                    current = runtime.conversation_inflight.get(prepared.conversation_key, 0)
                    if current <= 1:
                        runtime.conversation_inflight.pop(prepared.conversation_key, None)
                    else:
                        runtime.conversation_inflight[prepared.conversation_key] = current - 1

    async def _outbound_writer(self, runtime: BindingRuntime) -> None:
        while True:
            _, _, envelope = await runtime.outbound_queue.get()
            if envelope.kind == "stop":
                return
            try:
                if envelope.kind == "stream":
                    await self._flush_stream_state(runtime, envelope)
                else:
                    await self._deliver_frame(runtime, envelope)
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception(
                    "WeCom AIBot outbound writer failed bot_id=%s kind=%s conversation_key=%s stream_id=%s",
                    runtime.binding["bot_id"],
                    envelope.kind,
                    envelope.conversation_key,
                    envelope.stream_id,
                )

    async def _flush_stream_state(self, runtime: BindingRuntime, envelope: OutboundEnvelope) -> None:
        state = runtime.active_streams.get(envelope.stream_id)
        if state is None or state.final_sent:
            return
        if not envelope.force_send and not state.finished and state.flushed_revision >= state.revision:
            return
        if (
            not envelope.force_send
            and not state.finished
            and state.latest_content == state.last_sent_content
            and state.flushed_revision >= envelope.revision
        ):
            return

        frame = build_stream_frame(state.req_id, state.stream_id, state.latest_content, state.finished)
        try:
            await self._deliver_payload(
                runtime,
                frame,
                conversation_key=state.conversation_key,
                apply_pacing=not state.finished and not envelope.force_send,
            )
        except Exception as exc:
            logging.error(
                "WeCom AIBot stream send failed bot_id=%s conversation_key=%s stream_id=%s: %s",
                runtime.binding["bot_id"],
                state.conversation_key,
                state.stream_id,
                exc,
            )
            return

        state.last_sent_at = time.monotonic()
        state.last_sent_content = state.latest_content
        state.flushed_revision = max(state.flushed_revision, state.revision)
        if state.finished:
            state.final_sent = True
            logging.info(
                "WeCom AIBot final stream sent bot_id=%s conversation_key=%s stream_id=%s",
                runtime.binding["bot_id"],
                state.conversation_key,
                state.stream_id,
            )
            await self._enqueue_pending_media(runtime, state)
            runtime.active_streams.pop(state.stream_id, None)

    async def _enqueue_pending_media(self, runtime: BindingRuntime, state: ActiveStreamState) -> None:
        if state.media_enqueued:
            return
        state.media_enqueued = True
        for frame in state.pending_media_frames:
            await self._enqueue_frame(
                runtime,
                frame,
                conversation_key=state.conversation_key,
                apply_pacing=True,
                priority=PRIORITY_MEDIA,
                retryable=True,
            )

    async def _deliver_frame(self, runtime: BindingRuntime, envelope: OutboundEnvelope) -> None:
        if envelope.frame is None:
            return
        try:
            await self._deliver_payload(
                runtime,
                envelope.frame,
                conversation_key=envelope.conversation_key,
                apply_pacing=envelope.apply_pacing,
            )
        except Exception as exc:
            logging.error(
                "WeCom AIBot send-path failure bot_id=%s conversation_key=%s stream_id=%s cmd=%s: %s",
                runtime.binding["bot_id"],
                envelope.conversation_key,
                envelope.stream_id,
                envelope.frame.get("cmd"),
                exc,
            )
            if envelope.retryable and envelope.attempts == 0:
                envelope.attempts += 1
                await self._put_outbound_envelope(runtime, envelope)

    async def _deliver_payload(
        self,
        runtime: BindingRuntime,
        frame: dict[str, Any],
        *,
        conversation_key: str,
        apply_pacing: bool,
    ) -> None:
        await self._wait_for_connection(runtime)
        if apply_pacing and conversation_key:
            await self._wait_for_conversation_interval(conversation_key)
        sender = runtime.send_frame
        if sender is None:
            raise RuntimeError("WeCom AIBot writer has no active connection.")
        try:
            await sender(frame)
        except Exception as exc:
            self._mark_connection_failed(runtime, exc)
            raise

    async def _wait_for_connection(self, runtime: BindingRuntime) -> None:
        while not runtime.stopped and not self._stop_event.is_set():
            if runtime.connection_ready.is_set() and runtime.send_frame is not None:
                return
            try:
                await asyncio.wait_for(runtime.connection_ready.wait(), timeout=1)
            except TimeoutError:
                continue
        raise RuntimeError("WeCom AIBot runtime is stopping.")

    def _mark_connection_failed(self, runtime: BindingRuntime, exc: Exception) -> None:
        runtime.connection_ready.clear()
        if runtime.connection_failure is not None and not runtime.connection_failure.done():
            runtime.connection_failure.set_exception(exc)

    async def _heartbeat(self, send_frame: FrameSender, lock_key: str, lock_value: str) -> None:
        while True:
            await asyncio.sleep(self.config.heartbeat_seconds)
            if not self._renew_lock(lock_key, lock_value):
                raise RuntimeError("WeCom AIBot lock renew failed.")
            await send_frame(build_ping_frame())

    async def _send_terminal_response(self, runtime: BindingRuntime, prepared: PreparedPayload, content: str) -> None:
        if not prepared.req_id or not prepared.stream_id:
            return
        await self._enqueue_frame(
            runtime,
            build_stream_frame(prepared.req_id, prepared.stream_id, content, True),
            conversation_key=prepared.conversation_key,
            apply_pacing=False,
            priority=PRIORITY_STREAM_FINAL,
            retryable=True,
        )

    async def _handle_message_error(
        self,
        *,
        binding: dict,
        runtime: BindingRuntime | None,
        send_frame: FrameSender | None,
        conversation_key: str,
        req_id: str,
        stream_id: str,
        session_id: str | None,
        exc: Exception,
    ) -> AgentBridgeResult:
        error_message = self._sanitize_error_message(str(exc), binding.get("secret") or "")
        logging.error("WeCom AIBot message handling failed for bot_id=%s: %s", binding["bot_id"], error_message)
        WeComAIBotBindingService.update_status(binding["bot_id"], "error", error_message)
        user_message = self._user_visible_error_message(error_message)
        if req_id and stream_id:
            await self._publish_stream_update(
                runtime=runtime,
                send_frame=send_frame,
                conversation_key=conversation_key,
                req_id=req_id,
                stream_id=stream_id,
                content=user_message,
                finish=True,
            )
        return AgentBridgeResult(session_id=session_id, content=user_message)

    @staticmethod
    def _user_visible_error_message(error_message: str) -> str:
        lowered = (error_message or "").lower()
        if "no dataset is selected" in lowered:
            return DATASET_NOT_SELECTED_MESSAGE
        if "401" in lowered or "unauthorized" in lowered:
            return UNAUTHORIZED_TOOL_MESSAGE
        if "timed out" in lowered or "timeout" in lowered or "exceeded" in lowered:
            return TIMEOUT_ERROR_MESSAGE
        return GENERIC_ERROR_MESSAGE

    async def _publish_stream_update(
        self,
        *,
        runtime: BindingRuntime | None,
        send_frame: FrameSender | None,
        conversation_key: str,
        req_id: str,
        stream_id: str,
        content: str,
        finish: bool,
        pending_media_frames: list[dict[str, Any]] | None = None,
    ) -> None:
        if runtime is None:
            frame = build_stream_frame(req_id, stream_id, content, finish)
            await self._send_rate_limited(send_frame, conversation_key, frame)
            return

        state = runtime.active_streams.get(stream_id)
        if state is None:
            state = ActiveStreamState(req_id=req_id, conversation_key=conversation_key, stream_id=stream_id)
            runtime.active_streams[stream_id] = state
        state.latest_content = content
        state.finished = state.finished or finish
        state.revision += 1
        if pending_media_frames is not None:
            state.pending_media_frames = list(pending_media_frames)
            state.media_enqueued = False
        await self._enqueue_stream(runtime, state, priority=PRIORITY_STREAM_FINAL if finish else PRIORITY_STREAM_UPDATE)

    async def _enqueue_stream_recovery(self, runtime: BindingRuntime) -> None:
        recoverable = [state for state in runtime.active_streams.values() if not state.final_sent]
        if not recoverable:
            return
        logging.info(
            "WeCom AIBot recovering streams bot_id=%s active_streams=%s",
            runtime.binding["bot_id"],
            len(recoverable),
        )
        for state in recoverable:
            await self._enqueue_stream(runtime, state, priority=PRIORITY_RECOVERY)

    async def _enqueue_stream(self, runtime: BindingRuntime, state: ActiveStreamState, *, priority: int) -> None:
        await self._put_outbound_envelope(
            runtime,
            OutboundEnvelope(
                kind="stream",
                priority=priority,
                sequence=0,
                conversation_key=state.conversation_key,
                stream_id=state.stream_id,
                revision=state.revision,
                force_send=priority == PRIORITY_RECOVERY,
            ),
        )

    async def _enqueue_frame(
        self,
        runtime: BindingRuntime,
        frame: dict[str, Any],
        *,
        conversation_key: str,
        apply_pacing: bool,
        priority: int,
        retryable: bool,
    ) -> None:
        await self._put_outbound_envelope(
            runtime,
            OutboundEnvelope(
                kind="frame",
                priority=priority,
                sequence=0,
                frame=frame,
                conversation_key=conversation_key,
                stream_id=((frame.get("body") or {}).get("stream") or {}).get("id") or "",
                apply_pacing=apply_pacing,
                retryable=retryable,
            ),
        )

    async def _put_outbound_envelope(self, runtime: BindingRuntime, envelope: OutboundEnvelope) -> bool:
        sequence = self._next_sequence(runtime)
        envelope.sequence = sequence
        item = (envelope.priority, sequence, envelope)
        try:
            runtime.outbound_queue.put_nowait(item)
            return True
        except asyncio.QueueFull:
            if self._drop_stale_outbound(runtime, allow_any=envelope.kind == "stop"):
                try:
                    runtime.outbound_queue.put_nowait(item)
                    return True
                except asyncio.QueueFull:
                    pass

        logging.warning(
            "WeCom AIBot outbound queue full bot_id=%s kind=%s conversation_key=%s stream_id=%s",
            runtime.binding["bot_id"],
            envelope.kind,
            envelope.conversation_key,
            envelope.stream_id,
        )
        return False

    @staticmethod
    def _drop_stale_outbound(runtime: BindingRuntime, allow_any: bool = False) -> bool:
        queue = runtime.outbound_queue._queue
        for index, (_, _, envelope) in enumerate(queue):
            if envelope.kind == "stream" and not envelope.force_send and envelope.priority == PRIORITY_STREAM_UPDATE:
                queue.pop(index)
                heapq.heapify(queue)
                return True
        if allow_any and queue:
            queue.pop()
            heapq.heapify(queue)
            return True
        return False

    async def _send_plain_frame(
        self,
        *,
        runtime: BindingRuntime | None,
        send_frame: FrameSender | None,
        frame: dict[str, Any],
        conversation_key: str,
        apply_pacing: bool,
        priority: int,
        retryable: bool,
    ) -> None:
        if runtime is None:
            if apply_pacing:
                await self._send_rate_limited(send_frame, conversation_key, frame)
            else:
                await send_frame(frame)
            return
        await self._enqueue_frame(
            runtime,
            frame,
            conversation_key=conversation_key,
            apply_pacing=apply_pacing,
            priority=priority,
            retryable=retryable,
        )

    def _runtime_media_sender(self, runtime: BindingRuntime | None, conversation_key: str) -> FrameSender | None:
        if runtime is None:
            return None

        async def send_frame(frame: dict) -> None:
            await self._enqueue_frame(
                runtime,
                frame,
                conversation_key=conversation_key,
                apply_pacing=False,
                priority=PRIORITY_MEDIA,
                retryable=False,
            )

        return send_frame

    def _runtime_wait_response(self, runtime: BindingRuntime | None) -> ResponseWaiter | None:
        if runtime is None:
            return None

        async def wait_response(req_id: str) -> dict:
            deadline = time.monotonic() + max(self.config.test_connection_timeout_seconds, 1)
            while True:
                waiter = runtime.wait_response or self.wait_response
                if waiter is not None:
                    try:
                        return await waiter(req_id)
                    except asyncio.CancelledError:
                        if runtime.stopped or self._stop_event.is_set():
                            raise
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise TimeoutError("Timed out waiting for WeCom AIBot response waiter.")
                        try:
                            await asyncio.wait_for(runtime.connection_ready.wait(), timeout=remaining)
                        except TimeoutError:
                            raise TimeoutError("Timed out waiting for WeCom AIBot response waiter.")
                        continue
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Timed out waiting for WeCom AIBot response waiter.")
                await asyncio.sleep(min(remaining, 0.1))

        return wait_response

    def _next_sequence(self, runtime: BindingRuntime) -> int:
        runtime.sequence += 1
        return runtime.sequence

    def _prepare_payload(
        self,
        payload: dict[str, Any],
        *,
        binding_override: dict[str, Any] | None,
        acquire_dedup: bool,
    ) -> PreparedPayload | None:
        self._log_incoming_payload_debug(payload)
        event = extract_event(payload)
        if event:
            binding = binding_override or WeComAIBotBindingService.get_by_bot_id(event.aibotid, include_secret=False)
            if not binding or not binding.get("enabled", True):
                logging.warning("No enabled WeCom AIBot binding found for bot_id=%s", event.aibotid)
                return None
            return PreparedPayload(
                payload=payload,
                binding=binding,
                event=event,
                conversation_key=self._conversation_key(binding, event.chattype, event.chatid, event.userid),
            )

        message = extract_text_message(payload)
        media_message = None if message else extract_media_message(payload)
        self._log_parsed_payload_debug(payload, message, media_message)
        if not message and not media_message:
            return None

        incoming = message or media_message
        binding = binding_override or WeComAIBotBindingService.get_by_bot_id(incoming.aibotid, include_secret=False)
        if not binding or not binding.get("enabled", True):
            logging.warning("No enabled WeCom AIBot binding found for bot_id=%s", incoming.aibotid)
            return None

        if acquire_dedup and incoming.msgid and not self.dedup_store.acquire(incoming.msgid):
            logging.info("Skip duplicated WeCom AIBot message: %s", incoming.msgid)
            return None

        return PreparedPayload(
            payload=payload,
            binding=binding,
            message=message,
            media_message=media_message,
            conversation_key=self._conversation_key(binding, incoming.chattype, incoming.chatid, incoming.userid),
            stream_id=f"wecom-{incoming.msgid or int(time.time() * 1000)}",
        )

    def _conversation_key(self, binding: dict, chattype: str, chatid: str, userid: str) -> str:
        if chattype == "group" and self.config.group_context_mode == "shared":
            raw = f"{binding['agent_id']}:{binding['bot_id']}:group:{chatid}"
        elif chattype == "group":
            raw = f"{binding['agent_id']}:{binding['bot_id']}:group:{chatid}:{userid}"
        else:
            raw = f"{binding['agent_id']}:{binding['bot_id']}:single:{userid}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _log_incoming_payload_debug(self, payload: dict[str, Any]) -> None:
        if not self.config.debug_payload_log:
            return
        headers = payload.get("headers") if isinstance(payload.get("headers"), dict) else {}
        body = payload.get("body") if isinstance(payload.get("body"), dict) else {}
        logging.info(
            "WeCom AIBot inbound payload debug cmd=%s req_id=%s msgid=%s msgtype=%s body_keys=%s raw=%s",
            payload.get("cmd"),
            headers.get("req_id"),
            body.get("msgid"),
            body.get("msgtype") or body.get("type"),
            sorted(body.keys()),
            self._payload_debug_json(payload),
        )

    def _log_parsed_payload_debug(
        self,
        payload: dict[str, Any],
        message: WeComIncomingMessage | None,
        media_message: WeComIncomingMedia | None,
    ) -> None:
        if not self.config.debug_payload_log:
            return
        body = payload.get("body") if isinstance(payload.get("body"), dict) else {}
        incoming = message or media_message
        content = incoming.content if incoming else ""
        logging.info(
            "WeCom AIBot parsed payload debug cmd=%s msgtype=%s parsed=%s content_len=%s content_preview=%s",
            payload.get("cmd"),
            body.get("msgtype") or body.get("type"),
            type(incoming).__name__ if incoming else "None",
            len(content or ""),
            self._truncate_debug_text(content),
        )

    @classmethod
    def _payload_debug_json(cls, payload: dict[str, Any]) -> str:
        return json.dumps(cls._redact_debug_payload(payload), ensure_ascii=False, sort_keys=True)

    @classmethod
    def _redact_debug_payload(cls, value: Any) -> Any:
        if isinstance(value, dict):
            redacted = {}
            for key, item in value.items():
                key_text = str(key).lower()
                if any(token in key_text for token in ("secret", "token", "key", "password", "aeskey")):
                    redacted[key] = "********"
                else:
                    redacted[key] = cls._redact_debug_payload(item)
            return redacted
        if isinstance(value, list):
            return [cls._redact_debug_payload(item) for item in value]
        if isinstance(value, str):
            return cls._truncate_debug_text(value)
        return value

    @staticmethod
    def _truncate_debug_text(value: str, limit: int = 500) -> str:
        value = value or ""
        if len(value) <= limit:
            return value
        return value[:limit] + "...[truncated]"

    async def _send_rate_limited(self, send_frame: FrameSender | None, conversation_key: str, frame: dict) -> None:
        await self._wait_for_conversation_interval(conversation_key)
        await send_frame(frame)

    async def _wait_for_conversation_interval(self, conversation_key: str) -> None:
        interval_ms = self.config.effective_send_interval_ms
        interval_seconds = interval_ms / 1000
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
                    int(interval_ms),
                    max(int(interval_ms) * 4, 1000),
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
        elif not ok and not response_message:
            response_message = f"Subscribe failed with errcode {response_code}."
        return {
            "ok": ok,
            "message": "Subscribe succeeded." if ok else response_message,
            "response_cmd": response_cmd,
            "response_code": response_code,
            "response_message": response_message,
        }

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
