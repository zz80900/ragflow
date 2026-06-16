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

from dataclasses import dataclass, field
import logging

from api.apps.services.wecom_aibot.protocol import extract_markdown_image_urls, extract_sse_content, parse_sse_message
from api.db.services.api_service import API4ConversationService
from api.db.services.canvas_service import UserCanvasService, completion as agent_completion
from common.misc_utils import thread_pool_exec


@dataclass
class AgentBridgeResult:
    session_id: str | None = None
    content: str = ""
    image_urls: list[str] = field(default_factory=list)


class WeComAgentBridge:
    async def run(
        self,
        tenant_id: str,
        agent_id: str,
        query: str,
        user_id: str,
        session_id: str | None = None,
        release: bool = False,
    ):
        if not await thread_pool_exec(UserCanvasService.accessible, agent_id, tenant_id):
            raise PermissionError("Make sure you have permission to access the agent.")

        if session_id:
            exists, conv = await thread_pool_exec(API4ConversationService.get_by_id, session_id)
            if not exists:
                raise LookupError("Session not found.")
            if conv.dialog_id != agent_id:
                raise PermissionError("Session does not belong to this agent.")

        accumulated = ""
        latest_session_id = session_id
        async for chunk in agent_completion(
            tenant_id=tenant_id,
            agent_id=agent_id,
            session_id=session_id,
            query=query,
            user_id=user_id,
            release="true" if release else "false",
        ):
            try:
                event = parse_sse_message(chunk)
            except Exception:
                logging.exception("Failed to parse agent SSE chunk for WeCom AIBot.")
                continue
            if not event:
                continue
            latest_session_id = event.get("session_id") or latest_session_id
            content = extract_sse_content(event)
            if content:
                accumulated += content
                yield AgentBridgeResult(
                    session_id=latest_session_id,
                    content=accumulated,
                    image_urls=extract_markdown_image_urls(accumulated),
                )
