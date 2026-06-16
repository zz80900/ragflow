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

from dataclasses import dataclass
import hashlib

from rag.utils.redis_conn import REDIS_CONN


@dataclass(frozen=True)
class WeComConversation:
    key: str
    ragflow_session_id: str | None
    ragflow_user_id: str
    query_prefix: str


class WeComAIBotSessionStore:
    def __init__(self, ttl_seconds: int, group_context_mode: str = "shared"):
        self.ttl_seconds = ttl_seconds
        self.group_context_mode = group_context_mode

    def resolve(
        self,
        agent_id: str,
        bot_id: str,
        chattype: str,
        chatid: str,
        userid: str,
    ) -> WeComConversation:
        conversation_key = self._conversation_key(agent_id, bot_id, chattype, chatid, userid)
        session_key = self._redis_key(conversation_key)
        ragflow_session_id = None
        if REDIS_CONN.is_alive():
            ragflow_session_id = REDIS_CONN.get(session_key)
        ragflow_user_id = userid
        query_prefix = ""
        if chattype == "group" and self.group_context_mode == "shared":
            ragflow_user_id = chatid or userid
            query_prefix = f"[来自 {userid}] " if userid else ""
        return WeComConversation(conversation_key, ragflow_session_id, ragflow_user_id, query_prefix)

    def save(self, conversation_key: str, ragflow_session_id: str | None) -> None:
        if ragflow_session_id and REDIS_CONN.is_alive():
            REDIS_CONN.set(self._redis_key(conversation_key), ragflow_session_id, self.ttl_seconds)

    def _conversation_key(self, agent_id: str, bot_id: str, chattype: str, chatid: str, userid: str) -> str:
        if chattype == "group" and self.group_context_mode == "shared":
            raw = f"{agent_id}:{bot_id}:group:{chatid}"
        elif chattype == "group":
            raw = f"{agent_id}:{bot_id}:group:{chatid}:{userid}"
        else:
            raw = f"{agent_id}:{bot_id}:single:{userid}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _redis_key(conversation_key: str) -> str:
        return f"wecom:aibot:session:{conversation_key}"
