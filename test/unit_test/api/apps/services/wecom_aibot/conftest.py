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

import sys
import types
from dataclasses import dataclass, field
from pathlib import Path


ROOT = Path(__file__).resolve().parents[6]
API_APPS_PATH = ROOT / "api" / "apps"


if "api.apps" not in sys.modules:
    api_apps = types.ModuleType("api.apps")
    api_apps.__path__ = [str(API_APPS_PATH)]
    sys.modules["api.apps"] = api_apps


class _RedisConn:
    REDIS = None

    @staticmethod
    def is_alive():
        return False

    @staticmethod
    def get(key):
        return None

    @staticmethod
    def set(key, value, ttl):
        return None

    @staticmethod
    def delete_if_equal(key, value):
        return None


redis_conn = types.ModuleType("rag.utils.redis_conn")
redis_conn.REDIS_CONN = _RedisConn()
sys.modules.setdefault("rag.utils.redis_conn", redis_conn)


@dataclass
class AgentBridgeResult:
    session_id: str | None = None
    content: str = ""
    image_urls: list[str] = field(default_factory=list)


class WeComAgentBridge:
    async def run(self, **kwargs):
        return
        yield


agent_bridge = types.ModuleType("api.apps.services.wecom_aibot.agent_bridge")
agent_bridge.AgentBridgeResult = AgentBridgeResult
agent_bridge.WeComAgentBridge = WeComAgentBridge
sys.modules.setdefault("api.apps.services.wecom_aibot.agent_bridge", agent_bridge)


class WeComAIBotBindingService:
    @staticmethod
    def get_by_bot_id(bot_id, include_secret=False):
        return None

    @staticmethod
    def list_enabled(include_secret=False):
        return []

    @staticmethod
    def update_status(*args, **kwargs):
        return None


binding_store = types.ModuleType("api.apps.services.wecom_aibot.binding_store")
binding_store.WeComAIBotBindingService = WeComAIBotBindingService
sys.modules.setdefault("api.apps.services.wecom_aibot.binding_store", binding_store)
