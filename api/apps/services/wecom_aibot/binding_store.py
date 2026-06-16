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
import time

from peewee import IntegrityError

from api.apps.services.wecom_aibot.crypto import decrypt_secret, encrypt_secret, mask_secret
from api.db.db_models import DB, WeComAIBotBinding
from api.db.services.common_service import CommonService
from common.misc_utils import get_uuid


@dataclass(frozen=True)
class WeComAIBotBindingDTO:
    id: str
    tenant_id: str
    agent_id: str
    bot_id: str
    secret: str
    has_secret: bool
    enabled: bool
    status: str
    last_connected_at: int | None = None
    last_error: str | None = None


class WeComAIBotBindingConflict(ValueError):
    pass


class WeComAIBotBindingService(CommonService):
    model = WeComAIBotBinding

    @classmethod
    def _to_dto(cls, binding: WeComAIBotBinding, include_secret: bool = False) -> dict:
        return {
            "id": binding.id,
            "tenant_id": binding.tenant_id,
            "agent_id": binding.agent_id,
            "bot_id": binding.bot_id,
            "secret": decrypt_secret(binding.secret_ciphertext) if include_secret else mask_secret(binding.secret_ciphertext),
            "has_secret": bool(binding.secret_ciphertext),
            "enabled": binding.enabled,
            "status": binding.status,
            "last_connected_at": binding.last_connected_at,
            "last_error": binding.last_error,
            "created_at": binding.create_time,
            "updated_at": binding.update_time,
        }

    @classmethod
    @DB.connection_context()
    def get_by_agent_id(cls, agent_id: str, include_secret: bool = False) -> dict | None:
        binding = cls.model.get_or_none(cls.model.agent_id == agent_id)
        if not binding:
            return None
        return cls._to_dto(binding, include_secret=include_secret)

    @classmethod
    @DB.connection_context()
    def get_by_bot_id(cls, bot_id: str, include_secret: bool = False) -> dict | None:
        binding = cls.model.get_or_none(cls.model.bot_id == bot_id)
        if not binding:
            return None
        return cls._to_dto(binding, include_secret=include_secret)

    @classmethod
    @DB.connection_context()
    def list_enabled(cls, include_secret: bool = False) -> list[dict]:
        bindings = cls.model.select().where(cls.model.enabled == True)  # noqa: E712
        return [cls._to_dto(binding, include_secret=include_secret) for binding in bindings]

    @classmethod
    @DB.connection_context()
    def upsert(cls, tenant_id: str, agent_id: str, bot_id: str, secret: str | None, enabled: bool) -> dict:
        bot_id = (bot_id or "").strip()
        if not bot_id:
            raise ValueError("BotID is required.")

        existing_by_agent = cls.model.get_or_none(cls.model.agent_id == agent_id)
        existing_by_bot = cls.model.get_or_none(cls.model.bot_id == bot_id)
        if existing_by_bot and (not existing_by_agent or existing_by_bot.id != existing_by_agent.id):
            raise WeComAIBotBindingConflict("BotID has already been bound to another agent.")

        now_status = "enabled" if enabled else "disabled"
        data = {
            "tenant_id": tenant_id,
            "agent_id": agent_id,
            "bot_id": bot_id,
            "enabled": bool(enabled),
            "status": now_status,
            "last_error": None,
        }
        if secret and secret != mask_secret(secret):
            data["secret_ciphertext"] = encrypt_secret(secret)
        elif not existing_by_agent:
            raise ValueError("Secret is required when creating a WeCom AIBot binding.")

        try:
            if existing_by_agent:
                cls.model.update(data).where(cls.model.id == existing_by_agent.id).execute()
                binding = cls.model.get_by_id(existing_by_agent.id)
            else:
                data["id"] = get_uuid()
                binding = cls.model.create(**data)
        except IntegrityError as exc:
            raise WeComAIBotBindingConflict("Agent or BotID has already been bound.") from exc

        return cls._to_dto(binding)

    @classmethod
    @DB.connection_context()
    def delete_by_agent_id(cls, agent_id: str) -> bool:
        return cls.model.delete().where(cls.model.agent_id == agent_id).execute() > 0

    @classmethod
    @DB.connection_context()
    def update_status(cls, bot_id: str, status: str, last_error: str | None = None, connected: bool = False) -> None:
        data: dict = {"status": status, "last_error": last_error}
        if connected:
            data["last_connected_at"] = int(time.time())
        cls.model.update(data).where(cls.model.bot_id == bot_id).execute()
