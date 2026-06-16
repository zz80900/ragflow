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

from quart import make_response

from api.apps import login_required
from api.apps.services.wecom_aibot.binding_store import WeComAIBotBindingConflict, WeComAIBotBindingService
from api.apps.services.wecom_aibot.config import WeComAIBotConfig
from api.apps.services.wecom_aibot.media_public import MediaPublicFailure, load_signed_public_media
from api.apps.services.wecom_aibot.service import WeComAIBotService
from api.db.services.canvas_service import UserCanvasService
from api.utils.api_utils import add_tenant_id_to_kwargs, get_data_error_result, get_json_result, get_request_json
from common.constants import RetCode
from common import settings
from common.misc_utils import thread_pool_exec


async def _require_agent_access(agent_id: str, tenant_id: str):
    if not await thread_pool_exec(UserCanvasService.accessible, agent_id, tenant_id):
        return get_data_error_result(message="Make sure you have permission to access the agent.", code=RetCode.OPERATING_ERROR)
    return None


async def _require_agent_owner(agent_id: str, tenant_id: str):
    owned = await thread_pool_exec(UserCanvasService.query, user_id=tenant_id, id=agent_id)
    if not owned:
        return get_data_error_result(message="Only the owner of the agent is authorized for this operation.", code=RetCode.OPERATING_ERROR)
    return None


@manager.route("/agents/<agent_id>/wecom", methods=["GET"])  # noqa: F821
@manager.route("/agents/<agent_id>/wecom/status", methods=["GET"])  # noqa: F821
@login_required
@add_tenant_id_to_kwargs
async def get_agent_wecom_status(agent_id, tenant_id):
    error = await _require_agent_access(agent_id, tenant_id)
    if error:
        return error
    binding = await thread_pool_exec(WeComAIBotBindingService.get_by_agent_id, agent_id)
    if not binding:
        binding = {
            "agent_id": agent_id,
            "bot_id": "",
            "secret": "",
            "has_secret": False,
            "enabled": False,
            "status": "unbound",
            "last_connected_at": None,
            "last_error": None,
        }
    return get_json_result(data=binding)


@manager.route("/agents/<agent_id>/wecom", methods=["PUT"])  # noqa: F821
@login_required
@add_tenant_id_to_kwargs
async def save_agent_wecom(agent_id, tenant_id):
    error = await _require_agent_owner(agent_id, tenant_id)
    if error:
        return error
    req = await get_request_json()
    try:
        binding = await thread_pool_exec(
            WeComAIBotBindingService.upsert,
            tenant_id,
            agent_id,
            req.get("bot_id") or req.get("botId") or "",
            req.get("secret"),
            bool(req.get("enabled")),
        )
    except WeComAIBotBindingConflict as exc:
        return get_data_error_result(message=str(exc), code=RetCode.OPERATING_ERROR)
    except ValueError as exc:
        return get_data_error_result(message=str(exc), code=RetCode.ARGUMENT_ERROR)
    return get_json_result(data=binding)


@manager.route("/agents/<agent_id>/wecom/test-connection", methods=["POST"])  # noqa: F821
@manager.route("/agents/<agent_id>/wecom/test", methods=["POST"])  # noqa: F821
@login_required
@add_tenant_id_to_kwargs
async def test_agent_wecom_connection(agent_id, tenant_id):
    error = await _require_agent_owner(agent_id, tenant_id)
    if error:
        return error
    binding = await thread_pool_exec(WeComAIBotBindingService.get_by_agent_id, agent_id, True)
    if not binding:
        return get_data_error_result(message="WeCom AIBot binding not found.", code=RetCode.DATA_ERROR)
    result = await WeComAIBotService().test_connection(binding)
    return get_json_result(data=result)


@manager.route("/agents/<agent_id>/wecom/test-message", methods=["POST"])  # noqa: F821
@login_required
@add_tenant_id_to_kwargs
async def test_agent_wecom_message(agent_id, tenant_id):
    error = await _require_agent_owner(agent_id, tenant_id)
    if error:
        return error
    binding = await thread_pool_exec(WeComAIBotBindingService.get_by_agent_id, agent_id, True)
    if not binding:
        return get_data_error_result(message="WeCom AIBot binding not found.", code=RetCode.DATA_ERROR)
    req = await get_request_json()
    content = req.get("content") or req.get("query") or req.get("message") or ""
    media = req.get("media") or req.get("media_fixture") or req.get("mediaFixture")
    if not content and not media:
        return get_data_error_result(message="content is required.", code=RetCode.ARGUMENT_ERROR)
    service = WeComAIBotService()
    if media:
        result = await service.simulate_media_message(
            binding=binding,
            userid=req.get("userid") or "debug-user",
            chatid=req.get("chatid") or req.get("userid") or "debug-chat",
            chattype=req.get("chattype") or "single",
            content=content,
            media=media,
        )
    else:
        result = await service.simulate_text_message(
            binding=binding,
            userid=req.get("userid") or "debug-user",
            chatid=req.get("chatid") or req.get("userid") or "debug-chat",
            chattype=req.get("chattype") or "single",
            content=content,
        )
    return get_json_result(data=result)


@manager.route("/agents/wecom/media/<token>", methods=["GET"])  # noqa: F821
async def get_wecom_public_media(token):
    config = WeComAIBotConfig.from_env()
    try:
        data, content_type = await thread_pool_exec(
            load_signed_public_media,
            settings.STORAGE_IMPL,
            token,
            secret=config.media_public_token_secret,
        )
    except MediaPublicFailure as exc:
        return get_data_error_result(message=exc.reason, code=RetCode.DATA_ERROR)

    response = await make_response(data)
    response.headers.set("Content-Type", content_type)
    response.headers.set("Cache-Control", "private, max-age=60")
    return response


@manager.route("/agents/<agent_id>/wecom", methods=["DELETE"])  # noqa: F821
@login_required
@add_tenant_id_to_kwargs
async def delete_agent_wecom(agent_id, tenant_id):
    error = await _require_agent_owner(agent_id, tenant_id)
    if error:
        return error
    deleted = await thread_pool_exec(WeComAIBotBindingService.delete_by_agent_id, agent_id)
    return get_json_result(data={"deleted": deleted})
