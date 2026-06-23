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
from types import SimpleNamespace

import pytest

from api.db.joint_services import tenant_model_service
from common.constants import ActiveStatusEnum, LLMType


def _mock_openai_compatible_provider(monkeypatch):
    provider = SimpleNamespace(id="provider-1", provider_name="OpenAI-API-Compatible")
    instance = SimpleNamespace(
        id="instance-1",
        instance_name="prod",
        api_key='{"api_key": "sk-test", "is_tools": true}',
        extra='{"base_url": "https://example.test"}',
        status=ActiveStatusEnum.ACTIVE.value,
    )
    model = SimpleNamespace(
        model_name="qwen-embed",
        model_type=LLMType.EMBEDDING.value,
        status=ActiveStatusEnum.ACTIVE.value,
        extra='{"max_tokens": 2048}',
    )

    monkeypatch.setattr(
        tenant_model_service.TenantModelProviderService,
        "get_by_tenant_id_and_provider_name",
        lambda tenant_id, provider_name: provider if provider_name == provider.provider_name else None,
    )
    monkeypatch.setattr(
        tenant_model_service.TenantModelInstanceService,
        "get_by_provider_id_and_instance_name",
        lambda provider_id, instance_name: None,
    )
    monkeypatch.setattr(
        tenant_model_service.TenantModelInstanceService,
        "get_all_by_provider_id",
        lambda provider_id: [instance],
    )

    def get_model(provider_id, instance_id, model_type, model_name):
        if (
            provider_id == provider.id
            and instance_id == instance.id
            and model_type == model.model_type
            and model_name == model.model_name
        ):
            return model
        return None

    monkeypatch.setattr(
        tenant_model_service.TenantModelService,
        "get_by_provider_id_and_instance_id_and_model_type_and_model_name",
        get_model,
    )
    return provider, instance, model


def test_get_model_config_resolves_legacy_openai_compatible_instance(monkeypatch):
    _mock_openai_compatible_provider(monkeypatch)

    config = tenant_model_service.get_model_config_from_provider_instance(
        "tenant-1",
        LLMType.EMBEDDING,
        "qwen-embed___OpenAI-API@OpenAI-API-Compatible",
    )

    assert config["llm_factory"] == "OpenAI-API-Compatible"
    assert config["api_key"] == "sk-test"
    assert config["llm_name"] == "qwen-embed"
    assert config["api_base"] == "https://example.test"
    assert config["model_type"] == LLMType.EMBEDDING.value
    assert config["is_tools"] is True
    assert config["max_tokens"] == 2048


def test_get_model_config_resolves_legacy_default_instance(monkeypatch):
    _mock_openai_compatible_provider(monkeypatch)

    config = tenant_model_service.get_model_config_from_provider_instance(
        "tenant-1",
        LLMType.EMBEDDING,
        "qwen-embed___OpenAI-API@default@OpenAI-API-Compatible",
    )

    assert config["llm_factory"] == "OpenAI-API-Compatible"
    assert config["llm_name"] == "qwen-embed"
    assert config["api_base"] == "https://example.test"


def test_get_model_config_does_not_fallback_for_explicit_instance(monkeypatch):
    _mock_openai_compatible_provider(monkeypatch)

    with pytest.raises(LookupError, match="Instance missing not found"):
        tenant_model_service.get_model_config_from_provider_instance(
            "tenant-1",
            LLMType.EMBEDDING,
            "qwen-embed___OpenAI-API@missing@OpenAI-API-Compatible",
        )
