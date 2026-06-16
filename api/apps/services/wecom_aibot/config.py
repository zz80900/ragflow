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
import os


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_list(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.environ.get(name)
    if raw is None:
        return default
    values = tuple(value.strip() for value in raw.split(",") if value.strip())
    return values or default


@dataclass(frozen=True)
class WeComAIBotConfig:
    enabled: bool = False
    ws_url: str = "wss://openws.work.weixin.qq.com"
    stream_interval_ms: int = 2000
    heartbeat_seconds: int = 30
    session_ttl_seconds: int = 2592000
    dedup_ttl_seconds: int = 86400
    lock_ttl_seconds: int = 60
    conversation_interval_ms: int = 2000
    max_stream_seconds: int = 600
    reconnect_initial_seconds: int = 1
    reconnect_max_seconds: int = 30
    test_connection_timeout_seconds: int = 10
    binding_refresh_seconds: int = 30
    group_context_mode: str = "shared"
    welcome_message: str = "Hello, I am the assistant."
    media_public_base_url: str = ""
    media_public_url_ttl_seconds: int = 300
    media_max_download_bytes: int = 20 * 1024 * 1024
    media_download_timeout_seconds: int = 10
    media_allowed_types: tuple[str, ...] = (
        "image/png",
        "image/jpeg",
        "image/gif",
        "image/webp",
        "application/pdf",
        "text/plain",
        "application/msword",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-powerpoint",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/zip",
        "application/octet-stream",
    )
    media_reply_mode: str = "auto"
    media_temp_cache_seconds: int = 259200
    media_public_token_secret: str = ""

    @classmethod
    def from_env(cls) -> "WeComAIBotConfig":
        return cls(
            enabled=_env_bool("WECOM_AIBOT_ENABLED", False),
            ws_url=os.environ.get("WECOM_AIBOT_WS_URL", cls.ws_url),
            stream_interval_ms=_env_int("WECOM_AIBOT_STREAM_INTERVAL_MS", cls.stream_interval_ms),
            heartbeat_seconds=_env_int("WECOM_AIBOT_HEARTBEAT_SECONDS", cls.heartbeat_seconds),
            session_ttl_seconds=_env_int("WECOM_AIBOT_SESSION_TTL_SECONDS", cls.session_ttl_seconds),
            dedup_ttl_seconds=_env_int("WECOM_AIBOT_DEDUP_TTL_SECONDS", cls.dedup_ttl_seconds),
            lock_ttl_seconds=_env_int("WECOM_AIBOT_LOCK_TTL_SECONDS", cls.lock_ttl_seconds),
            conversation_interval_ms=_env_int("WECOM_AIBOT_CONVERSATION_INTERVAL_MS", cls.conversation_interval_ms),
            max_stream_seconds=_env_int("WECOM_AIBOT_MAX_STREAM_SECONDS", cls.max_stream_seconds),
            reconnect_initial_seconds=_env_int("WECOM_AIBOT_RECONNECT_INITIAL_SECONDS", cls.reconnect_initial_seconds),
            reconnect_max_seconds=_env_int("WECOM_AIBOT_RECONNECT_MAX_SECONDS", cls.reconnect_max_seconds),
            test_connection_timeout_seconds=_env_int("WECOM_AIBOT_TEST_CONNECTION_TIMEOUT_SECONDS", cls.test_connection_timeout_seconds),
            binding_refresh_seconds=_env_int("WECOM_AIBOT_BINDING_REFRESH_SECONDS", cls.binding_refresh_seconds),
            group_context_mode=os.environ.get("WECOM_AIBOT_GROUP_CONTEXT_MODE", cls.group_context_mode),
            welcome_message=os.environ.get("WECOM_AIBOT_WELCOME_MESSAGE", cls.welcome_message),
            media_public_base_url=os.environ.get("WECOM_AIBOT_PUBLIC_BASE_URL", cls.media_public_base_url).rstrip("/"),
            media_public_url_ttl_seconds=_env_int("WECOM_AIBOT_MEDIA_PUBLIC_URL_TTL_SECONDS", cls.media_public_url_ttl_seconds),
            media_max_download_bytes=_env_int("WECOM_AIBOT_MEDIA_MAX_DOWNLOAD_BYTES", cls.media_max_download_bytes),
            media_download_timeout_seconds=_env_int("WECOM_AIBOT_MEDIA_DOWNLOAD_TIMEOUT_SECONDS", cls.media_download_timeout_seconds),
            media_allowed_types=_env_list("WECOM_AIBOT_MEDIA_ALLOWED_TYPES", cls.media_allowed_types),
            media_reply_mode=os.environ.get("WECOM_AIBOT_MEDIA_REPLY_MODE", cls.media_reply_mode),
            media_temp_cache_seconds=_env_int("WECOM_AIBOT_MEDIA_TEMP_CACHE_SECONDS", cls.media_temp_cache_seconds),
            media_public_token_secret=(
                os.environ.get("WECOM_AIBOT_MEDIA_PUBLIC_TOKEN_SECRET")
                or os.environ.get("RAGFLOW_SECRET_KEY")
                or os.environ.get("SECRET_KEY")
                or cls.media_public_token_secret
            ),
        )
