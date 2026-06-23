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
import json
import re
import uuid
from typing import Any


CMD_SUBSCRIBE = "aibot_subscribe"
CMD_MSG_CALLBACK = "aibot_msg_callback"
CMD_EVENT_CALLBACK = "aibot_event_callback"
CMD_RESPOND_MSG = "aibot_respond_msg"
CMD_RESPOND_WELCOME_MSG = "aibot_respond_welcome_msg"
CMD_SEND_MSG = "aibot_send_msg"
CMD_PING = "ping"


@dataclass(frozen=True)
class WeComIncomingMessage:
    req_id: str
    msgid: str
    aibotid: str
    userid: str
    chattype: str
    chatid: str
    msgtype: str
    content: str
    raw: dict[str, Any]


@dataclass(frozen=True)
class WeComIncomingMedia:
    req_id: str
    msgid: str
    aibotid: str
    userid: str
    chattype: str
    chatid: str
    msgtype: str
    download_url: str
    aeskey: str
    filename: str
    content_type: str
    size: int | None
    content: str
    raw: dict[str, Any]


@dataclass(frozen=True)
class WeComIncomingEvent:
    req_id: str
    event_type: str
    aibotid: str
    userid: str
    chattype: str
    chatid: str
    raw: dict[str, Any]

    @property
    def is_enter_conversation(self) -> bool:
        event_type = _string_value(self.event_type, "event_type", "event", "type", "name").lower().replace("-", "_")
        return event_type in {
            "enter_chat",
            "enter_conversation",
            "open_conversation",
            "user_enter_chat",
            "conversation_enter",
        }


def parse_payload(raw: str | bytes | dict) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw)


def build_subscribe_frame(bot_id: str, secret: str, req_id: str | None = None) -> dict[str, Any]:
    return {
        "cmd": CMD_SUBSCRIBE,
        "headers": {"req_id": req_id or str(uuid.uuid4())},
        "body": {
            "bot_id": bot_id,
            "secret": secret,
        },
    }


def build_ping_frame() -> dict[str, Any]:
    return {"cmd": CMD_PING}


def build_stream_frame(req_id: str, stream_id: str, content: str, finish: bool) -> dict[str, Any]:
    return {
        "cmd": CMD_RESPOND_MSG,
        "headers": {"req_id": req_id},
        "body": {
            "msgtype": "stream",
            "stream": {
                "id": stream_id,
                "finish": finish,
                "content": content,
            },
        },
    }


def build_markdown_frame(req_id: str, content: str) -> dict[str, Any]:
    return {
        "cmd": CMD_RESPOND_MSG,
        "headers": {"req_id": req_id},
        "body": {
            "msgtype": "markdown",
            "markdown": {"content": content},
        },
    }


def build_image_frame(req_id: str, media_id: str) -> dict[str, Any]:
    return {
        "cmd": CMD_RESPOND_MSG,
        "headers": {"req_id": req_id},
        "body": {
            "msgtype": "image",
            "image": {"media_id": media_id},
        },
    }


def build_file_frame(req_id: str, media_id: str) -> dict[str, Any]:
    return {
        "cmd": CMD_RESPOND_MSG,
        "headers": {"req_id": req_id},
        "body": {
            "msgtype": "file",
            "file": {"media_id": media_id},
        },
    }


def build_welcome_frame(req_id: str, content: str) -> dict[str, Any]:
    return {
        "cmd": CMD_RESPOND_WELCOME_MSG,
        "headers": {"req_id": req_id},
        "body": {
            "msgtype": "markdown",
            "markdown": {"content": content},
        },
    }


def extract_text_message(payload: dict[str, Any]) -> WeComIncomingMessage | None:
    if payload.get("cmd") != CMD_MSG_CALLBACK:
        return None

    headers = payload.get("headers") or {}
    body = payload.get("body") or {}
    msgtype = body.get("msgtype") or ""
    content = ""
    if msgtype == "text":
        content = (body.get("text") or {}).get("content") or ""
    elif msgtype == "markdown":
        content = (body.get("markdown") or {}).get("content") or ""
    else:
        return None

    sender = body.get("from") or {}
    userid = sender.get("userid") or sender.get("user_id") or ""
    chatid = body.get("chatid") or userid

    return WeComIncomingMessage(
        req_id=headers.get("req_id") or "",
        msgid=body.get("msgid") or "",
        aibotid=body.get("aibotid") or body.get("bot_id") or "",
        userid=userid,
        chattype=body.get("chattype") or "single",
        chatid=chatid,
        msgtype=msgtype,
        content=strip_bot_mention(content),
        raw=payload,
    )


def extract_media_message(payload: dict[str, Any]) -> WeComIncomingMedia | None:
    if payload.get("cmd") != CMD_MSG_CALLBACK:
        return None

    headers = payload.get("headers") or {}
    body = payload.get("body") or {}
    msgtype = (body.get("msgtype") or body.get("type") or "").lower()
    if msgtype not in {"image", "file"}:
        return None

    media_body = body.get(msgtype) or body.get("media") or {}
    download_url = (
        media_body.get("url")
        or media_body.get("download_url")
        or media_body.get("downloadUrl")
        or media_body.get("media_url")
        or body.get("url")
        or ""
    )
    aeskey = media_body.get("aeskey") or media_body.get("aes_key") or body.get("aeskey") or ""
    if not download_url and not media_body.get("data_base64") and not media_body.get("content_base64"):
        return None

    sender = body.get("from") or {}
    userid = sender.get("userid") or sender.get("user_id") or ""
    chatid = body.get("chatid") or userid
    content = ""
    if isinstance(body.get("text"), dict):
        content = (body.get("text") or {}).get("content") or ""
    elif isinstance(body.get("content"), str):
        content = body.get("content") or ""

    return WeComIncomingMedia(
        req_id=headers.get("req_id") or "",
        msgid=body.get("msgid") or "",
        aibotid=body.get("aibotid") or body.get("bot_id") or "",
        userid=userid,
        chattype=body.get("chattype") or "single",
        chatid=chatid,
        msgtype=msgtype,
        download_url=download_url,
        aeskey=aeskey,
        filename=media_body.get("filename") or media_body.get("file_name") or media_body.get("name") or f"wecom-{msgtype}",
        content_type=media_body.get("content_type") or media_body.get("mime_type") or media_body.get("mimetype") or "",
        size=_safe_int(media_body.get("size") or media_body.get("file_size") or media_body.get("total_size")),
        content=strip_bot_mention(content),
        raw=payload,
    )


def extract_event(payload: dict[str, Any]) -> WeComIncomingEvent | None:
    if payload.get("cmd") != CMD_EVENT_CALLBACK:
        return None

    headers = payload.get("headers") or {}
    body = payload.get("body") or {}
    sender = body.get("from") or {}
    userid = sender.get("userid") or sender.get("user_id") or ""
    event_type = (
        _string_value(body.get("event_type"), "event_type", "event", "type", "name")
        or _string_value(body.get("event"), "event_type", "event", "type", "name")
        or _string_value(body.get("type"), "event_type", "event", "type", "name")
    )

    return WeComIncomingEvent(
        req_id=headers.get("req_id") or "",
        event_type=event_type,
        aibotid=body.get("aibotid") or body.get("bot_id") or "",
        userid=userid,
        chattype=body.get("chattype") or "single",
        chatid=body.get("chatid") or userid,
        raw=payload,
    )


def strip_bot_mention(content: str) -> str:
    content = (content or "").strip()
    content = re.sub(r"^@[\w\u4e00-\u9fff\-_.]+\s*", "", content)
    return content.strip()


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _string_value(value: Any, *nested_keys: str) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in nested_keys:
            nested = _string_value(value.get(key), *nested_keys)
            if nested:
                return nested
    return ""


def dumps_frame(frame: dict[str, Any]) -> str:
    return json.dumps(frame, ensure_ascii=False)


def parse_sse_message(chunk: str) -> dict[str, Any] | None:
    chunk = (chunk or "").strip()
    if not chunk:
        return None
    if chunk.startswith("data:"):
        chunk = chunk[5:].strip()
    if not chunk or chunk == "[DONE]":
        return None
    return json.loads(chunk)


def extract_sse_content(event: dict[str, Any]) -> str:
    if event.get("event") != "message":
        return ""
    data = event.get("data") or {}
    if data.get("start_to_think"):
        return "<think>"
    if data.get("end_to_think"):
        return "</think>"
    return data.get("content") or ""


def extract_markdown_image_urls(content: str) -> list[str]:
    return re.findall(r"!\[[^\]]*\]\((https://[^)\s]+)\)", content or "")
