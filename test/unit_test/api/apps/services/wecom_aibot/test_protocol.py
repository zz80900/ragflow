import json

from api.apps.services.wecom_aibot.protocol import (
    build_file_frame,
    build_image_frame,
    build_subscribe_frame,
    build_stream_frame,
    build_welcome_frame,
    extract_event,
    extract_markdown_image_urls,
    extract_media_message,
    extract_sse_content,
    extract_text_message,
    parse_sse_message,
    strip_bot_mention,
)


def test_build_subscribe_frame_includes_req_id_and_secret():
    frame = build_subscribe_frame("bot-1", "secret-1", "req-1")

    assert frame == {
        "cmd": "aibot_subscribe",
        "headers": {"req_id": "req-1"},
        "body": {"bot_id": "bot-1", "secret": "secret-1"},
    }


def test_build_stream_frame_uses_wecom_stream_body_without_msgid():
    frame = build_stream_frame("req-1", "stream-1", "hello", False)

    assert frame == {
        "cmd": "aibot_respond_msg",
        "headers": {"req_id": "req-1"},
        "body": {
            "msgtype": "stream",
            "stream": {"id": "stream-1", "finish": False, "content": "hello"},
        },
    }
    assert "msgid" not in frame["body"]


def test_build_welcome_frame_uses_welcome_command():
    frame = build_welcome_frame("req-1", "hello")

    assert frame == {
        "cmd": "aibot_respond_welcome_msg",
        "headers": {"req_id": "req-1"},
        "body": {
            "msgtype": "markdown",
            "markdown": {"content": "hello"},
        },
    }


def test_build_media_reply_frames_use_wecom_media_id_field():
    assert build_image_frame("req-1", "mid-1")["body"] == {
        "msgtype": "image",
        "image": {"media_id": "mid-1"},
    }
    assert build_file_frame("req-1", "mid-2")["body"] == {
        "msgtype": "file",
        "file": {"media_id": "mid-2"},
    }


def test_extract_text_message_strips_bot_mention():
    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-1"},
        "body": {
            "msgid": "msg-1",
            "aibotid": "bot-1",
            "chatid": "chat-1",
            "chattype": "group",
            "from": {"userid": "u-1"},
            "msgtype": "text",
            "text": {"content": "@RobotA hello"},
        },
    }

    message = extract_text_message(payload)

    assert message is not None
    assert message.req_id == "req-1"
    assert message.msgid == "msg-1"
    assert message.aibotid == "bot-1"
    assert message.userid == "u-1"
    assert message.content == "hello"


def test_strip_bot_mention_handles_chinese_names():
    assert strip_bot_mention("@智能机器人 查询知识库") == "查询知识库"


def test_extract_text_message_handles_markdown_and_fallbacks():
    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-1"},
        "body": {
            "msgid": "msg-1",
            "bot_id": "bot-1",
            "from": {"user_id": "u-1"},
            "msgtype": "markdown",
            "markdown": {"content": "hello"},
        },
    }

    message = extract_text_message(payload)

    assert message is not None
    assert message.aibotid == "bot-1"
    assert message.userid == "u-1"
    assert message.chatid == "u-1"
    assert message.content == "hello"


def test_extract_text_message_ignores_non_text_callbacks():
    assert extract_text_message({"cmd": "aibot_event_callback"}) is None
    assert (
        extract_text_message(
            {
                "cmd": "aibot_msg_callback",
                "body": {"msgtype": "image"},
            }
        )
        is None
    )


def test_extract_media_message_normalizes_image_callback():
    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-1"},
        "body": {
            "msgid": "msg-1",
            "aibotid": "bot-1",
            "chatid": "user-1",
            "chattype": "single",
            "from": {"userid": "user-1"},
            "msgtype": "image",
            "image": {
                "url": "https://example.com/a.png",
                "aeskey": "a" * 32,
                "filename": "a.png",
                "content_type": "image/png",
                "size": "12",
            },
        },
    }

    message = extract_media_message(payload)

    assert message is not None
    assert message.req_id == "req-1"
    assert message.msgid == "msg-1"
    assert message.aibotid == "bot-1"
    assert message.userid == "user-1"
    assert message.msgtype == "image"
    assert message.download_url == "https://example.com/a.png"
    assert message.aeskey == "a" * 32
    assert message.filename == "a.png"
    assert message.content_type == "image/png"
    assert message.size == 12


def test_extract_media_message_normalizes_file_callback_aliases():
    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-1"},
        "body": {
            "msgid": "msg-1",
            "bot_id": "bot-1",
            "from": {"user_id": "user-1"},
            "msgtype": "file",
            "file": {
                "download_url": "https://example.com/a.pdf",
                "file_name": "a.pdf",
                "mime_type": "application/pdf",
                "total_size": 42,
            },
        },
    }

    message = extract_media_message(payload)

    assert message is not None
    assert message.aibotid == "bot-1"
    assert message.userid == "user-1"
    assert message.chatid == "user-1"
    assert message.download_url == "https://example.com/a.pdf"
    assert message.filename == "a.pdf"
    assert message.content_type == "application/pdf"
    assert message.size == 42


def test_extract_event_handles_enter_conversation():
    event = extract_event(
        {
            "cmd": "aibot_event_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "event_type": "enter_conversation",
                "aibotid": "bot-1",
                "from": {"userid": "u-1"},
            },
        }
    )

    assert event is not None
    assert event.req_id == "req-1"
    assert event.aibotid == "bot-1"
    assert event.userid == "u-1"
    assert event.is_enter_conversation is True


def test_extract_event_normalizes_object_event_type():
    event = extract_event(
        {
            "cmd": "aibot_event_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "event_type": {"type": "enter-conversation"},
                "aibotid": "bot-1",
                "from": {"userid": "u-1"},
            },
        }
    )

    assert event is not None
    assert event.event_type == "enter-conversation"
    assert event.is_enter_conversation is True


def test_parse_sse_message_ignores_done_and_parses_json():
    assert parse_sse_message("data:[DONE]\n\n") is None
    payload = {"event": "message", "data": {"content": "hi"}}
    assert parse_sse_message("data:" + json.dumps(payload) + "\n\n") == payload


def test_extract_sse_content_and_markdown_image_urls():
    assert extract_sse_content({"event": "message", "data": {"content": "hi"}}) == "hi"
    assert extract_sse_content({"event": "message", "data": {"start_to_think": True}}) == "<think>"
    assert extract_sse_content({"event": "message", "data": {"end_to_think": True}}) == "</think>"
    assert extract_sse_content({"event": "other", "data": {"content": "hi"}}) == ""
    assert extract_markdown_image_urls("![a](https://example.com/a.png) ![b](http://example.com/b.png)") == [
        "https://example.com/a.png"
    ]
