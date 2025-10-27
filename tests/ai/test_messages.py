from __future__ import annotations

import json

import pytest

import daft
from daft.functions.ai.messages import append_message, build_messages


def test_build_messages_responses_template_text_only():
    df = daft.from_pydict({"id": [1]})
    out = df.select(build_messages(user_text="Hello world").alias("messages")).to_pylist()[0]["messages"]

    messages = json.loads(out)
    assert len(messages) == 1
    assert messages[0]["role"] == "user"
    assert messages[0]["content"] == [{"type": "input_text", "text": "Hello world"}]


def test_build_messages_chat_template_with_history_and_system():
    history = json.dumps([{"role": "assistant", "content": "previous"}])
    df = daft.from_pydict({"id": [1]})
    out = df.select(
        build_messages(
            "openai-chat-completion",
            message_history=history,
            system_message="sys",
            user_text="hi",
        ).alias("messages")
    ).to_pylist()[0]["messages"]

    messages = json.loads(out)
    assert messages[0] == {"role": "assistant", "content": "previous"}
    assert messages[1] == {"role": "system", "content": "sys"}
    assert messages[2]["role"] == "user"
    assert messages[2]["content"] == [{"type": "text", "text": "hi"}]


def test_build_messages_invalid_template_raises():
    with pytest.raises(ValueError, match="Unsupported chat template"):
        build_messages("unsupported-template")


def test_append_message_appends_plain_text():
    history = json.dumps([{"role": "user", "content": "hello"}])
    result = append_message(history, "more text")
    messages = json.loads(result)
    assert len(messages) == 2
    assert messages[1] == {"role": "user", "content": "more text"}


def test_append_message_with_custom_role():
    history = json.dumps([{"role": "user", "content": "hello"}])
    new_message = json.dumps({"role": "assistant", "content": "response"})
    result = append_message(history, new_message, role="assistant")
    messages = json.loads(result)
    assert len(messages) == 2
    assert messages[1] == {"role": "assistant", "content": "response"}
