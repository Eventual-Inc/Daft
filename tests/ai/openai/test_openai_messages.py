from __future__ import annotations

import json

import pytest

import daft
from daft.ai.openai.messages import (
    build_chat_completion_messages,
    build_responses_messages,
    file2dataurl,
    is_http_or_data_url,
    prepare_file_or_audio_for_openai,
    prepare_image_for_openai,
    safe_json_loads_messages,
)
from daft.functions import file as file_expr


def test_safe_json_loads_messages_json_list():
    messages_json = '[{"role": "user", "content": "hi"}]'
    res = safe_json_loads_messages(messages_json)
    assert isinstance(res, list)
    assert res == [{"role": "user", "content": "hi"}]


def test_safe_json_loads_messages_json_string():
    messages_json = '"hello world"'
    res = safe_json_loads_messages(messages_json)
    assert res == [{"role": "user", "content": "hello world"}]


def test_safe_json_loads_messages_json_dict():
    messages_json = '{"role": "assistant", "content": "ok"}'
    res = safe_json_loads_messages(messages_json)
    assert res == [{"role": "assistant", "content": "ok"}]


def test_safe_json_loads_messages_plain_string():
    res = safe_json_loads_messages("plain text")
    assert res == [{"role": "user", "content": "plain text"}]


def test_safe_json_loads_messages_invalid_json_raises():
    with pytest.raises(ValueError):
        safe_json_loads_messages("{invalid")


def test_build_chat_completion_messages_text_only():
    df = daft.from_pydict({"x": [1]})
    out = df.select(build_chat_completion_messages(system_message="sys", user_text="hi").alias("messages")).to_pylist()[
        0
    ]["messages"]

    messages = json.loads(out)
    assert messages[0] == {"role": "system", "content": "sys"}
    assert messages[1]["role"] == "user"
    assert messages[1]["content"] == [{"type": "text", "text": "hi"}]


def test_build_chat_completion_messages_multimodal_and_history():
    history = json.dumps([{"role": "assistant", "content": "prev"}])
    df = daft.from_pydict({"x": [1]})
    out = df.select(
        build_chat_completion_messages(
            message_history=history,
            system_message="sys",
            user_text="hi",
            user_image="https://img",
            user_audio="https://audio",
        ).alias("messages")
    ).to_pylist()[0]["messages"]

    messages = json.loads(out)
    assert messages[0] == {"role": "assistant", "content": "prev"}
    assert messages[1] == {"role": "system", "content": "sys"}
    user = messages[2]
    assert user["role"] == "user"
    content = user["content"]
    types = [c["type"] for c in content]
    assert types == ["text", "image_url", "audio_url"]


def test_build_chat_completion_messages_file_unsupported():
    df = daft.from_pydict({"x": [1]})
    with pytest.raises(ValueError, match="File inputs are not supported"):
        df.select(
            build_chat_completion_messages(
                user_file={"filename": "f", "url": "data:application/octet-stream;base64,AA"}
            )
        ).to_pylist()


def test_build_responses_messages_text_image_file():
    df = daft.from_pydict({"x": [1]})
    out = df.select(
        build_responses_messages(
            history=json.dumps([{"role": "assistant", "content": "prev"}]),
            system_message="sys",
            user_text="hi",
            user_image="data:image/png;base64,AAA",
            user_file={"filename": "f.bin", "url": "data:application/octet-stream;base64,AA"},
        ).alias("messages")
    ).to_pylist()[0]["messages"]

    messages = json.loads(out)
    assert messages[0] == {"role": "assistant", "content": "prev"}
    assert messages[1] == {"role": "system", "content": "sys"}
    user = messages[2]
    content = user["content"]
    types = [c["type"] for c in content]
    assert types == ["input_text", "input_image", "input_file"]
    assert content[2]["filename"] == "f.bin"
    assert content[2]["file_data"].startswith("data:")


def test_build_responses_messages_audio_unsupported():
    df = daft.from_pydict({"x": [1]})
    with pytest.raises(ValueError, match="Audio input is not supported"):
        df.select(
            build_responses_messages(user_audio={"filename": "a.wav", "url": "data:audio/wav;base64,AA"})
        ).to_pylist()


def test_file2dataurl(tmp_path):
    temp_file = tmp_path / "test_file.txt"
    temp_file.write_text("hello")

    df = daft.from_pydict({"path": [str(temp_file.absolute())]})
    df = df.select(file_expr(df["path"]).alias("f"))
    out = df.select(file2dataurl(df["f"]).alias("d")).to_pylist()[0]["d"]
    assert set(out.keys()) == {"filename", "url"}
    assert out["filename"].endswith("test_file.txt")
    assert out["url"].startswith("data:text/plain;base64,")


def test_prepare_file_or_audio_for_openai_passthrough_http_and_data():
    df1 = daft.from_pydict({"x": ["https://example.com/file.bin"]})
    out1 = df1.select(prepare_file_or_audio_for_openai(df1["x"]).alias("y")).to_pylist()[0]["y"]
    assert out1 == "https://example.com/file.bin"

    df2 = daft.from_pydict({"x": ["data:application/octet-stream;base64,AA"]})
    out2 = df2.select(prepare_file_or_audio_for_openai(df2["x"]).alias("y")).to_pylist()[0]["y"]
    assert out2.startswith("data:application/octet-stream;base64,")


def test_prepare_file_or_audio_for_openai_file_to_dataurl(tmp_path):
    temp_file = tmp_path / "test_file.bin"
    temp_file.write_bytes(b"\x00\x01")

    df = daft.from_pydict({"path": [str(temp_file.absolute())]})
    df = df.select(file_expr(df["path"]).alias("f"))
    out = df.select(prepare_file_or_audio_for_openai(df["f"]).alias("y")).to_pylist()[0]["y"]
    assert set(out.keys()) == {"filename", "url"}
    assert out["url"].startswith("data:application/octet-stream;base64,")


def test_is_http_or_data_url():
    df = daft.from_pydict(
        {
            "u": [
                "https://example.com/x",
                "data:image/png;base64,AAAA",
                "not-a-url",
            ]
        }
    )
    out = df.select(is_http_or_data_url(df["u"]).alias("b")).to_pylist()
    assert [row["b"] for row in out] == [True, True, False]


def test_prepare_image_for_openai_passthrough():
    df1 = daft.from_pydict({"x": ["https://example.com/img.png"]})
    out1 = df1.select(prepare_image_for_openai(df1["x"]).alias("y")).to_pylist()[0]["y"]
    assert out1 == "https://example.com/img.png"

    df2 = daft.from_pydict({"x": ["data:image/png;base64,AAA="]})
    out2 = df2.select(prepare_image_for_openai(df2["x"]).alias("y")).to_pylist()[0]["y"]
    assert out2.startswith("data:image/png;base64,")
