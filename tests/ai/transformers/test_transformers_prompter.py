from __future__ import annotations

import asyncio
import warnings
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest

pytest.importorskip("transformers")

from daft.ai.transformers.protocols.prompter import (
    TransformersPrompter,
    TransformersPrompterDescriptor,
)
from daft.file import File


def _make_text_file(content: bytes | str, mime_type: str = "text/plain") -> Mock:
    """Mock a daft.File with a given MIME and content."""
    file = Mock(spec=File)
    file.mime_type.return_value = mime_type
    handle = MagicMock()
    handle.read.return_value = content
    cm = MagicMock()
    cm.__enter__.return_value = handle
    file.open.return_value = cm
    return file


def _make_pipeline(*, chat_template: str | None = "{{messages}}", output: Any | None = None) -> Mock:
    pipe = Mock()
    pipe.tokenizer = Mock()
    pipe.tokenizer.chat_template = chat_template
    pipe.return_value = output if output is not None else [{"generated_text": "ok"}]
    return pipe


def _make_prompter(mock_pipeline: Mock, **kwargs: Any) -> TransformersPrompter:
    with patch("daft.ai.transformers.protocols.prompter.pipeline", return_value=mock_pipeline):
        return TransformersPrompter(
            provider_name=kwargs.pop("provider_name", "transformers"),
            model_name=kwargs.pop("model_name", "fake/model"),
            system_message=kwargs.pop("system_message", None),
            prompt_options=kwargs.pop("prompt_options", None),
        )


def test_descriptor_default_fields():
    d = TransformersPrompterDescriptor(provider_name="transformers", model_name="fake/model")
    assert d.get_provider() == "transformers"
    assert d.get_model() == "fake/model"
    assert d.get_options() == {}
    assert d.return_format is None
    assert d.system_message is None


def test_descriptor_carries_prompt_options():
    d = TransformersPrompterDescriptor(
        provider_name="transformers",
        model_name="fake/model",
        prompt_options={"max_new_tokens": 32, "temperature": 0.5},
    )
    assert d.get_options() == {"max_new_tokens": 32, "temperature": 0.5}


def test_descriptor_get_udf_options_picks_up_udf_keys():
    d = TransformersPrompterDescriptor(
        provider_name="transformers",
        model_name="fake/model",
        prompt_options={"max_retries": 7, "max_new_tokens": 32},
    )
    udf_options = d.get_udf_options()
    assert udf_options.max_retries == 7


def test_descriptor_instantiate_rejects_return_format():
    class Dummy:
        pass

    d = TransformersPrompterDescriptor(
        provider_name="transformers",
        model_name="fake/model",
        return_format=Dummy,
    )
    with pytest.raises(NotImplementedError, match=r"return_format"):
        d.instantiate()


def test_provider_get_prompter_default():
    from daft.ai.transformers.provider import TransformersProvider

    provider = TransformersProvider()
    descriptor = provider.get_prompter()
    assert isinstance(descriptor, TransformersPrompterDescriptor)
    assert descriptor.get_provider() == "transformers"
    assert descriptor.get_model() == TransformersProvider.DEFAULT_PROMPTER


@pytest.mark.filterwarnings("ignore:Could not determine vision capability")
def test_provider_get_prompter_with_overrides():
    from daft.ai.transformers.provider import TransformersProvider

    provider = TransformersProvider()
    descriptor = provider.get_prompter(
        model="custom/model",
        system_message="be brief",
        max_new_tokens=64,
        pipeline_kwargs={"dtype": "auto"},
    )
    assert descriptor.get_model() == "custom/model"
    assert descriptor.system_message == "be brief"
    assert descriptor.get_options() == {
        "max_new_tokens": 64,
        "pipeline_kwargs": {"dtype": "auto"},
    }


def test_prompt_chat_template_basic():
    pipe = _make_pipeline(
        chat_template="...",
        output=[{"generated_text": [{"role": "user", "content": "hi"}, {"role": "assistant", "content": "hello!"}]}],
    )
    prompter = _make_prompter(pipe)

    result = asyncio.run(prompter.prompt(("hi",)))

    assert result == "hello!"
    sent_inputs, sent_kwargs = pipe.call_args.args, pipe.call_args.kwargs
    assert sent_inputs[0] == [{"role": "user", "content": "hi"}]
    assert sent_kwargs.get("return_full_text") is False


def test_prompt_chat_template_with_system_message():
    pipe = _make_pipeline(
        chat_template="...",
        output=[{"generated_text": [{"role": "assistant", "content": "ack"}]}],
    )
    prompter = _make_prompter(pipe, system_message="be terse")

    asyncio.run(prompter.prompt(("ping",)))

    chat = pipe.call_args.args[0]
    assert chat == [
        {"role": "system", "content": "be terse"},
        {"role": "user", "content": "ping"},
    ]


def test_prompt_chat_joins_multiple_text_parts_with_newline():
    pipe = _make_pipeline(
        chat_template="...",
        output=[{"generated_text": [{"role": "assistant", "content": "ack"}]}],
    )
    prompter = _make_prompter(pipe)

    asyncio.run(prompter.prompt(("first", "second", "third")))

    chat = pipe.call_args.args[0]
    assert chat == [{"role": "user", "content": "first\nsecond\nthird"}]


def test_prompt_base_model_returns_string_output():
    pipe = _make_pipeline(chat_template=None, output=[{"generated_text": "world"}])
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        prompter = _make_prompter(pipe, model_name="base/no-template")
    assert any("no chat template" in str(w.message) for w in caught)

    result = asyncio.run(prompter.prompt(("hello",)))

    assert result == "world"
    assert pipe.call_args.args[0] == "hello"


def test_prompt_base_model_prepends_system_message():
    pipe = _make_pipeline(chat_template=None, output=[{"generated_text": "ok"}])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        prompter = _make_prompter(pipe, system_message="be terse")

    asyncio.run(prompter.prompt(("ping",)))

    assert pipe.call_args.args[0] == "be terse\n\nping"


def test_generation_kwargs_forwarded_to_pipeline_call():
    pipe = _make_pipeline(chat_template="...", output=[{"generated_text": [{"role": "assistant", "content": "x"}]}])
    prompter = _make_prompter(
        pipe,
        prompt_options={"max_new_tokens": 20, "temperature": 0.7, "top_p": 0.9},
    )

    asyncio.run(prompter.prompt(("q",)))

    kwargs = pipe.call_args.kwargs
    assert kwargs["max_new_tokens"] == 20
    assert kwargs["temperature"] == 0.7
    assert kwargs["top_p"] == 0.9


def test_udf_keys_stripped_from_generation_kwargs():
    """`max_retries` etc. are UDF-level; they must not leak to pipeline.__call__."""
    pipe = _make_pipeline(chat_template="...", output=[{"generated_text": [{"role": "assistant", "content": "x"}]}])
    prompter = _make_prompter(
        pipe,
        prompt_options={"max_retries": 5, "on_error": "log", "max_new_tokens": 16},
    )

    asyncio.run(prompter.prompt(("q",)))

    kwargs = pipe.call_args.kwargs
    assert "max_retries" not in kwargs
    assert "on_error" not in kwargs
    assert kwargs["max_new_tokens"] == 16


def test_pipeline_kwargs_forwarded_to_pipeline_constructor():
    pipe = _make_pipeline()
    with patch("daft.ai.transformers.protocols.prompter.pipeline", return_value=pipe) as mock_pipeline_ctor:
        TransformersPrompter(
            provider_name="transformers",
            model_name="fake/model",
            prompt_options={"pipeline_kwargs": {"dtype": "auto", "trust_remote_code": True}},
        )

    init_kwargs = mock_pipeline_ctor.call_args.kwargs
    assert init_kwargs["dtype"] == "auto"
    assert init_kwargs["trust_remote_code"] is True
    assert init_kwargs["model"] == "fake/model"


def test_pipeline_kwargs_can_override_device():
    pipe = _make_pipeline()
    with patch("daft.ai.transformers.protocols.prompter.pipeline", return_value=pipe) as mock_pipeline_ctor:
        TransformersPrompter(
            provider_name="transformers",
            model_name="fake/model",
            prompt_options={"pipeline_kwargs": {"device": "cpu"}},
        )
    assert mock_pipeline_ctor.call_args.kwargs["device"] == "cpu"


def test_device_map_in_pipeline_kwargs_skips_default_device():
    """Device and device_map are mutually exclusive in transformers.pipeline."""
    pipe = _make_pipeline()
    with patch("daft.ai.transformers.protocols.prompter.pipeline", return_value=pipe) as mock_pipeline_ctor:
        TransformersPrompter(
            provider_name="transformers",
            model_name="fake/model",
            prompt_options={"pipeline_kwargs": {"device_map": "auto"}},
        )
    init_kwargs = mock_pipeline_ctor.call_args.kwargs
    assert "device" not in init_kwargs
    assert init_kwargs["device_map"] == "auto"


def test_return_full_text_in_options_is_stripped():
    """`return_full_text` is fixed in _sync_prompt; user-supplied value must not collide."""
    pipe = _make_pipeline(chat_template="...", output=[{"generated_text": [{"role": "assistant", "content": "x"}]}])
    prompter = _make_prompter(
        pipe,
        prompt_options={"return_full_text": True, "max_new_tokens": 8},
    )

    asyncio.run(prompter.prompt(("q",)))

    kwargs = pipe.call_args.kwargs
    assert kwargs["return_full_text"] is False
    assert kwargs["max_new_tokens"] == 8


def test_pipeline_kwargs_not_in_generation_kwargs():
    pipe = _make_pipeline(chat_template="...", output=[{"generated_text": [{"role": "assistant", "content": "x"}]}])
    prompter = _make_prompter(
        pipe,
        prompt_options={"pipeline_kwargs": {"dtype": "auto"}, "max_new_tokens": 8},
    )

    asyncio.run(prompter.prompt(("q",)))

    assert "pipeline_kwargs" not in pipe.call_args.kwargs
    assert "dtype" not in pipe.call_args.kwargs


@pytest.mark.parametrize(
    "bad_input",
    [
        12345,
        [1, 2, 3],
    ],
    ids=["int", "list"],
)
def test_prompt_raises_on_unsupported_input_types(bad_input: Any):
    pipe = _make_pipeline(chat_template="...")
    prompter = _make_prompter(pipe)
    with pytest.raises(NotImplementedError, match=r"does not support input of type"):
        asyncio.run(prompter.prompt((bad_input,)))


def test_prompt_accepts_text_file_inlined_with_tag():
    pipe = _make_pipeline(chat_template="...", output=[{"generated_text": [{"role": "assistant", "content": "ok"}]}])
    prompter = _make_prompter(pipe)
    text_file = _make_text_file(b"hello from file", mime_type="text/plain")

    asyncio.run(prompter.prompt((text_file, "summarize")))

    chat = pipe.call_args.args[0]
    user_content = chat[-1]["content"]
    assert "<file_text_plain>hello from file</file_text_plain>" in user_content
    assert "summarize" in user_content


def test_prompt_accepts_text_bytes_via_mime_sniff():
    pipe = _make_pipeline(chat_template="...", output=[{"generated_text": [{"role": "assistant", "content": "ok"}]}])
    prompter = _make_prompter(pipe)
    payload = b"<html><body>hi</body></html>"  # sniffed as text/html

    asyncio.run(prompter.prompt((payload,)))

    user_content = pipe.call_args.args[0][-1]["content"]
    assert "<file_text_html>" in user_content
    assert "hi" in user_content


def test_prompt_accepts_plain_text_bytes_via_utf8_fallback():
    """Raw ASCII / Markdown / JSON aren't recognized by the MIME sniffer; UTF-8 decode fallback should accept them."""
    pipe = _make_pipeline(chat_template="...", output=[{"generated_text": [{"role": "assistant", "content": "ok"}]}])
    prompter = _make_prompter(pipe)

    asyncio.run(prompter.prompt((b"# Title\n\nhello world",)))

    user_content = pipe.call_args.args[0][-1]["content"]
    assert "<file_text_plain># Title\n\nhello world</file_text_plain>" in user_content


def test_prompt_file_with_image_mime_raises():
    pipe = _make_pipeline(chat_template="...")
    prompter = _make_prompter(pipe)
    image_file = _make_text_file(b"\x89PNG\r\n...", mime_type="image/png")

    with pytest.raises(NotImplementedError, match=r"text/\* File inputs"):
        asyncio.run(prompter.prompt((image_file,)))


def test_prompt_bytes_with_binary_mime_raises():
    pipe = _make_pipeline(chat_template="...")
    prompter = _make_prompter(pipe)
    png_bytes = b"\x89PNG\r\n\x1a\n" + b"\x00" * 32  # sniffed as image/png

    with pytest.raises(NotImplementedError, match=r"text/\* bytes inputs"):
        asyncio.run(prompter.prompt((png_bytes,)))


def test_prompt_file_text_replaces_invalid_utf8():
    pipe = _make_pipeline(chat_template="...", output=[{"generated_text": [{"role": "assistant", "content": "ok"}]}])
    prompter = _make_prompter(pipe)
    file = _make_text_file(b"valid \xff\xfe invalid", mime_type="text/plain")

    asyncio.run(prompter.prompt((file,)))

    user_content = pipe.call_args.args[0][-1]["content"]
    assert "valid " in user_content
    assert "invalid" in user_content
