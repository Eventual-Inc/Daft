from __future__ import annotations

import asyncio
import warnings
from typing import Any
from unittest.mock import Mock, patch

import pytest

pytest.importorskip("transformers")

from daft.ai.protocols import Prompter
from daft.ai.transformers.protocols.prompter import (
    TransformersPrompter,
    TransformersPrompterDescriptor,
)


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
        b"raw bytes",
        12345,
        [1, 2, 3],
    ],
    ids=["bytes", "int", "list"],
)
def test_prompt_raises_on_unsupported_input_types(bad_input: Any):
    pipe = _make_pipeline(chat_template="...")
    prompter = _make_prompter(pipe)
    with pytest.raises(NotImplementedError, match=r"only supports str inputs"):
        asyncio.run(prompter.prompt((bad_input,)))


def test_prompt_raises_on_numpy_input():
    np = pytest.importorskip("numpy")
    pipe = _make_pipeline(chat_template="...")
    prompter = _make_prompter(pipe)
    with pytest.raises(NotImplementedError, match=r"only supports str inputs"):
        asyncio.run(prompter.prompt((np.zeros((4, 4), dtype=np.uint8),)))


def test_protocol_compliance():
    pipe = _make_pipeline(chat_template="...")
    prompter = _make_prompter(pipe)
    assert isinstance(prompter, Prompter)
    assert callable(prompter.prompt)


def test_provider_name_preserved():
    pipe = _make_pipeline(chat_template="...", output=[{"generated_text": [{"role": "assistant", "content": "x"}]}])
    prompter = _make_prompter(pipe, provider_name="my-custom-name")
    assert prompter.provider_name == "my-custom-name"
