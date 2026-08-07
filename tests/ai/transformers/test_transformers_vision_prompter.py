from __future__ import annotations

import asyncio
import io
import warnings
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import numpy as np
import pytest

pytest.importorskip("transformers")
pytest.importorskip("PIL")

from PIL import Image as PILImage

from daft.ai.transformers.protocols.vision_prompter import (
    TransformersVisionPrompter,
    TransformersVisionPrompterDescriptor,
)
from daft.file import File


def _make_pipeline(*, chat_template: str | None = "{{messages}}") -> Mock:
    pipe = Mock()
    pipe.processor = Mock()
    pipe.processor.chat_template = chat_template
    pipe.return_value = [{"generated_text": "ok"}]
    return pipe


def _make_prompter(mock_pipeline: Mock, **kwargs: Any) -> TransformersVisionPrompter:
    with patch("daft.ai.transformers.protocols.vision_prompter.pipeline", return_value=mock_pipeline):
        return TransformersVisionPrompter(
            provider_name=kwargs.pop("provider_name", "transformers"),
            model_name=kwargs.pop("model_name", "fake/vlm"),
            system_message=kwargs.pop("system_message", None),
            prompt_options=kwargs.pop("prompt_options", None),
        )


def _make_file(content: bytes | str, mime_type: str) -> Mock:
    """Mock a daft.File with a given MIME and content."""
    file = Mock(spec=File)
    file.mime_type.return_value = mime_type
    handle = MagicMock()
    handle.read.return_value = content
    cm = MagicMock()
    cm.__enter__.return_value = handle
    file.open.return_value = cm
    return file


def _png_bytes() -> bytes:
    buf = io.BytesIO()
    PILImage.new("RGB", (2, 2), color=(255, 0, 0)).save(buf, format="PNG")
    return buf.getvalue()


def test_descriptor_default_fields():
    d = TransformersVisionPrompterDescriptor(provider_name="transformers", model_name="fake/vlm")
    assert d.get_provider() == "transformers"
    assert d.get_model() == "fake/vlm"
    assert d.get_options() == {}
    assert d.return_format is None


def test_descriptor_instantiate_rejects_return_format():
    class Dummy:
        pass

    d = TransformersVisionPrompterDescriptor(
        provider_name="transformers",
        model_name="fake/vlm",
        return_format=Dummy,
    )
    with pytest.raises(NotImplementedError, match=r"return_format"):
        d.instantiate()


_FAKE_MAPPING_PATH = "transformers.models.auto.modeling_auto.MODEL_FOR_IMAGE_TEXT_TO_TEXT_MAPPING_NAMES"


def test_provider_auto_detects_vlm_returns_vision_descriptor():
    from daft.ai.transformers.provider import TransformersProvider

    fake_config = Mock()
    fake_config.model_type = "fakevlm"
    with (
        patch("transformers.AutoConfig.from_pretrained", return_value=fake_config),
        patch(_FAKE_MAPPING_PATH, new={"fakevlm": "FakeVLMForConditionalGeneration"}),
    ):
        descriptor = TransformersProvider().get_prompter(model="any/vlm-name")

    assert isinstance(descriptor, TransformersVisionPrompterDescriptor)


def test_provider_non_vlm_returns_text_descriptor():
    from daft.ai.transformers.protocols.prompter import TransformersPrompterDescriptor
    from daft.ai.transformers.provider import TransformersProvider

    fake_config = Mock()
    fake_config.model_type = "faketext"
    with (
        patch("transformers.AutoConfig.from_pretrained", return_value=fake_config),
        patch(_FAKE_MAPPING_PATH, new={"fakevlm": "FakeVLMForConditionalGeneration"}),
    ):
        descriptor = TransformersProvider().get_prompter(model="any/text-name")

    assert isinstance(descriptor, TransformersPrompterDescriptor)


def test_provider_autoconfig_failure_warns_and_falls_back_to_text():
    from daft.ai.transformers.protocols.prompter import TransformersPrompterDescriptor
    from daft.ai.transformers.provider import TransformersProvider

    with patch("transformers.AutoConfig.from_pretrained", side_effect=OSError("offline")):
        with pytest.warns(UserWarning, match="vision capability"):
            descriptor = TransformersProvider().get_prompter(model="some/unknown-model")

    assert isinstance(descriptor, TransformersPrompterDescriptor)


def test_prompt_str_input_produces_text_block():
    pipe = _make_pipeline()
    prompter = _make_prompter(pipe)

    asyncio.run(prompter.prompt(("hello",)))

    chat = pipe.call_args.kwargs["text"]
    assert chat == [{"role": "user", "content": [{"type": "text", "text": "hello"}]}]
    assert pipe.call_args.kwargs["return_full_text"] is False


def test_prompt_numpy_image_produces_image_block():
    pipe = _make_pipeline()
    prompter = _make_prompter(pipe)
    image_array = np.zeros((4, 4, 3), dtype=np.uint8)

    asyncio.run(prompter.prompt((image_array, "what is it?")))

    content = pipe.call_args.kwargs["text"][0]["content"]
    assert content[0]["type"] == "image"
    assert isinstance(content[0]["image"], PILImage.Image)
    assert content[1] == {"type": "text", "text": "what is it?"}


def test_prompt_pil_image_passes_through():
    pipe = _make_pipeline()
    prompter = _make_prompter(pipe)
    img = PILImage.new("RGB", (2, 2))

    asyncio.run(prompter.prompt((img,)))

    content = pipe.call_args.kwargs["text"][0]["content"]
    assert content == [{"type": "image", "image": img}]


def test_prompt_file_image_opens_pil():
    pipe = _make_pipeline()
    prompter = _make_prompter(pipe)
    image_file = _make_file(_png_bytes(), mime_type="image/png")

    asyncio.run(prompter.prompt((image_file, "describe")))

    content = pipe.call_args.kwargs["text"][0]["content"]
    assert content[0]["type"] == "image"
    assert isinstance(content[0]["image"], PILImage.Image)
    assert content[1] == {"type": "text", "text": "describe"}


def test_prompt_file_text_wraps_with_tag():
    pipe = _make_pipeline()
    prompter = _make_prompter(pipe)
    text_file = _make_file(b"# doc\n\ncontents", mime_type="text/markdown")

    asyncio.run(prompter.prompt((text_file,)))

    content = pipe.call_args.kwargs["text"][0]["content"]
    assert content == [{"type": "text", "text": "<file_text_markdown># doc\n\ncontents</file_text_markdown>"}]


def test_prompt_file_unsupported_mime_raises():
    pipe = _make_pipeline()
    prompter = _make_prompter(pipe)
    file = _make_file(b"...", mime_type="application/pdf")

    with pytest.raises(NotImplementedError, match=r"image/\* or text/\* File inputs"):
        asyncio.run(prompter.prompt((file,)))


def test_prompt_bytes_image_via_mime_sniff():
    pipe = _make_pipeline()
    prompter = _make_prompter(pipe)

    asyncio.run(prompter.prompt((_png_bytes(),)))

    content = pipe.call_args.kwargs["text"][0]["content"]
    assert content[0]["type"] == "image"
    assert isinstance(content[0]["image"], PILImage.Image)


def test_prompt_bytes_plain_text_via_utf8_fallback():
    pipe = _make_pipeline()
    prompter = _make_prompter(pipe)

    asyncio.run(prompter.prompt((b"raw markdown text",)))

    content = pipe.call_args.kwargs["text"][0]["content"]
    assert content == [{"type": "text", "text": "<file_text_plain>raw markdown text</file_text_plain>"}]


def test_prompt_mixed_image_and_text_in_user_content():
    pipe = _make_pipeline()
    prompter = _make_prompter(pipe)
    img = PILImage.new("RGB", (2, 2))

    asyncio.run(prompter.prompt((img, "first", "second")))

    content = pipe.call_args.kwargs["text"][0]["content"]
    assert len(content) == 3
    assert content[0]["type"] == "image"
    assert content[1] == {"type": "text", "text": "first"}
    assert content[2] == {"type": "text", "text": "second"}


def test_prompt_system_message_prepended_as_chat_message():
    pipe = _make_pipeline()
    prompter = _make_prompter(pipe, system_message="be terse")

    asyncio.run(prompter.prompt(("hi",)))

    chat = pipe.call_args.kwargs["text"]
    assert chat[0] == {"role": "system", "content": [{"type": "text", "text": "be terse"}]}
    assert chat[1]["role"] == "user"


def test_prompt_unsupported_input_raises():
    pipe = _make_pipeline()
    prompter = _make_prompter(pipe)

    with pytest.raises(NotImplementedError, match=r"does not support input of type"):
        asyncio.run(prompter.prompt((12345,)))


def test_no_chat_template_warns():
    pipe = _make_pipeline(chat_template=None)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _make_prompter(pipe)
    assert any("no chat template" in str(w.message) for w in caught)
