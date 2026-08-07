from __future__ import annotations

import asyncio
import io
import warnings
from dataclasses import dataclass, field
from functools import singledispatchmethod
from typing import TYPE_CHECKING, Any

from transformers import pipeline

from daft.ai.protocols import Prompter, PrompterDescriptor
from daft.ai.transformers.protocols import model_loading_lock
from daft.ai.typing import Options, PromptOptions, UDFOptions
from daft.ai.utils import get_gpu_udf_options, get_torch_device
from daft.daft import guess_mimetype_from_content
from daft.dependencies import np, pil_image
from daft.file import File

if TYPE_CHECKING:
    from pydantic import BaseModel


class TransformersVisionPromptOptions(PromptOptions, total=False):
    """Options for the Transformers vision prompter.

    Attributes:
        pipeline_kwargs (dict): Forwarded to the ``transformers.pipeline``
            constructor (e.g. ``dtype``, ``device_map``, ``trust_remote_code``).

    Note:
        Any other kwargs (``temperature``, ``max_new_tokens``, ``top_p``, ...)
        are forwarded to the pipeline call.
    """

    pipeline_kwargs: dict[str, Any]


@dataclass
class TransformersVisionPrompterDescriptor(PrompterDescriptor):
    provider_name: str
    model_name: str
    prompt_options: TransformersVisionPromptOptions = field(default_factory=lambda: TransformersVisionPromptOptions())
    system_message: str | None = None
    return_format: BaseModel | None = None

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return dict(self.prompt_options)

    def get_udf_options(self) -> UDFOptions:
        udf_options = get_gpu_udf_options()
        for key, value in self.prompt_options.items():
            if key in udf_options.__annotations__.keys():
                setattr(udf_options, key, value)
        return udf_options

    def instantiate(self) -> Prompter:
        if self.return_format is not None:
            raise NotImplementedError("return_format is not yet supported for the 'transformers' provider.")
        return TransformersVisionPrompter(
            provider_name=self.provider_name,
            model_name=self.model_name,
            system_message=self.system_message,
            prompt_options=self.prompt_options,
        )


class TransformersVisionPrompter(Prompter):
    """Pipeline based image-text-to-text generation."""

    def __init__(
        self,
        provider_name: str,
        model_name: str,
        system_message: str | None = None,
        prompt_options: TransformersVisionPromptOptions | None = None,
    ) -> None:
        self.provider_name = provider_name
        self.model = model_name
        self.system_message = system_message

        opts: dict[str, Any] = dict(prompt_options or {})
        pipeline_kwargs: dict[str, Any] = opts.pop("pipeline_kwargs", {})
        opts.pop("return_full_text", None)  # always set to False in _sync_prompt
        for udf_only_key in UDFOptions.__annotations__:
            opts.pop(udf_only_key, None)
        self.generation_kwargs: dict[str, Any] = opts

        pipeline_init: dict[str, Any] = dict(pipeline_kwargs)
        if "device" not in pipeline_init and "device_map" not in pipeline_init:
            pipeline_init["device"] = get_torch_device()
        with model_loading_lock:
            self._pipeline = pipeline(task="image-text-to-text", model=model_name, **pipeline_init)

        # Vision chat template lives on the processor, not the tokenizer.
        self._has_chat_template = getattr(self._pipeline.processor, "chat_template", None) is not None
        if not self._has_chat_template:
            warnings.warn(
                f"Model '{model_name}' has no chat template; multimodal prompting requires an "
                "instruction-tuned vision-language model.",
                stacklevel=2,
            )

    @singledispatchmethod
    def _process_message(self, msg: Any) -> dict[str, Any]:
        raise NotImplementedError(
            f"The 'transformers' vision prompter does not support input of type {type(msg).__name__}."
        )

    @_process_message.register
    def _(self, msg: str) -> dict[str, Any]:
        return {"type": "text", "text": msg}

    @_process_message.register
    def _(self, msg: np.ndarray) -> dict[str, Any]:  # type: ignore[type-arg,unused-ignore]
        return {"type": "image", "image": pil_image.fromarray(msg)}

    @_process_message.register
    def _(self, msg: File) -> dict[str, Any]:
        mime_type = msg.mime_type()
        if mime_type.startswith("image/"):
            with msg.open() as f:
                return {"type": "image", "image": pil_image.open(io.BytesIO(f.read()))}
        if self._is_text_mime_type(mime_type):
            return {"type": "text", "text": self._wrap_filetag(mime_type, self._read_text_content(msg))}
        raise NotImplementedError(
            f"The 'transformers' vision prompter only supports image/* or text/* File inputs, got '{mime_type}'."
        )

    @_process_message.register
    def _(self, msg: bytes) -> dict[str, Any]:
        mime_type = guess_mimetype_from_content(msg)
        if mime_type and mime_type.startswith("image/"):
            return {"type": "image", "image": pil_image.open(io.BytesIO(msg))}
        if mime_type is None:
            try:
                return {"type": "text", "text": self._wrap_filetag("text/plain", msg.decode("utf-8"))}
            except UnicodeDecodeError:
                raise NotImplementedError(
                    "The 'transformers' vision prompter only supports image/* or text/* bytes inputs."
                )
        if self._is_text_mime_type(mime_type):
            return {"type": "text", "text": self._wrap_filetag(mime_type, msg.decode("utf-8", errors="replace"))}
        raise NotImplementedError(
            f"The 'transformers' vision prompter only supports image/* or text/* bytes inputs, got '{mime_type}'."
        )

    @staticmethod
    def _is_text_mime_type(mime_type: str) -> bool:
        return mime_type.split(";")[0].strip().lower().startswith("text/")

    @staticmethod
    def _read_text_content(file_obj: File) -> str:
        with file_obj.open() as f:
            data = f.read()
        if isinstance(data, str):
            return data
        return data.decode("utf-8", errors="replace")

    @staticmethod
    def _wrap_filetag(mime_type: str, text: str) -> str:
        tag = f"file_{mime_type.split(';')[0].strip().replace('/', '_')}"
        return f"<{tag}>{text}</{tag}>"

    def _build_inputs(self, messages: tuple[Any, ...]) -> list[dict[str, Any]]:
        content = [self._process_message(m) for m in messages]
        chat: list[dict[str, Any]] = []
        if self.system_message:
            chat.append({"role": "system", "content": [{"type": "text", "text": self.system_message}]})
        chat.append({"role": "user", "content": content})
        return chat

    def _sync_prompt(self, messages: tuple[Any, ...]) -> str:
        chat = self._build_inputs(messages)
        outputs = self._pipeline(text=chat, return_full_text=False, **self.generation_kwargs)
        generated = outputs[0]["generated_text"]
        if isinstance(generated, list):
            return generated[-1]["content"]
        return generated

    async def prompt(self, messages: tuple[Any, ...]) -> Any:
        return await asyncio.to_thread(self._sync_prompt, messages)


if pil_image.module_available():

    @TransformersVisionPrompter._process_message.register(pil_image.Image)  # type: ignore[attr-defined, untyped-decorator]
    def _(self: TransformersVisionPrompter, msg: Any) -> dict[str, Any]:
        return {"type": "image", "image": msg}
