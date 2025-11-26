from __future__ import annotations

from dataclasses import dataclass, field
from functools import singledispatchmethod
from typing import TYPE_CHECKING, Any, get_type_hints

from openai import AsyncOpenAI
from openai.types.completion_usage import CompletionUsage
from openai.types.responses import ResponseUsage

from daft.ai.metrics import record_token_metrics
from daft.ai.protocols import Prompter, PrompterDescriptor
from daft.ai.typing import UDFOptions
from daft.dependencies import np
from daft.file import File

if TYPE_CHECKING:
    from pydantic import BaseModel

    from daft.ai.openai.typing import OpenAIProviderOptions
    from daft.ai.typing import Options


def _extract_pydantic_model_info(model_cls: type[BaseModel]) -> dict[str, Any]:
    """Extract serializable information from a Pydantic model class.

    This extracts the model name and field type hints, which can be serialized
    and used to reconstruct an equivalent Pydantic model on a worker.
    """
    return {
        "name": model_cls.__name__,
        "field_types": get_type_hints(model_cls),
        "field_defaults": {
            name: info.default for name, info in model_cls.model_fields.items() if not info.is_required()
        },
    }


def _reconstruct_pydantic_model(model_info: dict[str, Any]) -> type[BaseModel]:
    """Reconstruct a Pydantic model class from serialized model info."""
    from pydantic import create_model

    fields = {}
    for name, typ in model_info["field_types"].items():
        if name in model_info["field_defaults"]:
            fields[name] = (typ, model_info["field_defaults"][name])
        else:
            fields[name] = (typ, ...)

    return create_model(model_info["name"], **fields)


@dataclass
class OpenAIPrompterDescriptor(PrompterDescriptor):
    """Descriptor for OpenAI prompter configuration.

    Note: Instead of storing the Pydantic return_format class directly (which causes
    serialization issues with cloudpickle in environments like Google Colab), we store
    serializable model info and reconstruct the class on the worker side.
    """

    provider_name: str
    provider_options: OpenAIProviderOptions
    model_name: str
    model_options: Options
    system_message: str | None = None
    return_format: BaseModel | None = field(default=None, repr=False)
    _return_format_info: dict[str, Any] | None = field(default=None, repr=False)
    udf_options: UDFOptions | None = None
    use_chat_completions: bool = False

    def __post_init__(self) -> None:
        """Extract serializable model info from return_format if provided."""
        if self.return_format is not None and self._return_format_info is None:
            self._return_format_info = _extract_pydantic_model_info(self.return_format)

    def __getstate__(self) -> dict[str, Any]:
        """Custom pickle state that excludes the Pydantic class.

        This avoids issues with cloudpickle capturing IPython/ZMQ internals when
        serializing Pydantic classes defined in interactive environments like Google Colab.
        """
        state = self.__dict__.copy()
        # Don't serialize the Pydantic class - we'll reconstruct from _return_format_info
        state["return_format"] = None
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Restore state after unpickling."""
        self.__dict__.update(state)

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return self.model_options

    def get_udf_options(self) -> UDFOptions:
        return self.udf_options or UDFOptions(concurrency=None, num_gpus=None)

    def instantiate(self) -> Prompter:
        # Reconstruct return_format from serialized info if needed
        return_format = self.return_format
        if return_format is None and self._return_format_info is not None:
            return_format = _reconstruct_pydantic_model(self._return_format_info)

        return OpenAIPrompter(
            provider_name=self.provider_name,
            provider_options=self.provider_options,
            model=self.model_name,
            system_message=self.system_message,
            return_format=return_format,
            generation_config=self.model_options,
            use_chat_completions=self.use_chat_completions,
        )


class OpenAIPrompter(Prompter):
    """OpenAI prompter implementation using AsyncOpenAI for chat completions."""

    def __init__(
        self,
        provider_name: str,
        provider_options: OpenAIProviderOptions,
        model: str,
        system_message: str | None = None,
        return_format: BaseModel | None = None,
        generation_config: dict[str, Any] = {},
        use_chat_completions: bool = False,
    ) -> None:
        self.provider_name = provider_name
        self.model = model
        self.return_format = return_format
        self.system_message = system_message
        self.use_chat_completions = use_chat_completions
        # Separate client params from generation params
        client_params_keys = ["base_url", "api_key", "timeout", "max_retries"]
        client_params = {**provider_options}
        for key, value in generation_config.items():
            if key in client_params_keys:
                client_params[key] = value

        self.generation_config = {k: v for k, v in generation_config.items() if k not in client_params_keys}
        self.llm = AsyncOpenAI(**client_params)

    @singledispatchmethod
    def _process_message(self, msg: Any) -> dict[str, Any]:
        """Fallback for unsupported message content types."""
        raise ValueError(f"Unsupported content type in prompt: {type(msg)}")

    @_process_message.register
    def _process_str_message(self, msg: str) -> dict[str, Any]:
        """Handle string messages as plain text."""
        if self.use_chat_completions:
            return {"type": "text", "text": msg}
        else:
            return {"type": "input_text", "text": msg}

    @_process_message.register
    def _process_bytes_message(self, msg: bytes) -> dict[str, Any]:
        """Handle bytes messages by converting to File and processing."""
        mime_type, encoded_content = self._encode_bytes(msg)

        if mime_type.startswith("image/"):
            return self._build_image_message(encoded_content)
        return self._build_file_message(encoded_content)

    @_process_message.register
    def _process_file_message(self, msg: File) -> dict[str, Any]:
        """Handle File objects."""
        mime_type = msg.mime_type()
        if self._is_text_mime_type(mime_type):
            filetag = f"file_{mime_type.replace('/', '_')}"
            text_content = f"<{filetag}>{self._read_text_content(msg)}</{filetag}>"
            return self._process_str_message(text_content)

        mime_type, encoded_content = self._encode_file(msg)

        if mime_type.startswith("image/"):
            return self._build_image_message(encoded_content)
        return self._build_file_message(encoded_content)

    if np.module_available():  # type: ignore[attr-defined]

        @_process_message.register(np.ndarray)
        def _process_image_message(self, msg: np.typing.NDArray[Any]) -> dict[str, Any]:
            """Handle numpy array messages (images)."""
            import base64
            import io

            from daft.dependencies import pil_image

            pil_image = pil_image.fromarray(msg)
            bio = io.BytesIO()
            pil_image.save(bio, "PNG")
            base64_string = base64.b64encode(bio.getvalue()).decode("utf-8")
            encoded_content = f"data:image/png;base64,{base64_string}"
            return self._build_image_message(encoded_content)

    def _encode_bytes(self, msg: bytes) -> tuple[str, str]:
        import base64

        from daft.daft import guess_mimetype_from_content

        maybe_mime_type = guess_mimetype_from_content(msg)
        mime_type = maybe_mime_type if maybe_mime_type else "application/octet-stream"
        base64_string = base64.b64encode(msg).decode("utf-8")
        encoded_content = f"data:{mime_type};base64,{base64_string}"
        return mime_type, encoded_content

    def _encode_file(self, file_obj: File) -> tuple[str, str]:
        import base64

        mime_type = file_obj.mime_type()
        with file_obj.open() as f:
            base64_string = base64.b64encode(f.read()).decode("utf-8")
        encoded_content = f"data:{mime_type};base64,{base64_string}"
        return mime_type, encoded_content

    def _is_text_mime_type(self, mime_type: str) -> bool:
        normalized = mime_type.split(";")[0].strip().lower()
        return normalized.startswith("text/")

    def _read_text_content(self, file_obj: File) -> str:
        with file_obj.open() as f:
            file_bytes = f.read()

        if isinstance(file_bytes, str):
            return file_bytes

        if isinstance(file_bytes, bytes):
            try:
                return file_bytes.decode("utf-8")
            except UnicodeDecodeError:
                return file_bytes.decode("utf-8", errors="replace")

        raise TypeError("File contents must be bytes or string")

    def _build_image_message(self, encoded_content: str) -> dict[str, Any]:
        if self.use_chat_completions:
            return {"type": "image_url", "image_url": {"url": encoded_content}}
        return {"type": "input_image", "image_url": encoded_content}

    def _build_file_message(self, encoded_content: str, filename: str = "file") -> dict[str, Any]:
        if self.use_chat_completions:
            return {
                "type": "file",
                "file": {
                    "filename": filename,
                    "file_data": encoded_content,
                },
            }
        return {
            "type": "input_file",
            "filename": filename,
            "file_data": encoded_content,
        }

    def _record_usage_metrics(
        self,
        input_tokens: int,
        output_tokens: int,
        total_tokens: int,
    ) -> None:
        record_token_metrics(
            protocol="prompt",
            model=self.model,
            provider=self.provider_name,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens,
        )

    async def _prompt_with_chat_completions(self, messages_list: list[dict[str, Any]]) -> Any:
        """Generate responses using the Chat Completions API."""
        if self.return_format is not None:
            # Use structured outputs with Pydantic model
            response = await self.llm.chat.completions.parse(
                model=self.model,
                messages=messages_list,
                response_format=self.return_format,
                **self.generation_config,
            )
            result = response.choices[0].message.parsed
        else:
            # Return plain text
            response = await self.llm.chat.completions.create(
                model=self.model,
                messages=messages_list,
                **self.generation_config,
            )
            result = response.choices[0].message.content

        usage = response.usage
        if usage is not None and isinstance(usage, CompletionUsage):
            self._record_usage_metrics(
                input_tokens=usage.prompt_tokens,
                output_tokens=usage.completion_tokens,
                total_tokens=usage.total_tokens,
            )
        return result

    async def _prompt_with_responses(self, messages_list: list[dict[str, Any]]) -> Any:
        """Generate responses using the Responses API."""
        if self.return_format is not None:
            response = await self.llm.responses.parse(
                model=self.model,
                input=messages_list,
                text_format=self.return_format,
                **self.generation_config,
            )
            result = response.output_parsed
        else:
            response = await self.llm.responses.create(
                model=self.model,
                input=messages_list,
                **self.generation_config,
            )
            result = response.output_text

        usage = response.usage
        if usage is not None and isinstance(usage, ResponseUsage):
            self._record_usage_metrics(
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                total_tokens=usage.total_tokens,
            )
        return result

    async def prompt(self, messages: tuple[Any, ...]) -> Any:
        """Generate responses for a batch of message strings."""
        messages_list = []
        if self.system_message is not None:
            messages_list.append({"role": "system", "content": self.system_message})

        content = [self._process_message(msg) for msg in messages]
        messages_list.append({"role": "user", "content": content})  # type: ignore [dict-item]

        if self.use_chat_completions:
            return await self._prompt_with_chat_completions(messages_list)
        else:
            return await self._prompt_with_responses(messages_list)
