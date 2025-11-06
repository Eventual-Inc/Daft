from __future__ import annotations

from dataclasses import dataclass
from functools import singledispatchmethod
from typing import TYPE_CHECKING, Any

from openai import AsyncOpenAI

from daft.ai.protocols import Prompter, PrompterDescriptor
from daft.ai.typing import UDFOptions
from daft.dependencies import np
from daft.file import File

if TYPE_CHECKING:
    import numpy.typing as npt
    from pydantic import BaseModel

    from daft.ai.openai.typing import OpenAIProviderOptions
    from daft.ai.typing import Options


@dataclass
class OpenAIPrompterDescriptor(PrompterDescriptor):
    provider_name: str
    provider_options: OpenAIProviderOptions
    model_name: str
    model_options: Options
    system_message: str | None = None
    return_format: BaseModel | None = None
    udf_options: UDFOptions | None = None
    use_chat_completions: bool = False

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return self.model_options

    def get_udf_options(self) -> UDFOptions:
        return self.udf_options or UDFOptions(concurrency=None, num_gpus=None)

    def instantiate(self) -> Prompter:
        return OpenAIPrompter(
            provider_options=self.provider_options,
            model=self.model_name,
            system_message=self.system_message,
            return_format=self.return_format,
            generation_config=self.model_options,
            use_chat_completions=self.use_chat_completions,
        )


class OpenAIPrompter(Prompter):
    """OpenAI prompter implementation using AsyncOpenAI for chat completions."""

    def __init__(
        self,
        provider_options: OpenAIProviderOptions,
        model: str,
        system_message: str | None = None,
        return_format: BaseModel | None = None,
        generation_config: dict[str, Any] = {},
        use_chat_completions: bool = False,
    ) -> None:
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
        daft_file = File(msg)
        mime_type, encoded_content = self._encode_file(daft_file)

        if mime_type.startswith("image/"):
            return self._build_image_message(encoded_content)
        return self._build_file_message(encoded_content)

    @_process_message.register
    def _process_file_message(self, msg: File) -> dict[str, Any]:
        """Handle File objects."""
        mime_type, encoded_content = self._encode_file(msg)

        if mime_type.startswith("image/"):
            return self._build_image_message(encoded_content)
        return self._build_file_message(encoded_content)

    @_process_message.register(np.ndarray)
    def _process_image_message(self, msg: npt.NDArray[Any]) -> dict[str, Any]:
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

    def _encode_file(self, file_obj: File) -> tuple[str, str]:
        import base64

        mime_type = file_obj.mime_type()
        with file_obj.open() as f:
            base64_string = base64.b64encode(f.read()).decode("utf-8")
        encoded_content = f"data:{mime_type};base64,{base64_string}"
        return mime_type, encoded_content

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
            return response.choices[0].message.parsed
        else:
            # Return plain text
            response = await self.llm.chat.completions.create(
                model=self.model,
                messages=messages_list,
                **self.generation_config,
            )
            return response.choices[0].message.content

    async def _prompt_with_responses(self, messages_list: list[dict[str, Any]]) -> Any:
        """Generate responses using the Responses API."""
        if self.return_format is not None:
            response = await self.llm.responses.parse(
                model=self.model,
                input=messages_list,
                text_format=self.return_format,
                **self.generation_config,
            )
            return response.output_parsed
        else:
            response = await self.llm.responses.create(
                model=self.model,
                input=messages_list,
                **self.generation_config,
            )
            return response.output_text

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
