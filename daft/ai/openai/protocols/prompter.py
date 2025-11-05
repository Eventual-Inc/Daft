from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from openai import AsyncOpenAI

from daft.ai.protocols import Prompter, PrompterDescriptor
from daft.ai.typing import UDFOptions
from daft.file import File

if TYPE_CHECKING:
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
    ) -> None:
        self.model = model
        self.return_format = return_format
        self.system_message = system_message
        # Separate client params from generation params
        client_params_keys = ["base_url", "api_key", "timeout", "max_retries"]
        client_params = {**provider_options}
        for key, value in generation_config.items():
            if key in client_params_keys:
                client_params[key] = value

        self.generation_config = {k: v for k, v in generation_config.items() if k not in client_params_keys}
        self.llm = AsyncOpenAI(**client_params)

    def _process_message_content(self, msg: Any) -> dict[str, str]:
        import base64

        from daft.dependencies import np

        # Strings are always treated as plain text
        if isinstance(msg, str):
            return {"type": "input_text", "text": msg}

        # Handle numpy arrays (images)
        if isinstance(msg, np.ndarray):
            import io

            from daft.dependencies import pil_image

            pil_image = pil_image.fromarray(msg)
            bio = io.BytesIO()
            pil_image.save(bio, "PNG")
            base64_string = base64.b64encode(bio.getvalue()).decode("utf-8")
            encoded_content = f"data:image/png;base64,{base64_string}"
            return {"type": "input_image", "image_url": encoded_content}

        # Handle bytes and File objects
        if isinstance(msg, bytes):
            daft_file = File(msg)
            mime_type = daft_file.mime_type()
            with daft_file.open() as f:
                base64_string = base64.b64encode(f.read()).decode("utf-8")
            encoded_content = f"data:{mime_type};base64,{base64_string}"
        elif isinstance(msg, File):
            mime_type = msg.mime_type()
            with msg.open() as f:
                base64_string = base64.b64encode(f.read()).decode("utf-8")
            encoded_content = f"data:{mime_type};base64,{base64_string}"
        else:
            raise ValueError(f"Unsupported content type in prompt: {type(msg)}")
        # Determine if it's an image or generic file based on MIME type
        if mime_type.startswith("image/"):
            return {"type": "input_image", "image_url": encoded_content}
        else:
            return {"type": "input_file", "file_url": encoded_content}

    async def prompt(self, messages: tuple[Any, ...]) -> Any:
        messages_list = []
        if self.system_message is not None:
            messages_list.append({"role": "system", "content": self.system_message})

        content = [self._process_message_content(msg) for msg in messages]
        messages_list.append({"role": "user", "content": content})  # type: ignore [dict-item]

        if self.return_format is not None:
            # Use structured outputs with Pydantic model
            response = await self.llm.responses.parse(
                model=self.model,
                input=messages_list,
                text_format=self.return_format,
                **self.generation_config,
            )
            return response.output_parsed
        else:
            # Return plain text
            response = await self.llm.responses.create(
                model=self.model,
                input=messages_list,
                **self.generation_config,
            )
            return response.output_text
