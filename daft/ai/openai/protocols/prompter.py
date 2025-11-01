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

    def _encode_image(self, image: Any) -> str:
        """Encodes an image into a base64 string."""
        import base64

        from daft.dependencies import np

        # If the image is a numpy array, convert it to a PIL image and save it to a bytes buffer in PNG format, then encode it to a base64 string.
        if isinstance(image, np.ndarray):
            import io

            from daft.dependencies import pil_image

            pil_image = pil_image.fromarray(image)
            bio = io.BytesIO()
            pil_image.save(bio, "PNG")
            base64_string = base64.b64encode(bio.getvalue()).decode("utf-8")
            return f"data:image/png;base64,{base64_string}"
        # If the image is a bytes object or a string, use the File class to get the mime type and read the file into a bytes object, then encode it to a base64 string.
        elif isinstance(image, bytes) or isinstance(image, str):
            daft_file = File(image)
            mime_type = daft_file.mime_type()
            with daft_file.open() as f:
                base64_string = base64.b64encode(f.read()).decode("utf-8")
                return f"data:{mime_type};base64,{base64_string}"
        # If the image is already a File object, get the mime type and read the file into a bytes object, then encode it to a base64 string.
        elif isinstance(image, File):
            mime_type = image.mime_type()
            with image.open() as f:
                base64_string = base64.b64encode(f.read()).decode("utf-8")
                return f"data:{mime_type};base64,{base64_string}"
        # If the image is not a supported type, raise an error.
        else:
            raise ValueError(f"Unsupported image type in prompt: {type(image)}")

    async def prompt(self, messages: tuple[Any, ...]) -> Any:
        """Generate responses for a batch of message strings."""
        # Each message is a string prompt
        messages_list = []
        if self.system_message is not None:
            messages_list.append({"role": "system", "content": self.system_message})

        content = []
        for message in messages:
            if isinstance(message, str):
                content.append({"type": "input_text", "text": message})
            else:
                content.append({"type": "input_image", "image_url": self._encode_image(message)})

        messages_list.append({"role": "user", "content": content})

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
