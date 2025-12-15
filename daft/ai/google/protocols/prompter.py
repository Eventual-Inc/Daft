from __future__ import annotations

from dataclasses import dataclass
from functools import singledispatchmethod
from typing import TYPE_CHECKING, Any

from google import genai
from google.genai import types

from daft.ai.google.typing import GoogleProviderOptions
from daft.ai.metrics import record_token_metrics
from daft.ai.protocols import Prompter, PrompterDescriptor
from daft.ai.provider import ProviderImportError
from daft.ai.utils import merge_provider_and_api_options
from daft.dependencies import np
from daft.file import File

if TYPE_CHECKING:
    from pydantic import BaseModel

    from daft.ai.typing import Options, PromptOptions


@dataclass
class GooglePrompterDescriptor(PrompterDescriptor):
    provider_name: str
    provider_options: GoogleProviderOptions
    model_name: str
    prompt_options: PromptOptions
    system_message: str | None = None
    return_format: BaseModel | None = None

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return dict(self.prompt_options)

    def instantiate(self) -> Prompter:
        return GooglePrompter(
            provider_name=self.provider_name,
            provider_options=self.provider_options,
            model=self.model_name,
            system_message=self.system_message,
            return_format=self.return_format,
            prompt_options=self.prompt_options,
        )


class GooglePrompter(Prompter):
    """Google prompter implementation using google-genai SDK."""

    def __init__(
        self,
        provider_name: str,
        provider_options: GoogleProviderOptions,
        model: str,
        system_message: str | None = None,
        return_format: BaseModel | None = None,
        prompt_options: PromptOptions = {},
    ) -> None:
        self.provider_name = provider_name
        self.model = model
        self.return_format = return_format
        self.system_message = system_message

        # Merge provider and remaining user options
        merged_provider_options = merge_provider_and_api_options(
            provider_options=provider_options,
            api_options=prompt_options,
            provider_option_type=GoogleProviderOptions,
        )

        # Prepare generation config
        generation_config_keys = types.GenerateContentConfig.model_fields.keys()

        config_params = {}
        for key, value in prompt_options.items():
            if key in generation_config_keys:
                config_params[key] = value

        if self.system_message:
            config_params["system_instruction"] = self.system_message

        if self.return_format:
            config_params["response_mime_type"] = "application/json"
            config_params["response_schema"] = self.return_format.model_json_schema()

        self.generation_config = types.GenerateContentConfig(**config_params)

        # Initialize client
        self.client = genai.Client(**merged_provider_options)

    @singledispatchmethod
    def _process_message(self, msg: Any) -> types.Part:
        """Fallback for unsupported message content types."""
        raise ValueError(f"Unsupported content type in prompt: {type(msg)}")

    @_process_message.register
    def _process_str_message(self, msg: str) -> types.Part:
        """Handle string messages as plain text."""
        return types.Part.from_text(text=msg)

    @_process_message.register
    def _process_bytes_message(self, msg: bytes) -> types.Part:
        """Handle bytes messages."""
        mime_type, _ = self._guess_mime_type(msg)
        return types.Part.from_bytes(data=msg, mime_type=mime_type)

    @_process_message.register
    def _process_file_message(self, msg: File) -> types.Part:
        """Handle File objects."""
        mime_type = msg.mime_type()

        # If it's a text file, read it as text
        if self._is_text_mime_type(mime_type):
            filetag = f"file_{mime_type.replace('/', '_')}"
            text_content = f"<{filetag}>{self._read_text_content(msg)}</{filetag}>"
            return types.Part.from_text(text=text_content)

        # Otherwise treat as binary (image, pdf, etc)
        with msg.open() as f:
            file_bytes = f.read()

        return types.Part.from_bytes(data=file_bytes, mime_type=mime_type)

    if np.module_available():  # type: ignore[attr-defined]

        @_process_message.register(np.ndarray)
        def _process_image_message(self, msg: np.typing.NDArray[Any]) -> types.Part:
            """Handle numpy array messages (images)."""
            import io

            from daft.dependencies import pil_image

            if not pil_image.module_available():
                raise ProviderImportError("google", function="prompt")

            pil_image = pil_image.fromarray(msg)
            bio = io.BytesIO()
            pil_image.save(bio, "PNG")
            return types.Part.from_bytes(data=bio.getvalue(), mime_type="image/png")

    def _guess_mime_type(self, msg: bytes) -> tuple[str, str | None]:
        from daft.daft import guess_mimetype_from_content

        maybe_mime_type = guess_mimetype_from_content(msg)
        mime_type = maybe_mime_type if maybe_mime_type else "application/octet-stream"
        return mime_type, None

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

    async def prompt(self, messages: tuple[Any, ...]) -> Any:
        """Generate responses for a batch of message strings."""
        # Convert messages to Parts
        parts = [self._process_message(msg) for msg in messages]

        # Call API
        response = await self.client.aio.models.generate_content(
            model=self.model,
            contents=[types.Content(role="user", parts=parts)],
            config=self.generation_config,
        )

        # Record metrics
        if response.usage_metadata:
            self._record_usage_metrics(
                input_tokens=response.usage_metadata.prompt_token_count or 0,
                output_tokens=response.usage_metadata.candidates_token_count or 0,
                total_tokens=response.usage_metadata.total_token_count or 0,
            )

        # Parse result
        if self.return_format:
            return response.parsed
        else:
            return response.text
