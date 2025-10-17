from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from openai import AsyncOpenAI

from daft.ai.protocols import Prompter, PrompterDescriptor
from daft.ai.utils import get_http_udf_options

if TYPE_CHECKING:
    from pydantic import BaseModel

    from daft.ai.openai.typing import OpenAIProviderOptions
    from daft.ai.typing import Options, UDFOptions


@dataclass
class OpenAIPrompterDescriptor(PrompterDescriptor):
    provider_name: str
    provider_options: OpenAIProviderOptions
    model_name: str
    model_options: Options
    system_message: str | None = None
    return_format: BaseModel | None = None

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return self.model_options

    def get_udf_options(self) -> UDFOptions:
        return get_http_udf_options()

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

    async def prompt(self, user_message: str) -> Any:
        """Generate responses for a batch of message strings."""
        # Each message is a string prompt
        if self.system_message is not None:
            messages_list = [
                {"role": "system", "content": self.system_message},
                {"role": "user", "content": user_message},
            ]
        else:
            messages_list = [{"role": "user", "content": user_message}]

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
