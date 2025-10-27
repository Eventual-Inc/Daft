from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from openai import AsyncOpenAI, OpenAI

from daft.ai.openai.typing import DEFAULT_OPENAI_BASE_URL
from daft.ai.openai.utils import normalize_model_name
from daft.ai.protocols import Prompter, PrompterDescriptor
from daft.ai.utils import get_http_udf_options

if TYPE_CHECKING:
    from typing import Any

    import pydantic

    from daft.ai.openai.typing import OpenAIProviderOptions
    from daft.ai.typing import Options, UDFOptions


@dataclass
class OpenAIPrompterDescriptor(PrompterDescriptor):
    provider_name: str
    provider_options: OpenAIProviderOptions
    model_name: str
    model_options: Options
    system_message: str | None = None
    return_format: pydantic.BaseModel | None = None

    def __post_init__(self) -> None:
        from daft.ai.openai.utils import validate_model_availability

        self.model_name = normalize_model_name(
            self.model_name, self.provider_options.get("base_url", DEFAULT_OPENAI_BASE_URL)
        )

        # Ensure model is listed in the client's model list.
        validate_model_availability(OpenAI(**self.provider_options), self.model_name)

        # Resolve Runtime Flags from provider/request options for Chat/Responses API
        self.use_chat_completions = self.provider_options.pop("use_chat_completions", False) or self.model_options.pop(
            "use_chat_completions", False
        )

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return self.model_options

    def get_udf_options(self) -> UDFOptions:
        return get_http_udf_options()

    def get_use_chat_completions(self) -> bool:
        return self.use_chat_completions

    def instantiate(self) -> Prompter:
        return OpenAIPrompter(
            client=AsyncOpenAI(**self.provider_options),
            model=self.model_name,
            system_message=self.system_message,
            return_format=self.return_format,
            use_chat_completions=self.use_chat_completions,
            generation_config=self.model_options,
        )


class OpenAIPrompter(Prompter):
    """OpenAI prompter implementation using AsyncOpenAI for chat completions."""

    def __init__(
        self,
        client: AsyncOpenAI,
        model: str,
        system_message: str | None,
        return_format: pydantic.BaseModel | None,
        use_chat_completions: bool,
        generation_config: dict[str, Any] = {},
    ) -> None:
        self.client = client

        # Inputs
        self.model = model
        self.return_format = return_format
        self.system_message = system_message
        self.generation_config = generation_config

        # Runtime Flags
        self.use_chat_completions = use_chat_completions

    async def prompt(self, messages: str) -> Any:
        """Prompt the model with the given messages.

        Args:
            messages (str): A single text string or JSON string list of messages as user content.

        Returns:
            The response from the model as a string.
        """
        from daft.ai.openai.messages import safe_json_loads_messages

        messages_list = safe_json_loads_messages(messages)

        try:
            if self.use_chat_completions:
                return await self.chat_completions(messages_list)

            else:
                return await self.responses(messages_list)
        except Exception as e:
            raise ValueError(f"Error prompting model: {e}") from e

    async def responses(self, messages: list[dict[str, Any]]) -> Any:
        if self.system_message is not None:
            system_messages = [{"role": "system", "content": self.system_message}]
            messages = system_messages + messages

        if self.return_format is not None:
            response = await self.client.responses.parse(
                model=self.model,
                input=messages,
                text_format=self.return_format,
                **self.generation_config,
            )
            return response.output_parsed

        else:
            response = await self.client.responses.create(
                model=self.model,
                input=messages,
                **self.generation_config,
            )

            return response.output_text

    async def chat_completions(self, messages: list[dict[str, Any]]) -> Any:
        if self.system_message is not None:
            system_messages = [{"role": "system", "content": self.system_message}]
            messages = system_messages + messages

        if self.return_format is not None:
            response = await self.client.chat.completions.parse(
                model=self.model,
                messages=messages,
                response_format=self.return_format,
                **self.generation_config,
            )
            result = response.choices[0].message

            # Handle model refusal
            if result.refusal:
                raise ValueError(f"""
Error: Model refused to respond.
{result.refusal}

Model: {self.model}
System message: {self.system_message}
Messages: {messages}
Generation config: {self.generation_config}
Return format: {self.return_format}
""")
            else:
                return result.parsed

        else:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                **self.generation_config,
            )
            return response.choices[0].message.content
