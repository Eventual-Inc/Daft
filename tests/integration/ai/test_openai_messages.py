from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest
from pydantic import BaseModel

import daft
from daft.functions.ai import prompt
from daft.functions.ai.messages import append_message, build_messages

if TYPE_CHECKING:
    from typing import Literal


@pytest.fixture(scope="module", autouse=True)
def skip_no_credential(pytestconfig):
    from dotenv import load_dotenv

    load_dotenv()
    if not pytestconfig.getoption("--credentials"):
        pytest.skip(reason="OpenAI integration tests require the `--credentials` flag.")
    if os.environ.get("OPENAI_API_KEY") is None:
        pytest.skip(reason="OpenAI integration tests require the OPENAI_API_KEY environment variable.")


@pytest.fixture(scope="module", autouse=True)
def session(skip_no_credential, url):
    """Configures the session to be used for all tests."""
    with daft.session() as session:
        # the key is not explicitly needed, but was added with angry lookup for clarity.
        session.set_provider("openai", api_key=os.environ["OPENAI_API_KEY"], base_url=url)
        yield


def input_multimodal_user_content(endpoint: Literal["chat", "responses"], input_type: str, modalities: list[str]):
    return {
        "text": ["Describe what attachments you see in less than 2 sentences?"],
        "image": ["https://docs.getdaft.io/en/stable/img/audio-transcription-cover.jpg"],
        "audio": [
            "https://huggingface.co/datasets/Eventual-Inc/sample-files/resolve/main/audio/Daft_Team_Takes_On_Climbing_Dogpatch_Boulders_SF.mp3"
        ],
        "file": ["tests/assets/sampled-tpch.csv"],
    }


def input_generation_config(endpoint: Literal["chat", "responses"]):
    out = None
    calculator_tool = {
        "type": "function",
        "name": "calculate",
        "description": "Perform mathematical calculations",
        "strict": None,
        "parameters": {
            "type": "object",
            "properties": {
                "expression": {
                    "type": "string",
                    "description": "The mathematical expression to evaluate",
                },
            },
            "required": ["expression"],
        },
    }
    weather_tool = {
        "type": "function",
        "name": "get_current_weather",
        "description": "Get the current weather in a given location",
        "parameters": {
            "type": "object",
            "properties": {
                "location": {
                    "type": "string",
                    "description": "The city and state, e.g. San Francisco, CA",
                },
                "unit": {"type": "string", "enum": ["celsius", "fahrenheit"]},
            },
            "required": ["location", "unit"],
        },
    }

    if endpoint == "chat":
        out = {
            "max_tokens": 100,  # Simple temperature & max_tokens
            "tools": [calculator_tool, weather_tool],
            "tool_choice": "auto",
        }

    if endpoint == "responses":
        out = {
            "max_output_tokens": 100,  # Simple temperature & max_output_tokens
            "instructions": "What is the weather like in Boston today?",
            "reasoning": {
                "effort": "low",
                "summary": "detailed",
            },
            "tools": [{"type": "web_search_preview"}, calculator_tool],  # built in oai tool
        }

    return out


def input_json_schema():
    from enum import Enum

    class Modalities(Enum):
        TEXT = "text"
        IMAGE = "image"
        AUDIO = "audio"
        FILE = "file"

    class InputDescription(BaseModel):
        modalities: list[Modalities]
        description: str

    return InputDescription


# @pytest.mark.parametrize(
#    "model_name, base_url, api_key, generation_config, chat_template, input_method",
#    [
#        (
#            "openai/gpt-5-nano",
#            "https://api.openai.com/v1",
#            os.environ["OPENAI_API_KEY"],
#            {},
#            "openai-responses",
#            "none",
#        ),
#        (
#            "openai/gpt-5-nano",
#            "https://api.openai.com/v1",
#            os.environ["OPENAI_API_KEY"],
#            {},
#            "openai-chat-completion",
#            "provider",
#        ),
#        (
#            "openai/gpt-5-nano",
#            "https://openrouter.ai/api/v1",
#            os.environ["OPENROUTER_API_KEY"],
#            {},
#            "openai-responses",
#            "none",
#        ),
#        (
#            "openai/gpt-5-nano",
#            "https://openrouter.ai/api/v1",
#            os.environ["OPENROUTER_API_KEY"],
#            {},
#            "openai-chat-completion",
#            "request",
#        ),
#    ],
# )
# def test_oai_responses_on_providers(model_name, base_url, api_key, generation_config):
#    from daft.ai.openai.provider import OpenAIProvider
#    from daft.sessions import Session
#
#    session = Session()
#    provider = OpenAIProvider(name="myprovider", api_key=api_key, base_url=base_url)
#    session.attach_provider(provider)
#    session.set_provider("myprovider")
#
#    messages = input_multimodal_user_content(
#        "responses",
#    )
#
#    df = daft.from_pydict(messages)


# @pytest.mark.parametrize(
#    "endpoint, input_type, include_extra_body, include_json_schema, include_history, modalities",
#    [
#        (endpoint, input_type, include_extra_body, include_json_schema, include_history, modalities)
#        for endpoint in ["chat", "responses"]
#        for input_type in ["single", "list"]
#        for include_extra_body in [False, True]
#        for include_json_schema in [False, True]
#        for include_history in [False, True]
#        for modalities in [
#            ["text"],
#            ["text", "image"],
#            ["text", "file"],
#            ["text", "image", "file"],
#            ["text", "audio"],
#            ["text", "image", "audio"],
#            ["text", "file", "audio"],
#            ["text", "image", "file", "audio"],
#        ]
#        if not ((endpoint == "responses" and "audio" in modalities) or (endpoint == "chat" and "file" in modalities)) # responses endpoint doesn't support audio and chat completions don't support file inputs
#    ],
# )
@pytest.mark.parametrize(
    "model_name, endpoint, input_type, include_extra_body, include_json_schema, modalities",
    [
        ("openai/gpt-5-nano", "responses", "single", False, True, ["text", "image", "audio", "file"]),
    ],
)
@pytest.mark.integration()
def test_prompt(session, model_name, endpoint, input_type, input_generation_config, modalities):
    import json

    from daft import col

    messages = input_multimodal_user_content("responses")
    generation_config = input_generation_config("responses")
    df = daft.from_pydict(messages)
    df = (
        df
        # Build Messages Column
        .with_column(
            "messages",
            build_messages(
                chat_template="openai-responses",
                user_text=col("text"),
                user_image=col("image"),
                user_audio=col("audio"),
                user_file=col("file"),
            ),
        )
        # Generate Text
        .with_column(
            "generated_text",
            prompt(
                messages=col("messages"),
                model=model_name,
                endpoint=endpoint,
                generation_config=generation_config,
            ),
        )
    )

    output = df.to_pydict()

    print(json.dumps(output, indent=2))


@pytest.mark.parametrize(
    "model_name, endpoint, input_type, include_extra_body, modalities",
    [
        ("openai/gpt-5-nano", "responses", "list", True, ["text"]),
    ],
)
@pytest.mark.integration()
def test_multiturn_prompt(session, model_name, endpoint, input_type, input_generation_config, modalities):
    import json

    from daft import col

    generation_config = input_generation_config("responses")
    messages = input_multimodal_user_content("responses")
    df = daft.from_pydict(messages)
    # Load the message history
    df = (
        # Load the multimodal user content
        df
        # Build Messages Column
        .with_column(
            "messages_1",
            build_messages(
                text=col("text"),
                image=col("image"),
                audio=col("audio"),
                file=col("file"),
                endpoint=endpoint,
            ),
        )
        # Generate Text
        .with_column(
            "response_1",
            prompt(
                messages=col("messages_1"),
                provider="openai",
                model=model_name,
                endpoint=endpoint,
            ),
        )
        # Append the output to the history
        .with_column(
            "history",
            append_message(col("messages_1"), col("response_1")),
        )
        # Build the messages column for the second turn
        .with_column(
            "messages_2",
            build_messages(
                text=col("text"),
                image=col("image"),
                audio=col("audio"),
                file=col("file"),
                history=col("history"),
                endpoint=endpoint,
            ),
        )
        # Generate Text
        .with_column(
            "responses_2",
            prompt(
                messages=col("messages_2"),
                model=model_name,
                endpoint=endpoint,
                generation_config=generation_config,
            ),
        )
    )
    output = df.to_pydict()
    df.show()
    print(json.dumps(output, indent=2))
