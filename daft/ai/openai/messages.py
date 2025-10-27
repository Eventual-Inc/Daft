from __future__ import annotations

import base64
import json
from typing import Any

from daft import func as daft_func
from daft.expressions import Expression  # noqa: TC001
from daft.file import File  # noqa: TC001

###
# Chat History Message Helpers
###


def safe_json_loads_messages(messages: str, role: str = "user") -> list[dict[str, Any]]:
    """This function is used to safely load the messages into a list of messages for the prompt ai function.

    Daft's prompt AI function accepts both a string or JSON string as inputs for the messages parameter.

    Supported inputs include:
    - A JSON string of a list of messages, like `[{"role": "user", "content": "Hello, world!"}]`
    - A list of messages, like `[{"role": "user", "content": "Hello, world!"}]`
    - A single message dict, like `{"role": "user", "content": "Hello, world!"}`
    - A single message string, like `"Hello, world!"`

    If the messages is not a valid JSON string, it will raise a ValueError.

    Args:
        messages (str): A JSON string of a list of messages, a list of messages, a single message dict, or a single message string.

    Returns:
        messages (list[dict[str, Any]]): A list of messages.
    """
    from json import JSONDecodeError

    try:
        data = json.loads(messages)

        # Case 1: Already a list of messages
        if isinstance(data, list):
            if "role" in data[0].keys():  # Assume data is a list of messages with roles already
                return data
            else:
                return [{"role": role, "content": data}]  # Assume data is a list of content.

        # Case 2: A json dumped string
        elif isinstance(data, str):
            return [{"role": role, "content": data}]  # Assume data is just text.

        # Case 3: Single message dict - wrap in list
        elif isinstance(data, dict):
            if "role" in data and "content" in data:
                return [data]
            else:
                raise ValueError(f"Unexpected messages json: {messages}")

        else:
            raise ValueError(f"Unexpected messages json: {messages}")

    except JSONDecodeError as e:
        if e.msg.startswith("Expecting value"):
            data = [{"role": role, "content": messages}]
            return data
        else:
            raise ValueError(f"Failed to load messages json of type {type(messages)}: {e}") from e
    except Exception as e:
        raise ValueError(f"Failed to load messages json of type {type(messages)}: {e}") from e


###
# Multimodal Input Helpers
###
@daft_func()
def file2dataurl(file: File) -> dict[str, str]:
    """Convert a File expression to a base64 data URL.

    Args:
        file (File): The file expression to convert.

    Returns:
        data_url (str): A base64 data URL of the file.
    """
    with file.open() as f:
        mime_type = f.mime_type()
        try:
            b64 = base64.b64encode(f.read()).decode("utf-8")
            return {"filename": str(f.name), "url": f"data:{mime_type};base64,{b64}"}
        except Exception as e:
            raise ValueError(f"Failed to convert bytes to base64 data URL: {e}") from e


# Expression-level Conversion Utilities
def is_http_or_data_url(expr: Expression) -> Expression:
    """Check if the expression is a HTTP or data URL.

    Args:
        expr (Expression): The expression to check.

    Returns:
        Expression: The expression if it is a HTTP or data URL, otherwise None.
    """
    from daft.functions import regexp

    return regexp(expr, r"^data:[^;]+;base64,") | regexp(expr, r"^(?:https?)://")


def image2dataurl(image: Expression) -> Expression:
    """Convert an image expression to a base64 data URL.

    Args:
        image (Image Expression): Image expression to convert.

    Returns:
        data_url (str): A base64 data URL of the image.
    """
    from daft.functions import encode, encode_image, format

    image_bytes = encode_image(image, "PNG")
    b64 = encode(image_bytes, "base64")
    return format("data:{};base64,{}", "image/png", b64)


###
# OpenAI Specific Modality Preprocessing Helpers.
# Ensures the input is in the correct format for all OpenAI APIs. (Provider Specific Implementation)
###


def prepare_image_for_openai(image: Expression) -> Expression:
    """Prepare an image expression for OpenAI.

    Args:
        image (Expression): The image expression to prepare.

    Returns:
        Expression: The prepared image expression.
    """
    return is_http_or_data_url(image).if_else(image, image2dataurl(image))


def prepare_file_or_audio_for_openai(file_or_audio: Expression) -> Expression:
    """Prepare a file or audio expression for OpenAI. If the expression is a HTTP or data URL, it will be returned as is. Otherwise, if provided as a  it will be converted to a base64 data URL.

    Args:
        file_or_audio (Expression): The file or audio expression to prepare.

    Returns:
        data_url: A base64 data URL of the file or audio if
    """
    return is_http_or_data_url(file_or_audio).if_else(file_or_audio, file2dataurl(file_or_audio))


###
# Chat Template Builders
###


@daft_func()
def build_chat_completion_messages(
    *,
    message_history: str | None = None,
    system_message: str | None = None,
    user_text: str | None = None,
    user_image: str | None = None,
    user_audio: str | None = None,
    user_file: dict[str, str] | None = None,
) -> str:
    """Build messages for OpenAI Chat Completions API.

    Chat Completions API Multimodal User Content Message Format:
    - text : {"type": "text", "text": "..."}
    - image : {"type": "image_url", "image_url": {"url": "..."}}
    - audio : {"type": "audio_url", "audio_url": {"url": "..."}}
    - file

    Args:
        history: JSON string of previous messages
        system: System message(s)
        user_text: Text input(s)
        user_image: Image input(s)
        user_audio: Audio input(s)
        user_file: File input(s)


    Returns:
        A JSON String of a list of messages formatted for chat.completions.create()
    """
    messages = []
    content = []

    # Load message history if provided
    if message_history is not None:
        messages = safe_json_loads_messages(message_history)

    if system_message is not None:
        messages.append({"role": "system", "content": system_message})

    # Add text content
    if user_text is not None:
        content.append({"type": "text", "text": user_text})

    # Add image content
    if user_image is not None:
        content.append({"type": "image_url", "image_url": {"url": user_image}})

    # Add audio content
    if user_audio is not None:
        content.append({"type": "audio_url", "audio_url": {"url": user_audio}})

    # Handle files
    if user_file is not None:
        raise ValueError("File inputs are not supported in Chat Completions API.")

    # Finally, append user message with all content
    if content:
        messages.append(
            {
                "role": "user",
                "content": content,
            }
        )

    return json.dumps(messages, indent=4)


@daft_func()
def build_responses_messages(
    *,
    history: str | None = None,
    system_message: str | None = None,
    user_text: str | None = None,
    user_image: str | None = None,
    user_audio: dict[str, str] | None = None,
    user_file: dict[str, str] | None = None,
) -> str:
    """Build messages for OpenAI Responses API.

    Responses API uses:
    - "input_text" type for text content
    - "input_image" type with direct value structure
    - "input_file" type for file content
    - No audio support yet

    Args:
        history: JSON string of previous messages
        system: System message(s)
        user_text: Text input(s)
        user_image: Image input(s)
        user_audio: Audio input(s) - not supported in Responses API, will warn
        user_file: File input(s)

    Returns:
        A JSON String of a list of messages formatted for responses.create()
    """
    messages = []
    content = []

    # Load message history if provided
    if history:
        messages = safe_json_loads_messages(history)

    if system_message is not None:
        messages.append({"role": "system", "content": system_message})

    # Add text content
    if user_text is not None:
        content.append({"type": "input_text", "text": user_text})

    # Add image content
    if user_image is not None:
        content.append({"type": "input_image", "input_image": user_image})

    # Audio not yet supported in Responses API
    if user_audio is not None:
        raise ValueError("Audio input is not supported in the OpenAI Responses API.")

    # Add file content
    if user_file is not None:
        content.append({"type": "input_file", "filename": user_file["filename"], "file_data": user_file["url"]})

    # Append user message with all content
    if content:
        messages.append(
            {
                "role": "user",
                "content": content,
            }
        )

    return json.dumps(messages, indent=4)
