from __future__ import annotations

from typing import TYPE_CHECKING

from daft import func as daft_func
from daft.ai.openai.messages import (
    build_chat_completion_messages,
    build_responses_messages,
    prepare_file_or_audio_for_openai,
    prepare_image_for_openai,
    safe_json_loads_messages,
)

if TYPE_CHECKING:
    from typing import Literal

    from daft.expressions import Expression


def build_messages(
    chat_template: Literal[
        "openai-chat-completion",
        "openai-responses",
    ] = "openai-responses",
    *,
    message_history: Expression | None = None,
    system_message: Expression | None = None,
    user_text: Expression | None = None,
    user_image: Expression | None = None,
    user_audio: Expression | None = None,
    user_file: Expression | None = None,
) -> Expression:
    """Build input messages for a chat template.

    Args:
        chat_template (Literal["openai-chat-completion", "openai-responses"]): The chat template to use.
        message_history (String Expression): The message history to include.
        system_message (String or List[String] Expression): The system messages to include.
        user_text (String or List[String] Expression): The user text to include.
        user_image (Image or List[Image] Expression): The user image to include.
        user_audio (File or List[File] Expression): The user audio to include.
        user_file (File or List[File] Expression): The user file to include.

    Returns:
        String Expression: A JSON string of a list of messages formatted for the chat template.
    """
    if user_image is not None:
        user_image = prepare_image_for_openai(user_image)
    if user_audio is not None:
        user_audio = prepare_file_or_audio_for_openai(user_audio)
    if user_file is not None:
        user_file = prepare_file_or_audio_for_openai(user_file)

    if chat_template == "openai-chat-completion":
        return build_chat_completion_messages(
            message_history=message_history,
            system_message=system_message,
            user_text=user_text,
            user_image=user_image,
            user_audio=user_audio,
            user_file=user_file,
        )
    elif chat_template == "openai-responses":
        return build_responses_messages(
            message_history=message_history,
            system_message=system_message,
            user_text=user_text,
            user_image=user_image,
            user_audio=user_audio,
            user_file=user_file,
        )
    else:
        raise ValueError(f"Unsupported chat template: {chat_template}")


@daft_func()
def append_message(history: str, messages: str, role: str = "user") -> str:
    """Append a message to a chat history.

    Args:
        history: The chat history to append to.
        response: The response to append.
        role: The role of the message to append.

    Returns:
        The updated chat history.
    """
    import json

    history_list = safe_json_loads_messages(history, role=role)
    messages_list = safe_json_loads_messages(messages, role=role)
    history_list.extend(messages_list)
    return json.dumps(history_list, indent=4)
