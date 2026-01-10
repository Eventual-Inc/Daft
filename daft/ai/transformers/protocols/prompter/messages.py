"""Message handling for the Transformers Prompter.

This module handles prompt(messages=...) processing:
- Content serialization (str, bytes, File, ndarray → content parts)
- Building content parts for HF chat template format

Follows the same singledispatch pattern as OpenAI prompter.
"""

from __future__ import annotations

import re
from functools import singledispatchmethod
from typing import Any

from daft.ai.provider import ProviderImportError
from daft.dependencies import np, pil_image
from daft.file import File


class TransformersPrompterMessageProcessor:
    """Formats raw prompt inputs into HF chat template content parts.

    Handles converting raw inputs (str, bytes, File, ndarray) to content parts
    that can be used with processor.apply_chat_template().

    For multimodal models, images are embedded directly in content as PIL Images:
        {"type": "image", "image": <PIL.Image>}

    This follows the same pattern as OpenAI prompter's message processing.
    """

    def process_messages(
        self,
        messages: tuple[Any, ...],
        system_message: str | None = None,
    ) -> list[dict[str, Any]]:
        """Process raw messages into HF content parts.

        Args:
            messages: Raw user inputs (strings, bytes, files, images).

        Returns:
            List of content parts ready for chat template.
        """
        messages_list: list[dict[str, Any]] = []

        if system_message is not None:
            # Use string for system message as some templates don't support list content for system role
            messages_list.append({"role": "system", "content": system_message})

        user_content = [self._process_content_part(msg) for msg in messages]
        messages_list.append({"role": "user", "content": user_content})

        return messages_list

    @singledispatchmethod
    def _process_content_part(self, msg: Any) -> dict[str, Any]:
        """Fallback for unsupported content types."""
        raise ValueError(f"Unsupported content type in prompt: {type(msg)}")

    @_process_content_part.register
    def _process_str(self, msg: str) -> dict[str, Any]:
        """Handle string messages.

        Strings that look like image URLs are treated as image references.
        """
        if self._is_image_url(msg):
            return {"type": "image", "url": msg}
        return {"type": "text", "text": msg}

    @_process_content_part.register
    def _process_bytes(self, msg: bytes) -> dict[str, Any]:
        """Handle bytes messages by detecting MIME type."""
        from daft.daft import guess_mimetype_from_content

        maybe_mime_type = guess_mimetype_from_content(msg)
        mime_type = maybe_mime_type if maybe_mime_type else "application/octet-stream"

        if mime_type.startswith("image/"):
            return self._build_image_from_bytes(msg)
        return self._build_text_from_bytes(msg)

    @_process_content_part.register
    def _process_file(self, msg: File) -> dict[str, Any]:
        """Handle File objects."""
        mime_type = msg.mime_type()

        if self._is_text_mime_type(mime_type):
            filetag = f"file_{mime_type.replace('/', '_')}"
            text_content = f"<{filetag}>{self._read_text_content(msg)}</{filetag}>"
            return {"type": "text", "text": text_content}

        with msg.open() as f:
            file_bytes = f.read()

        if mime_type.startswith("image/"):
            return self._build_image_from_bytes(file_bytes)
        return self._build_text_from_bytes(file_bytes)

    if np.module_available():  # type: ignore[attr-defined]

        @_process_content_part.register(np.ndarray)
        def _process_ndarray(self, msg: np.typing.NDArray[Any]) -> dict[str, Any]:
            """Handle numpy array messages (images)."""
            return {"type": "image", "image": msg}

    if pil_image.module_available():

        @_process_content_part.register(pil_image.Image)
        def _process_pil_image(self, msg: pil_image.Image) -> dict[str, Any]:
            """Handle PIL Image messages."""
            return {"type": "image", "image": msg}

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _build_image_from_bytes(self, data: bytes) -> dict[str, Any]:
        """Build an image content part from bytes."""
        import io

        from daft.dependencies import pil_image

        if not pil_image.module_available():
            raise ProviderImportError("transformers", function="prompt")

        image = pil_image.open(io.BytesIO(data))
        return {"type": "image", "image": image}

    def _build_text_from_bytes(self, data: bytes) -> dict[str, Any]:
        """Build a text content part from bytes."""
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = data.decode("utf-8", errors="replace")
        return {"type": "text", "text": text}

    def _is_text_mime_type(self, mime_type: str) -> bool:
        """Check if MIME type is text-based."""
        normalized = mime_type.split(";")[0].strip().lower()
        return normalized.startswith("text/")

    _HTTP_URL_RE = re.compile(r"^https?://", flags=re.IGNORECASE)
    _DATA_IMAGE_URL_RE = re.compile(r"^data:image/[^;]+;base64,", flags=re.IGNORECASE)

    def _is_image_url(self, s: str) -> bool:
        """Heuristic check for strings that represent images via URL / data-URL.

        This mirrors other providers where image URLs can be passed as strings.
        """
        return bool(self._HTTP_URL_RE.match(s) or self._DATA_IMAGE_URL_RE.match(s))

    def _read_text_content(self, file_obj: File) -> str:
        """Read text content from a File object."""
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
