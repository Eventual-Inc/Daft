"""Message handling for the Transformers Prompter.

This module is the single place where prompt(messages=...) is handled:
- Content serialization (str, bytes, File, ndarray → content parts)
- Chat structure building (content parts → messages list for chat template)
- Image extraction (for models needing separate image tensors)
"""

from __future__ import annotations

import re
from functools import singledispatchmethod
from typing import Any

from daft.ai.provider import ProviderImportError
from daft.dependencies import np
from daft.file import File


class ChatTemplateFormatter:
    """Formats raw prompt inputs into HF chat template format.

    Handles:
    - Converting raw inputs (str, bytes, File, ndarray) to content parts
    - Building chat message structure with roles
    - Extracting images for processors that need them separately
    """

    def format(
        self,
        raw_messages: tuple[Any, ...],
        system_message: str | None = None,
    ) -> tuple[list[dict[str, Any]], list[Any]]:
        """Format raw inputs into chat messages + extracted images.

        Args:
            raw_messages: Raw user inputs (strings, bytes, files, images).
            system_message: Optional system prompt.

        Returns:
            Tuple of (messages, images) ready for processor.apply_chat_template()
        """
        messages: list[dict[str, Any]] = []

        if system_message:
            messages.append({"role": "system", "content": system_message})

        content, images = self._build_content(raw_messages)
        messages.append({"role": "user", "content": content})

        return messages, images

    def _build_content(self, raw_messages: tuple[Any, ...]) -> tuple[list[dict[str, Any]], list[Any]]:
        """Convert raw inputs to content parts, extracting images."""
        content: list[dict[str, Any]] = []
        images: list[Any] = []

        for msg in raw_messages:
            part = self._process_content_part(msg)
            if part["type"] == "image":
                # Two image representations are supported:
                # - In-memory image objects (ndarray / PIL): extracted and passed separately
                # - URL/data-URL references: left inline for processor.apply_chat_template()
                if "url" in part:
                    content.append({"type": "image", "url": part["url"]})
                else:
                    # Image placeholder for chat template, actual image extracted
                    content.append({"type": "image"})
                    images.append(part["image"])
            else:
                content.append(part)

        return content, images

    # =========================================================================
    # Content Part Processing (singledispatch pattern)
    # =========================================================================

    @singledispatchmethod
    def _process_content_part(self, msg: Any) -> dict[str, Any]:
        """Fallback for unsupported content types."""
        raise ValueError(f"Unsupported content type in prompt: {type(msg)}")

    @_process_content_part.register
    def _process_str(self, msg: str) -> dict[str, Any]:
        """Handle string messages.

        By default, strings are treated as plain text.

        If the string looks like an image URL / data-URL, treat it as an image block
        (so multimodal HF processors can fetch/parse it via apply_chat_template()).
        """
        if self._is_image_url(msg):
            # HF multimodal chat templates expect {"type": "image", "url": "..."} blocks.
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
