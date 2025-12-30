from __future__ import annotations

from daft.udf.metrics import increment_counter


def record_token_metrics(
    protocol: str,
    model: str,
    provider: str,
    *,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    total_tokens: int | None = None,
) -> None:
    attrs = {
        "model": model,
        "protocol": protocol,
        "provider": provider,
    }
    if input_tokens is not None:
        increment_counter("input tokens", input_tokens, attributes=attrs)
    if output_tokens is not None:
        increment_counter("output tokens", output_tokens, attributes=attrs)
    if total_tokens is not None:
        increment_counter("total tokens", total_tokens, attributes=attrs)
    increment_counter("requests", attributes=attrs)


def record_text_embedding_metrics(
    *,
    model: str,
    provider: str,
    num_texts: int | None = None,
    input_characters: int | None = None,
    input_tokens: int | None = None,
    total_tokens: int | None = None,
    dimensions: int | None = None,
) -> None:
    """Record observability metrics for `embed_text` calls.

    This is intentionally provider-agnostic:
    - Remote providers (OpenAI/Google) may provide token counts.
    - Local providers (Transformers) generally won't, but we still want request/text volume.

    Notes:
    - `dimensions` is optional and recorded as an attribute (not a counter) when provided.
    - `total_tokens` is kept for providers that report it; embeddings usually have no output tokens.
    """
    attrs: dict[str, str] = {
        "model": model,
        "protocol": "embed_text",
        "provider": provider,
    }
    if dimensions is not None:
        attrs["dimensions"] = str(dimensions)

    if num_texts is not None:
        increment_counter("input texts", num_texts, attributes=attrs)
    if input_characters is not None:
        increment_counter("input characters", input_characters, attributes=attrs)
    if input_tokens is not None:
        increment_counter("input tokens", input_tokens, attributes=attrs)
    if total_tokens is not None:
        increment_counter("total tokens", total_tokens, attributes=attrs)

    increment_counter("requests", attributes=attrs)
