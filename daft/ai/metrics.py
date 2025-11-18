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
