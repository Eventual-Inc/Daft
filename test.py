from __future__ import annotations

import daft

call_count = 0


@daft.func(return_dtype=daft.DataType.string())
def random_prompt(_seed: str) -> str:
    global call_count
    call_count += 1
    return f"Hello, world! {call_count}"


df = (
    daft.from_pydict({"prompt": ["Hello, world!", "This is a test", "Goodbye, world!"]})
    .with_column("random_prompt", random_prompt(daft.lit("literal-only input")))
)

actual = df.to_pydict()
print(actual)
assert call_count == 3, f"expected UDF to run once per row, got {call_count}"
assert actual["random_prompt"] == ["Hello, world! 1", "Hello, world! 2", "Hello, world! 3"]
