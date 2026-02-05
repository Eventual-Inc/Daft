from __future__ import annotations

import pytest

import daft
from daft import col
from daft.expressions import Expression


@pytest.mark.parametrize(
    ("name", "build_expr", "expected"),
    [
        ("camel_case", Expression.to_camel_case, ["helloWorld", "helloWorld", "helloWorld"]),
        ("upper_camel_case", Expression.to_upper_camel_case, ["HelloWorld", "HelloWorld", "HelloWorld"]),
        ("snake_case", Expression.to_snake_case, ["hello_world", "hello_world", "hello_world"]),
        ("upper_snake_case", Expression.to_upper_snake_case, ["HELLO_WORLD", "HELLO_WORLD", "HELLO_WORLD"]),
        ("kebab_case", Expression.to_kebab_case, ["hello-world", "hello-world", "hello-world"]),
        ("upper_kebab_case", Expression.to_upper_kebab_case, ["HELLO-WORLD", "HELLO-WORLD", "HELLO-WORLD"]),
        ("title_case", Expression.to_title_case, ["Hello World", "Hello World", "Hello World"]),
    ],
)
def test_dataframe_string_case(name, build_expr, expected) -> None:
    df = daft.from_pydict({"text": ["helloWorld", "hello-world", "HelloWorld"]})
    expr = build_expr(col("text"))
    result = df.select(expr.alias(name)).to_pydict()[name]
    assert result == expected
