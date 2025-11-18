from __future__ import annotations

from typing import Any

import pytest

from daft import Expression, Series, col, from_pydict, sql
from daft.recordbatch import RecordBatch


@pytest.fixture(scope="function")
def test_expression():
    def _test_expression(
        *,
        expr_args: list[Expression] = [],
        data: list[Any],
        expected: list[Any],
        name: str,
        namespace: str | None = None,
        sql_name: str | None = None,
        args: list[Any] = [],
        kwargs: dict | None = None,
    ):
        fn_name = name
        fn_args = args
        fn_kwargs = kwargs if kwargs else {}
        namespace = namespace
        # Track if we need to skip Series assertion for replace with regex=True
        skip_series_assert = False

        col_expr = col("c0")
        if namespace:
            # For deprecated namespace methods, use daft.functions instead
            import daft

            # All namespace methods (str, dt, list, struct, etc.) are now in daft.functions
            if namespace in (
                "str",
                "dt",
                "list",
                "struct",
                "url",
                "partitioning",
                "embedding",
                "binary",
                "image",
                "float",
            ):
                # Map old function names to new ones
                function_name_map = {
                    "extract": "regexp_extract",  # .str.extract() -> regexp_extract()
                }
                actual_fn_name = function_name_map.get(fn_name, fn_name)
                # Handle replace with regex=True -> use regexp_replace instead
                if fn_name == "replace" and fn_kwargs.get("regex", False):
                    actual_fn_name = "regexp_replace"
                    # Remove regex from kwargs as regexp_replace doesn't take it
                    fn_kwargs = {k: v for k, v in fn_kwargs.items() if k != "regex"}
                    # Skip Series assertion as Series.str.replace() with regex=True has issues
                    skip_series_assert = True
                fn = getattr(daft.functions, actual_fn_name)
                expr = fn(col_expr, *fn_args, **fn_kwargs)
            else:
                col_expr = getattr(col_expr, namespace)
                expr = getattr(col_expr, fn_name)(*fn_args, **fn_kwargs)
        else:
            expr = getattr(col_expr, fn_name)(*fn_args, **fn_kwargs)

        sql_args = ["c0"]
        for arg in fn_args:
            if arg is None:
                sql_args.append("NULL")
            elif isinstance(arg, str):
                sql_args.append(f"'{arg}'")
            elif isinstance(arg, list):
                items = [f"'{item}'" if isinstance(item, str) else str(item) for item in arg]
                sql_args.append(f"[{', '.join(items)}]")
            else:
                sql_args.append(str(arg))

        # format kwargs for SQL
        kwargs_parts = []
        for k, v in fn_kwargs.items():
            if isinstance(v, bool):
                if sql_name == "regexp_replace" and k == "regex":
                    # dataframe replace func takes 'regex' bool param, but sql regexp_replace doesn't"
                    pass
                else:
                    kwargs_parts.append(f"{k}:={str(v).lower()}")
            elif isinstance(v, str):
                kwargs_parts.append(f"{k}:='{v}'")
            else:
                kwargs_parts.append(f"{k}:={v}")

        kwargs_str = ", ".join(kwargs_parts)

        sql_name = sql_name if sql_name else fn_name

        sql_expr = f"{sql_name}({', '.join(sql_args)}"
        if kwargs_str:
            sql_expr += f", {kwargs_str}"
        sql_expr += ")"

        df = from_pydict({"c0": data})
        df_res = df.select(expr).to_pydict()["c0"]
        rb_result = RecordBatch.from_pydict({"c0": data}).eval_expression_list([expr]).to_pydict()["c0"]
        sql_res = sql(f"select {sql_expr} from df").to_pydict()["c0"]

        series = Series.from_pylist(data)
        if namespace:
            series = getattr(series, namespace)
            # Handle replace with regex=True for Series (Series namespace still exists)
            if fn_name == "replace" and fn_kwargs.get("regex", False):
                # Series.str.replace() with regex=True maps to regexp_replace
                # Convert string args to Series (broadcast single value to all rows)
                pattern_val = fn_args[0] if fn_args else None
                replacement_val = fn_args[1] if len(fn_args) > 1 else None
                if isinstance(pattern_val, str):
                    pattern_series = Series.from_pylist([pattern_val] * len(data))
                else:
                    pattern_series = pattern_val
                if isinstance(replacement_val, str):
                    replacement_series = Series.from_pylist([replacement_val] * len(data))
                else:
                    replacement_series = replacement_val
                series_res = series.replace(pattern_series, replacement_series, regex=True)
            else:
                # Convert string args to Series if needed (for methods that expect Series)
                converted_args = []
                for arg in fn_args:
                    if isinstance(arg, str):
                        # Broadcast string to all rows
                        converted_args.append(Series.from_pylist([arg] * len(data)))
                    else:
                        converted_args.append(arg)
                series_res = getattr(series, fn_name)(*converted_args, **fn_kwargs)
        else:
            series_res = getattr(series, fn_name)(*fn_args, **fn_kwargs)
        series_res = series_res.rename("c0").to_pylist()
        assert df_res == expected
        assert rb_result == expected
        assert sql_res == expected
        if not skip_series_assert:
            assert series_res == expected

    yield _test_expression
