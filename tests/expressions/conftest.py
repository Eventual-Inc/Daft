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

        col_expr = col("c0")
        if namespace:
            col_expr = getattr(col_expr, namespace)
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

        series_res = getattr(series, fn_name)(*fn_args, **fn_kwargs)
        series_res = series_res.rename("c0").to_pylist()
        assert df_res == expected
        assert rb_result == expected
        assert sql_res == expected
        assert series_res == expected

    yield _test_expression
