from __future__ import annotations

from daft import Expression
from daft.expressions.expressions import ExpressionNamespace


class ExpressionJsonNamespace(ExpressionNamespace):
    def query(self, jq_query: str) -> Expression:
        """Query JSON data in a column using a JQ-style filter https://jqlang.github.io/jq/manual/
        This expression uses jaq as the underlying executor, see https://github.com/01mf02/jaq for the full list of supported filters.

        Example:
            >>> df = daft.from_pydict({"col": ['{"a": 1}', '{"a": 2}', '{"a": 3}']})
            >>> df.with_column("res", df["col"].json.query(".a")).collect()
            ╭──────────┬──────╮
            │ col      ┆ res  │
            │ ---      ┆ ---  │
            │ Utf8     ┆ Utf8 │
            ╞══════════╪══════╡
            │ {"a": 1} ┆ 1    │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
            │ {"a": 2} ┆ 2    │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
            │ {"a": 3} ┆ 3    │
            ╰──────────┴──────╯

        Args:
            jq_query (str): JQ query string

        Returns:
            Expression: Expression representing the result of the JQ query as a column of JSON-compatible strings
        """

        return Expression._from_pyexpr(self._expr.json_query(jq_query))
