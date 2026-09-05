from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from daft.dependencies import pa
from daft.logical.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from sqlalchemy.engine import Connection
    from sqlglot.expressions import Expr


logger = logging.getLogger(__name__)


class SQLConnection:
    def __init__(self, conn: str | Callable[[], Connection], driver: str, dialect: str, url: str) -> None:
        self.conn = conn
        self.dialect = dialect
        self.driver = driver
        self.url = url

    def __repr__(self) -> str:
        # Deliberately omit the URL: secrets can appear anywhere in a
        # connection string (userinfo, query params, driver extras), so
        # the safest mitigation is to not echo it at all.
        return f"SQLConnection(dialect={self.dialect!r}, driver={self.driver!r})"

    @classmethod
    def from_url(cls, url: str) -> SQLConnection:
        scheme = urlparse(url).scheme.strip().lower()
        if "+" in scheme:
            dialect, driver = scheme.split("+")
        else:
            dialect, driver = scheme, ""
        return cls(url, driver, dialect, url)

    @classmethod
    def from_connection_factory(cls, conn_factory: Callable[[], Connection]) -> SQLConnection:
        from sqlalchemy.engine import Connection

        try:
            with conn_factory() as connection:
                if not isinstance(connection, Connection):
                    raise ValueError(
                        f"Connection factory must return a SQLAlchemy connection object, got: {type(connection)}"
                    )
                dialect = connection.engine.dialect.name
                driver = connection.engine.driver
                url = connection.engine.url.render_as_string(hide_password=True)
            return cls(conn_factory, driver, dialect, url)
        except Exception as e:
            raise ValueError(f"Unexpected error while calling the connection factory: {e}") from e

    def read_schema(self, sql: str, infer_schema_length: int) -> Schema:
        if self._should_use_connectorx():
            sql = self.construct_sql_query(sql, limit=0)
        else:
            sql = self.construct_sql_query(sql, limit=infer_schema_length)
        table = self.execute_sql_query(sql)
        schema = Schema.from_pyarrow_schema(table.schema)
        return schema

    def construct_sql_query(
        self,
        sql: str,
        projection: Sequence[str | Expr] | None = None,
        predicate: str | None = None,
        limit: int | None = None,
        partition_bounds: tuple[str, str] | None = None,
    ) -> str:
        # If all options are None, just return the original sql
        if projection is None and predicate is None and limit is None and partition_bounds is None:
            return sql

        import sqlglot

        target_dialect = self.dialect
        # sqlglot does not support "postgresql" dialect, it only supports "postgres"
        if target_dialect == "postgresql":
            target_dialect = "postgres"
        # sqlglot does not recognize "mssql" as a dialect, it instead recognizes "tsql", which is the SQL dialect for Microsoft SQL Server
        elif target_dialect == "mssql":
            target_dialect = "tsql"
        # clickhouse-connect SQLAlchemy driver registers as "clickhousedb", but sqlglot recognizes "clickhouse"
        elif target_dialect == "clickhousedb":
            target_dialect = "clickhouse"
        # sqlglot does not recognize "awsathena", the dialect registered by PyAthena, SQLAlchemy driver for reading from AWS Athena. It only support "athena"
        elif target_dialect == "awsathena":
            target_dialect = "athena"

        if not any(target_dialect == supported_dialect.value for supported_dialect in sqlglot.Dialects):
            raise ValueError(
                f"Unsupported dialect: {target_dialect}, please refer to the documentation for supported dialects."
            )

        query = sqlglot.subquery(sql, "subquery", dialect=target_dialect)

        if projection is not None:
            projection = _adapt_projection_for_dialect(projection, target_dialect)
            query = query.select(*projection)
        else:
            query = query.select("*")

        if predicate is not None:
            query = query.where(predicate)

        if partition_bounds is not None:
            query = query.where(partition_bounds[0]).where(partition_bounds[1])

        if limit is not None:
            query = query.limit(limit)

        return query.sql(dialect=target_dialect)

    def _should_use_connectorx(self) -> bool:
        # Supported DBs extracted from here https://github.com/sfu-db/connector-x/tree/7b3147436b7e20b96691348143d605e2249d6119?tab=readme-ov-file#sources
        connectorx_supported_dbs = {
            "postgres",
            "postgresql",
            "mysql",
            "mssql",
            "oracle",
            "bigquery",
            "sqlite",
            "clickhouse",
            "redshift",
        }

        return isinstance(self.conn, str) and self.dialect in connectorx_supported_dbs and self.driver == ""

    def execute_sql_query(self, sql: str, schema: pa.Schema | None = None) -> pa.Table:
        if schema is None and self._should_use_connectorx():
            return self._execute_sql_query_with_connectorx(sql)
        else:
            return self._execute_sql_query_with_sqlalchemy(sql, schema=schema)

    def _execute_sql_query_with_connectorx(self, sql: str) -> pa.Table:
        import connectorx as cx

        assert isinstance(self.conn, str)
        logger.info("Using connectorx to execute sql: %s", sql)
        try:
            table = cx.read_sql(conn=self.conn, query=sql, return_type="arrow")
            return table
        except Exception as e:
            # The connection URL is deliberately omitted from the error message:
            # secrets can appear anywhere in it (userinfo, query params,
            # driver-specific extras), so dropping the URL is the only robust
            # mitigation. The caller knows which connection they passed in,
            # so the URL is redundant here.
            raise RuntimeError(f"Failed to execute sql: {sql}, error: {e}") from e

    def _execute_sql_query_with_sqlalchemy(self, sql: str, schema: pa.Schema | None = None) -> pa.Table:
        from sqlalchemy import create_engine, text

        logger.info("Using sqlalchemy to execute sql: %s", sql)
        try:
            if isinstance(self.conn, str):
                with create_engine(self.conn).connect() as connection:
                    result = connection.execute(text(sql))
                    rows = result.fetchall()
            else:
                with self.conn() as connection:
                    result = connection.execute(text(sql))
                    rows = result.fetchall()

            pydict = {column_name: [row[i] for row in rows] for i, column_name in enumerate(result.keys())}
            return pa.Table.from_pydict(pydict, schema=schema)
        except Exception as e:
            # See note in `_execute_sql_query_with_connectorx`: don't echo
            # back the connection URL.
            raise RuntimeError(f"Failed to execute sql: {sql}, error: {e}") from e


def _adapt_projection_for_dialect(
    projection: Sequence[str | Expr],
    target_dialect: str,
) -> Sequence[str | Expr]:
    """Apply dialect-specific AST transforms to projection expressions.

    Only rewrites ``exp.Expression`` items; raw ``str`` projections
    (e.g. ``"COUNT(*)"``) pass through unchanged.
    """
    import sqlglot.expressions as exp

    if target_dialect == "clickhouse":
        return [
            p.transform(_rewrite_percentile_to_clickhouse) if isinstance(p, exp.Expression) else p for p in projection
        ]
    if target_dialect == "tsql":
        return [p.transform(_rewrite_percentile_to_tsql) if isinstance(p, exp.Expression) else p for p in projection]
    return projection


def _rewrite_percentile_to_clickhouse(node: Expr) -> Expr:
    """Rewrite ``WithinGroup(PercentileDisc, Order)`` → ``quantileExactLow`` for ClickHouse.

    ``quantileExactLow`` uses ClickHouse's exact, lower-median convention and
    returns values in the input type. Its rank convention is not identical to
    ``PERCENTILE_DISC`` for every fraction, but it provides ordered, exact
    values suitable for adjacent range boundaries, including the true
    endpoints. ``quantile()`` is deliberately avoided because it uses
    randomized reservoir sampling and returns approximate, non-deterministic
    results.

    ``node`` is a single AST node visited by ``Expression.transform()``.
    The parent ``Alias`` wrapper (from ``.as_("bound_n")``) is preserved
    automatically by ``transform()``.
    """
    import sqlglot.expressions as exp

    if isinstance(node, exp.WithinGroup) and isinstance(node.this, exp.PercentileDisc):
        p_val = node.this.this
        if not isinstance(node.expression, exp.Order):
            raise TypeError(f"Expected exp.Order for percentile WITHIN GROUP, got {type(node.expression).__name__}")
        order_expressions = node.expression.expressions
        if len(order_expressions) != 1:
            raise ValueError(f"Expected exactly one ORDER BY expression for percentile, got {len(order_expressions)}")
        ordered = order_expressions[0]
        if not isinstance(ordered, exp.Ordered):
            raise TypeError(f"Expected exp.Ordered, got {type(ordered).__name__}")
        if ordered.args.get("desc"):
            raise ValueError("DESC ordering is not supported for percentile rewrite")
        order_col = ordered.this
        return exp.ParameterizedAgg(
            this="quantileExactLow",
            expressions=[p_val],
            params=[order_col],
        )
    return node


def _rewrite_percentile_to_tsql(node: Expr) -> Expr:
    """Wrap ``WithinGroup(PercentileDisc)`` in ``Window`` for TSQL ``OVER ()``.

    SQL Server requires ``PERCENTILE_DISC(...) WITHIN GROUP (...) OVER ()``;
    SQLGlot 30.8 does not add ``OVER ()`` automatically.

    Unlike the ClickHouse rewrite, TSQL **does** support ``DESC`` in the
    ``WITHIN GROUP (ORDER BY ...)`` clause, so we do not reject it.
    """
    import sqlglot.expressions as exp

    if isinstance(node, exp.WithinGroup) and isinstance(node.this, exp.PercentileDisc):
        if not isinstance(node.expression, exp.Order):
            raise TypeError(f"Expected exp.Order for percentile WITHIN GROUP, got {type(node.expression).__name__}")
        if not node.expression.expressions:
            raise ValueError("Expected at least one ORDER BY expression")
        return exp.Window(this=node)
    return node
