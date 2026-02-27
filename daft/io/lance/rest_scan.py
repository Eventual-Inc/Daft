# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from daft.context import get_context
from daft.daft import CountMode, PyExpr, PyPartitionField, PyPushdowns, PyRecordBatch, ScanTask
from daft.dependencies import pa
from daft.expressions import Expression
from daft.io.scan import ScanOperator
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch

from ..pushdowns import SupportsPushdownFilters
from .rest_config import LanceRestConfig

if TYPE_CHECKING:
    try:
        import lance_namespace
    except ImportError:
        lance_namespace = None

# Make lance_namespace available for testing/mocking
try:
    import lance_namespace
except ImportError:
    lance_namespace = None

logger = logging.getLogger(__name__)


def _lance_rest_factory_function(
    rest_config: LanceRestConfig,
    namespace: str,
    table_name: str,
    query_params: dict[str, Any],
) -> Iterator[PyRecordBatch]:
    """Factory function for REST-based Lance table scanning."""
    if lance_namespace is None:
        raise ImportError(
            "Unable to import the `lance_namespace` package, please ensure it is installed: `pip install lance-namespace`"
        )

    # Create REST client
    client_config: dict[str, Any] = {"uri": rest_config.base_url}
    if rest_config.api_key:
        client_config["api_key"] = rest_config.api_key
    if rest_config.headers:
        client_config.update(rest_config.headers)

    client = lance_namespace.connect("rest", client_config)

    # Execute query via REST API
    try:
        # Build query request
        query_request = {
            "k": query_params.get("limit", 1000000),  # Default large limit
            "vector": {"single_vector": [0.0]},  # Dummy vector for now
        }

        # Add filters if present
        if "filter" in query_params and query_params["filter"]:
            query_request["filter"] = query_params["filter"]

        # Add column selection if present
        if "columns" in query_params and query_params["columns"]:
            columns_list = query_params["columns"]
            if isinstance(columns_list, list):
                query_request["columns"] = {"column_names": columns_list}

        # Add limit if present
        if "limit" in query_params:
            limit_value = query_params["limit"]
            if isinstance(limit_value, int):
                query_request["k"] = limit_value

        # Add offset if present
        if "offset" in query_params:
            offset_value: int = query_params["offset"]
            query_request["offset"] = offset_value

        # Query the table
        result = client.query_table(namespace, table_name, query_request)

        # Convert Arrow IPC response to record batches
        if hasattr(result, "to_batches"):
            for batch in result.to_batches():
                yield RecordBatch.from_arrow_record_batches([batch], batch.schema)._recordbatch
        else:
            # Handle case where result is already Arrow data
            if isinstance(result, pa.Table):
                for batch in result.to_batches():
                    yield RecordBatch.from_arrow_record_batches([batch], batch.schema)._recordbatch
            elif isinstance(result, pa.RecordBatch):
                yield RecordBatch.from_arrow_record_batches([result], result.schema)._recordbatch
            else:
                # Try to convert to Arrow format
                arrow_table = pa.Table.from_pydict(result)
                for batch in arrow_table.to_batches():
                    yield RecordBatch.from_arrow_record_batches([batch], batch.schema)._recordbatch

    except Exception as e:
        logger.error("Failed to query Lance REST table %s/%s: %s", namespace, table_name, e)
        raise


def _lance_rest_count_function(
    rest_config: LanceRestConfig,
    namespace: str,
    table_name: str,
    required_column: str,
    filter_expr: str | None = None,
) -> Iterator[PyRecordBatch]:
    """Factory function for counting rows in REST-based Lance table."""
    if lance_namespace is None:
        raise ImportError(
            "Unable to import the `lance_namespace` package, please ensure it is installed: `pip install lance-namespace`"
        )

    # Create REST client
    client_config: dict[str, Any] = {"uri": rest_config.base_url}
    if rest_config.api_key:
        client_config["api_key"] = rest_config.api_key
    if rest_config.headers:
        client_config.update(rest_config.headers)

    client = lance_namespace.connect("rest", client_config)

    try:
        # Use count_rows endpoint if available, otherwise query with limit 0
        count_request = {
            "k": 0,  # Don't return actual data
            "vector": {"single_vector": [0.0]},  # Dummy vector
        }

        if filter_expr:
            count_request["filter"] = filter_expr

        result = client.count_rows(namespace, table_name, count_request)
        count = result if isinstance(result, int) else len(result)

        # Create Arrow batch with count result
        arrow_schema = pa.schema([pa.field(required_column, pa.uint64())])
        arrow_array = pa.array([count], type=pa.uint64())
        arrow_batch = pa.RecordBatch.from_arrays([arrow_array], [required_column])
        result_batch = RecordBatch.from_arrow_record_batches([arrow_batch], arrow_schema)._recordbatch
        yield result_batch

    except Exception as e:
        logger.error("Failed to count rows in Lance REST table %s/%s: %s", namespace, table_name, e)
        raise


class LanceRestScanOperator(ScanOperator, SupportsPushdownFilters):
    """Scan operator for Lance tables accessed via REST API."""

    def __init__(
        self,
        rest_config: LanceRestConfig,
        namespace: str,
        table_name: str,
        schema: Schema | None = None,
    ):
        self._rest_config = rest_config
        self._namespace = namespace
        self._table_name = table_name
        self._pushed_filters: list[PyExpr] | None = None
        self._remaining_filters: list[PyExpr] | None = None
        self._enable_strict_filter_pushdown = get_context().daft_planning_config.enable_strict_filter_pushdown

        # Get schema from REST API or use provided schema
        if schema is None:
            self._schema = self._fetch_table_schema()
        else:
            self._schema = schema

    def name(self) -> str:
        return "LanceRestScanOperator"

    def display_name(self) -> str:
        return f"LanceRestScanOperator({self._rest_config.base_url}/{self._namespace}/{self._table_name})"

    def schema(self) -> Schema:
        return self._schema

    def partitioning_keys(self) -> list[PyPartitionField]:
        return []

    def can_absorb_filter(self) -> bool:
        return isinstance(self, SupportsPushdownFilters)

    def can_absorb_limit(self) -> bool:
        return True

    def can_absorb_select(self) -> bool:
        return True

    def supports_count_pushdown(self) -> bool:
        """Returns whether this scan operator supports count pushdown."""
        return True

    def supported_count_modes(self) -> list[CountMode]:
        """Returns the count modes supported by this scan operator."""
        return [CountMode.All]

    def as_pushdown_filter(self) -> SupportsPushdownFilters | None:
        return self

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self.schema()}",
        ]

    def push_filters(self, filters: list[PyExpr]) -> tuple[list[PyExpr], list[PyExpr]]:
        """Push filters down to the REST API."""
        pushed = []
        remaining = []

        for expr in filters:
            try:
                # Try to convert to SQL-like filter expression for REST API
                Expression._from_pyexpr(expr).to_arrow_expr()
                pushed.append(expr)
            except NotImplementedError:
                remaining.append(expr)

        if pushed:
            self._pushed_filters = pushed
        else:
            self._pushed_filters = None

        self._remaining_filters = remaining if remaining else None

        return pushed, remaining

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        """Generate scan tasks for the REST-based Lance table."""
        required_columns: list[str] | None = None
        if pushdowns.columns is not None:
            filter_required_column_names = pushdowns.filter_required_column_names()
            required_columns = list(
                set(
                    pushdowns.columns
                    if filter_required_column_names is None
                    else pushdowns.columns + filter_required_column_names
                )
            )

        # Check if there is a count aggregation pushdown
        if (
            pushdowns.aggregation is not None
            and pushdowns.aggregation_count_mode() is not None
            and pushdowns.aggregation_required_column_names()
            and pushdowns.limit is None
        ):
            if pushdowns.aggregation_count_mode() not in self.supported_count_modes():
                logger.warning(
                    "Count mode %s is not supported for pushdown, falling back to original logic",
                    pushdowns.aggregation_count_mode(),
                )
                yield from self._create_regular_scan_tasks(pushdowns, required_columns)
            else:
                yield from self._create_count_rows_scan_task(pushdowns)
        else:
            yield from self._create_regular_scan_tasks(pushdowns, required_columns)

    def _create_count_rows_scan_task(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        """Create scan task for counting rows."""
        fields = pushdowns.aggregation_required_column_names()
        new_schema = Schema.from_pyarrow_schema(pa.schema([pa.field(fields[0], pa.uint64())]))

        filter_expr = self._convert_filters_to_sql()

        yield ScanTask.python_factory_func_scan_task(
            module=_lance_rest_count_function.__module__,
            func_name=_lance_rest_count_function.__name__,
            func_args=(self._rest_config, self._namespace, self._table_name, fields[0], filter_expr),
            schema=new_schema._schema,
            num_rows=1,
            size_bytes=None,
            pushdowns=pushdowns,
            stats=None,
            source_name=self.display_name(),
        )

    def _create_regular_scan_tasks(
        self, pushdowns: PyPushdowns, required_columns: list[str] | None
    ) -> Iterator[ScanTask]:
        """Create regular scan tasks for data retrieval."""
        query_params: dict[str, Any] = {}

        # Add column selection
        if required_columns:
            query_params["columns"] = required_columns

        # Add filters
        filter_expr = self._convert_filters_to_sql()
        if filter_expr:
            query_params["filter"] = filter_expr

        # Add limit
        if pushdowns.limit is not None:
            query_params["limit"] = pushdowns.limit

        yield ScanTask.python_factory_func_scan_task(
            module=_lance_rest_factory_function.__module__,
            func_name=_lance_rest_factory_function.__name__,
            func_args=(self._rest_config, self._namespace, self._table_name, query_params),
            schema=self.schema()._schema,
            num_rows=None,  # Unknown for REST
            size_bytes=None,  # Unknown for REST
            pushdowns=pushdowns,
            stats=None,
            source_name=self.display_name(),
        )

    def _fetch_table_schema(self) -> Schema:
        """Fetch table schema via REST API."""
        if lance_namespace is None:
            raise ImportError(
                "Unable to import the `lance_namespace` package, please ensure it is installed: `pip install lance-namespace`"
            )

        # Create REST client
        client_config: dict[str, Any] = {"uri": self._rest_config.base_url}
        if self._rest_config.api_key:
            client_config["api_key"] = self._rest_config.api_key
        if self._rest_config.headers:
            client_config.update(self._rest_config.headers)

        client = lance_namespace.connect("rest", client_config)

        try:
            # Get table information
            table_info = client.describe_table(self._namespace, self._table_name)

            if hasattr(table_info, "schema"):
                return Schema.from_pyarrow_schema(table_info.schema)
            else:
                # Fallback: try to infer schema from a small query
                query_request = {
                    "k": 1,
                    "vector": {"single_vector": [0.0]},
                }
                result = client.query_table(self._namespace, self._table_name, query_request)

                if hasattr(result, "schema"):
                    return Schema.from_pyarrow_schema(result.schema)
                elif isinstance(result, pa.Table):
                    return Schema.from_pyarrow_schema(result.schema)
                else:
                    # Create a basic schema as fallback
                    return Schema.from_pyarrow_schema(pa.schema([pa.field("data", pa.string())]))

        except Exception as e:
            logger.error("Failed to fetch schema for Lance REST table %s/%s: %s", self._namespace, self._table_name, e)
            # Return a basic schema as fallback
            return Schema.from_pyarrow_schema(pa.schema([pa.field("data", pa.string())]))

    def _convert_filters_to_sql(self) -> str | None:
        """Convert pushed filters to SQL expression for REST API."""
        if not self._pushed_filters:
            return None

        try:
            # Simple conversion - this would need to be more sophisticated in practice
            filter_parts = []
            for expr in self._pushed_filters:
                # Convert PyExpr to SQL-like string
                # This is a simplified implementation
                filter_parts.append(str(expr))

            return " AND ".join(filter_parts) if filter_parts else None
        except Exception as e:
            logger.warning("Failed to convert filters to SQL: %s", e)
            return None
