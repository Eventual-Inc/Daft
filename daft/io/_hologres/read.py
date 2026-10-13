"""Public API: create a DataFrame from a Hologres table via Arrow COPY reads."""

from __future__ import annotations

from daft import context
from daft.api_annotations import PublicAPI
from daft.catalog import Identifier
from daft.daft import ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.io._hologres._schema_loader import load_table_schema, table_exists
from daft.io._hologres.scan import HologresScanOperator
from daft.logical.builder import LogicalPlanBuilder
from daft.sql.sql_scan import PartitionBoundStrategy


@PublicAPI
def read_hologres(
    table: str | Identifier,
    conn: str,
    *,
    read_mode: str = "arrow",
    partition_col: str | None = None,
    num_partitions: int | None = None,
    partition_bound_strategy: str = "min-max",
    shard_parallelism: bool = True,
    disable_pushdowns: bool = False,
) -> DataFrame:
    """Create a DataFrame from a Hologres table.

    Reads use Hologres' ``COPY (...) TO STDOUT WITH (FORMAT ARROW_LZ4)`` export
    for high-throughput Arrow decoding, with Daft pushdowns (projection,
    predicate, limit) applied to the inner query. Reads are parallelized by
    physical shard (``hg_shard_id``) or by ``partition_col`` bounds.

    Args:
        table (str | Identifier): table name, ``"schema.table"`` or ``"table"``.
        conn (str): PostgreSQL connection string pointing at the Hologres instance.
        read_mode (str): ``"arrow"`` (default) for the COPY Arrow export, or
            ``"select"`` for the generic connectorx/SQLAlchemy path. The arrow
            path cannot decode ``json``/``jsonb`` columns and automatically
            falls back to ``"select"`` when they are projected.
        partition_col (Optional[str]): column used to split the read into
            parallel tasks via min-max or percentile bounds.
        num_partitions (Optional[int]): number of parallel read tasks.
        partition_bound_strategy (str): ``"min-max"`` (default) or ``"percentile"``.
        shard_parallelism (bool): split reads by physical shard when the table
            has more than one shard; on by default.
        disable_pushdowns (bool): disable pushing filters/projections into the query.

    Returns:
        DataFrame: DataFrame with the table's contents.
    """
    if num_partitions is not None and partition_col is None:
        raise ValueError("partition_col must be specified when num_partitions is specified")

    connection_string = conn
    identifier = Identifier.from_sql(str(table)) if isinstance(table, str) else table
    if len(identifier) == 1:
        schema_name, table_name = "public", identifier[0]
    elif len(identifier) == 2:
        schema_name, table_name = identifier[0], identifier[1]
    else:
        raise ValueError(f"Invalid Hologres table identifier: {identifier}")

    import psycopg

    conn_obj = psycopg.connect(connection_string, autocommit=True)
    try:
        if not table_exists(conn_obj, schema_name, table_name):
            raise ValueError(f"Hologres table {schema_name}.{table_name} not found")
        holo_schema = load_table_schema(conn_obj, schema_name, table_name)
    finally:
        conn_obj.close()

    io_config = context.get_context().daft_planning_config.default_io_config
    storage_config = StorageConfig(True, io_config)

    operator = HologresScanOperator(
        connection_uri=connection_string,
        holo_schema=holo_schema,
        storage_config=storage_config,
        disable_pushdowns=disable_pushdowns,
        read_mode=read_mode,
        shard_parallelism=shard_parallelism,
        partition_col=partition_col,
        num_partitions=num_partitions,
        partition_bound_strategy=PartitionBoundStrategy.from_str(partition_bound_strategy),
    )
    handle = ScanOperatorHandle.from_python_scan_operator(operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)
