"""DataSource for Iceberg COW changelog (CDC) reads."""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any

from pyiceberg.schema import visit

import daft
from daft.daft import ParquetSourceConfig, StorageConfig
from daft.dependencies import pa
from daft.io.iceberg._changelog_planning import (
    ChangelogFileTask,
    compute_snapshot_ordinals,
    ordered_changelog_snapshots,
    plan_changelog_file_tasks,
    resolve_snapshot_range,
)
from daft.io.iceberg._changelog_schema import validate_single_schema_table, validate_task_file_schemas
from daft.io.iceberg._metadata import convert_iceberg_schema
from daft.io.iceberg.iceberg_scan import iceberg_partition_spec_to_fields
from daft.io.iceberg.schema_field_id_mapping_visitor import SchemaFieldIdMappingVisitor
from daft.io.source import DataSource, DataSourceTask
from daft.logical.schema import DataType, Schema
from daft.recordbatch import RecordBatch

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.table import Table

    from daft.io.partitioning import PartitionField
    from daft.io.pushdowns import Pushdowns

logger = logging.getLogger(__name__)

# Reserved changelog metadata column names -- match the field names reserved by Iceberg's
# Spark/core changelog integration for the same purpose.
CHANGE_TYPE_COLUMN = "_change_type"
CHANGE_ORDINAL_COLUMN = "_change_ordinal"
COMMIT_SNAPSHOT_ID_COLUMN = "_commit_snapshot_id"
_RESERVED_COLUMN_NAMES = (CHANGE_TYPE_COLUMN, CHANGE_ORDINAL_COLUMN, COMMIT_SNAPSHOT_ID_COLUMN)

_CHANGELOG_METADATA_SCHEMA = Schema.from_pydict(
    {
        CHANGE_TYPE_COLUMN: DataType.string(),
        CHANGE_ORDINAL_COLUMN: DataType.int64(),
        COMMIT_SNAPSHOT_ID_COLUMN: DataType.int64(),
    }
)


def _check_no_reserved_data_schema_collision(baseline_iceberg_schema: IcebergSchema) -> None:
    """Raise if the reserved changelog column names collide with the baseline data schema.

    Cheap, metadata-only, and independent of which snapshot range/spec_ids the eventual scan
    touches, so this runs eagerly at operator construction time.
    """
    data_field_names = {f.name for f in baseline_iceberg_schema.fields}
    collisions = data_field_names & set(_RESERVED_COLUMN_NAMES)
    if collisions:
        raise ValueError(
            f"Iceberg table schema contains column(s) {sorted(collisions)} that collide with "
            f"daft.io.iceberg.read_iceberg_changes()'s reserved changelog metadata column names "
            f"{_RESERVED_COLUMN_NAMES}; rename the conflicting column(s) in the table before "
            "using read_iceberg_changes()."
        )


def _check_no_reserved_partition_field_collision(table: Table, spec_ids: set[int]) -> None:
    """Raise if the reserved changelog column names collide with any of `spec_ids`' partition fields.

    `spec_ids` must be exactly the set of `spec_id`s referenced by *this scan's own planned
    tasks*, not the
    table's entire historical spec set -- a spec that predates or postdates the requested
    range and was never actually read from should not be able to reject an otherwise-valid
    scan. This is why the call site is the lazy planning path (`_get_or_plan_tasks`, after
    `plan_changelog_file_tasks` has produced the task list), not `__init__`: which specs are
    involved isn't knowable before planning runs.
    """
    for spec_id in spec_ids:
        spec = table.specs()[spec_id]
        partition_field_names = {f.name for f in spec.fields}
        collisions = partition_field_names & set(_RESERVED_COLUMN_NAMES)
        if collisions:
            raise ValueError(
                f"Iceberg table has a partition spec (spec_id={spec.spec_id}) containing "
                f"partition field(s) {sorted(collisions)} that collide with "
                f"daft.io.iceberg.read_iceberg_changes()'s reserved changelog metadata column names "
                f"{_RESERVED_COLUMN_NAMES}; rename the conflicting partition field(s) before "
                "using read_iceberg_changes()."
            )


class IcebergChangesDataSource(DataSource):
    """Scans a copy-on-write Iceberg snapshot range as a changelog (CDC) stream.

    Every row produced carries three additional columns: `_change_type` (INSERT or DELETE),
    `_change_ordinal` (int64, scan-relative), and `_commit_snapshot_id` (int64). Planning
    (snapshot range resolution, manifest listing, and per-file Parquet-footer schema
    validation) is deferred to the first `get_tasks()` call and cached for the lifetime
    of this source instance -- not done eagerly in `__init__`, so constructing this
    source (and therefore building the `read_iceberg_changes()` DataFrame) never triggers
    I/O on its own.
    """

    def __init__(
        self,
        iceberg_table: Table,
        *,
        start_snapshot_id: int | None,
        end_snapshot_id: int | None,
        start_timestamp_ms: int | None,
        end_timestamp_ms: int | None,
        storage_config: StorageConfig,
    ) -> None:
        self._iceberg_table = iceberg_table
        self._start_snapshot_id = start_snapshot_id
        self._end_snapshot_id = end_snapshot_id
        self._start_timestamp_ms = start_timestamp_ms
        self._end_timestamp_ms = end_timestamp_ms
        self._storage_config = storage_config

        # Cheap, metadata-only work: safe to do eagerly, no I/O beyond what loading the
        # Table object already required.
        baseline_iceberg_schema = validate_single_schema_table(iceberg_table)
        self._baseline_iceberg_schema = baseline_iceberg_schema
        self._format_version = iceberg_table.metadata.format_version

        _check_no_reserved_data_schema_collision(baseline_iceberg_schema)

        field_id_mapping = visit(baseline_iceberg_schema, SchemaFieldIdMappingVisitor())
        self._parquet_config = ParquetSourceConfig(field_id_mapping=field_id_mapping)
        self._schema = convert_iceberg_schema(baseline_iceberg_schema).union(_CHANGELOG_METADATA_SCHEMA)

        # Planning + footer-validation cache. UNPLANNED (_cached_tasks is None) ->
        # PLANNING (inside the lock, doing planning + footer I/O) -> VALIDATED
        # (_cached_tasks populated, only after every file passed schema validation).
        # Any failure during planning leaves _cached_tasks as None, i.e. back to
        # UNPLANNED, so the next call retries from scratch rather than caching a failure.
        #
        # get_tasks() below is `async def`, but this lock is a plain threading.Lock, not an
        # asyncio.Lock: planning itself (manifest listing + per-file Parquet footer
        # validation) is synchronous I/O with no async concurrency of its own, and Daft's
        # Rust engine can still invoke get_tasks() more than once for the same DataSource
        # instance (e.g. once from the shard-scan optimizer rule, once when translating the
        # physical plan) -- each invocation runs inside its own
        # io_runtime.block_on_current_thread(...), so separate calls aren't guaranteed to
        # share one thread or event loop the way an asyncio.Lock would require. A plain
        # threading.Lock is what actually guards "plan + validate exactly once" here; see
        # test_concurrent_get_tasks_reads_each_footer_only_once for the regression test that
        # exercises this from multiple OS threads.
        self._lock = threading.Lock()
        self._cached_tasks: list[ChangelogFileTask] | None = None

    def __getstate__(self) -> dict[str, Any]:
        # A plain threading.Lock isn't picklable, and this operator may be pickled to a
        # remote execution environment (e.g. the Ray runner). _cached_tasks (plain data)
        # is fine to carry across -- if planning already completed on the sender, the
        # receiver gets to skip redoing it; only the lock itself needs to be rebuilt fresh.
        state = self.__dict__.copy()
        state.pop("_lock", None)
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.Lock()

    @property
    def schema(self) -> Schema:
        return self._schema

    @property
    def name(self) -> str:
        return f"IcebergChangesDataSource({'.'.join(self._iceberg_table.name())})"

    def get_partition_fields(self) -> list[PartitionField]:
        # A changelog range can span multiple historical partition specs;
        # rather than picking one arbitrarily (which would misrepresent the others),
        # report no partitioning fields.
        return []

    def _get_or_plan_tasks(self) -> list[ChangelogFileTask]:
        with self._lock:
            if self._cached_tasks is not None:
                return self._cached_tasks

            from_id, to_id = resolve_snapshot_range(
                self._iceberg_table,
                self._start_snapshot_id,
                self._end_snapshot_id,
                self._start_timestamp_ms,
                self._end_timestamp_ms,
            )
            snapshots = ordered_changelog_snapshots(self._iceberg_table, from_id, to_id)
            ordinals = compute_snapshot_ordinals(snapshots)
            tasks = list(plan_changelog_file_tasks(self._iceberg_table, snapshots, ordinals))

            # Scoped to exactly the spec_ids this scan's own tasks reference -- not the
            # table's entire historical spec set, so a conflicting spec
            # that predates or postdates this range and was never actually read from
            # cannot reject an otherwise-valid scan.
            _check_no_reserved_partition_field_collision(self._iceberg_table, {task.spec_id for task in tasks})

            # The schema safety gate reads every distinct data file's
            # Parquet footer via self._storage_config, the same I/O configuration the
            # DataSourceTasks below will use. Raises if anything is unsafe; _cached_tasks stays
            # None (UNPLANNED) so a retry re-plans from scratch rather than reusing a
            # partial/failed result.
            validate_task_file_schemas(
                self._baseline_iceberg_schema,
                self._format_version,
                self._storage_config,
                tasks,
            )

            self._cached_tasks = tasks
            return tasks

    def _changelog_partition_values(self, task: ChangelogFileTask) -> RecordBatch:
        """Build the single-row constant-column batch for one changelog file task.

        Combines the file's historical partition values (looked up by its own spec_id, not
        the table's current spec -- a changelog range can span multiple historical specs)
        with the three CDC metadata columns. Reuses the same `partition_values` mechanism
        the existing `IcebergScanOperator` uses to inject partition column values (design
        §2) -- Daft's `PartitionSpec.to_fill_map()` matches columns by name, independent of
        whether they're "partition columns" in the traditional sense.
        """
        spec = self._iceberg_table.specs()[task.spec_id]
        partition_fields = iceberg_partition_spec_to_fields(self._baseline_iceberg_schema, spec)
        partition_record = task.data_file.partition

        arrays: dict[str, daft.Series] = {}
        for idx, pfield in enumerate(partition_fields):
            field = pfield.field
            arrow_type = field.dtype.to_arrow_dtype()
            arrays[field.name] = daft.Series.from_arrow(
                pa.array([partition_record[idx]], type=arrow_type), name=field.name
            ).cast(field.dtype)

        arrays[CHANGE_TYPE_COLUMN] = daft.Series.from_arrow(
            pa.array([task.change_type], type=pa.string()), name=CHANGE_TYPE_COLUMN
        )
        arrays[CHANGE_ORDINAL_COLUMN] = daft.Series.from_arrow(
            pa.array([task.change_ordinal], type=pa.int64()), name=CHANGE_ORDINAL_COLUMN
        )
        arrays[COMMIT_SNAPSHOT_ID_COLUMN] = daft.Series.from_arrow(
            pa.array([task.commit_snapshot_id], type=pa.int64()), name=COMMIT_SNAPSHOT_ID_COLUMN
        )
        return RecordBatch.from_pydict(arrays)

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        tasks = self._get_or_plan_tasks()
        for task in tasks:
            partition_values = self._changelog_partition_values(task)
            yield DataSourceTask.parquet(
                path=task.data_file.file_path,
                schema=self._schema,
                parquet_config=self._parquet_config,
                pushdowns=pushdowns,
                num_rows=task.data_file.record_count,
                size_bytes=task.data_file.file_size_in_bytes,
                partition_values=partition_values,
                storage_config=self._storage_config,
                iceberg_delete_files=None,
            )
