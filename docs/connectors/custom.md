# Reading from and Writing to Custom Connectors

This guide shows you how to create custom connectors to read from or write to data sources that aren't currently implemented in Daft, which includes proprietary data sources that can't be implemented in open source.

## Overview

Daft provides two main interfaces for custom connectors:

- [**Reading from a custom `DataSource`**](#reading-from-a-custom-data-source)
- [**Writing to a custom `DataSink`**](#writing-to-a-custom-data-sink)

You can also take a look at actual code references on how we implemented:

- [Reads from Video Frames](https://github.com/Eventual-Inc/Daft/blob/main/daft/io/av/_read_video_frames.py)
- [Writes to Hugging Face 🤗](https://github.com/Eventual-Inc/Daft/blob/main/daft/io/huggingface/sink.py)
- [More examples below](#more-examples)

## Reading from a Custom Data Source

### Step 1: Implement the `DataSource` and `DataSourceTask` Interfaces

Create a class that inherits from [`DataSource`](../api/io.md#daft.io.source.DataSource), a class that inherits from [`DataSourceTask`](../api/io.md#daft.io.source.DataSourceTask), and implement the required methods. Here's a simple example doing this with a custom local file reader that reads each line from a file as a single String row.

=== "🐍 Python"
```python
from collections.abc import AsyncIterator
from pathlib import Path

from daft.datatype import DataType
from daft.io import DataSource, DataSourceTask
from daft.io.pushdowns import Pushdowns
from daft.recordbatch import RecordBatch
from daft.schema import Schema


class TextFileDataSource(DataSource):
    """A simple data source that reads text files line by line.

    Each line in the text file becomes a row in the dataframe.
    """

    def __init__(self, file_paths: list[str]):
        """Initialize the text file data source.

        Args:
            file_paths: List of text file paths to read from
        """
        self.file_paths = [Path(path) for path in file_paths]

    @property
    def name(self) -> str:
        """Return a descriptive name for this source."""
        return "Text File Data Source"

    @property
    def schema(self) -> Schema:
        """Return the schema for the data.

        Since we're reading text files line by line, each row will have a single
        string column containing the line content.
        """
        return Schema._from_field_name_and_types([
            ("line", DataType.string()),
        ])

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator["TextFileDataSourceTask"]:
        """Create tasks for each file to enable parallel processing.

        Args:
            pushdowns: Pushdown optimizations (not used in this simple implementation)

        Yields:
            TextFileDataSourceTask: A task for each file
        """
        for file_path in self.file_paths:
            yield TextFileDataSourceTask(file_path)


class TextFileDataSourceTask(DataSourceTask):
    """A task that reads a single text file and converts it to RecordBatches."""

    def __init__(self, file_path: Path):
        """Initialize the task with a specific file path.

        Args:
            file_path: Path to the text file to read
        """
        self.file_path = file_path

    @property
    def schema(self) -> Schema:
        """Return the schema for this task's data."""
        return Schema._from_field_name_and_types([
            ("line", DataType.string()),
        ])

    async def read(self) -> AsyncIterator[RecordBatch]:
        """Read the text file and yield RecordBatches.

        This method reads the file line by line and creates RecordBatches
        containing the line data.

        Yields:
            RecordBatch: Contains the lines from the text file
        """
        lines = []
        with open(self.file_path, encoding='utf-8') as f:
            for line in f:
                lines.append(line)

        # Create a single RecordBatch with all lines.
        yield RecordBatch.from_pydict({
            "line": lines,
        })
```

### Step 2: Use Your Custom Data Source to Create a Daft DataFrame

=== "🐍 Python"
```python
import daft

# Create a sample text file and read from it.
sample_file = "sample_text.txt"
with open(sample_file, "w") as f:
    f.write("Alice was beginning to get very tired of sitting by her sister on the bank.\n")
    f.write("So she was considering in her own mind (as well as she could, for the hot day made her feel very sleepy and stupid),\n")
    f.write("when suddenly a White Rabbit with pink eyes ran close by her.\n")
    f.write("There was nothing so very remarkable in that;\n")
    f.write("nor did Alice think it so very much out of the way to hear the Rabbit say to itself, 'Oh dear! Oh dear! I shall be late!'\n")

data_source = TextFileDataSource([sample_file])

(
    data_source
    .read()
    .show()
)
```

**Output:**

```bash
╭────────────────────────────────╮
│ line                           │
│ ---                            │
│ Utf8                           │
╞════════════════════════════════╡
│ Alice was beginning to get ve… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ So she was considering in her… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ when suddenly a White Rabbit … │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ There was nothing so very rem… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ nor did Alice think it so ver… │
╰────────────────────────────────╯

(Showing first 5 of 5 rows)
```

### Optional: Applying Pushdowns

Daft passes a [`Pushdowns`](../api/io.md#daft.io.pushdowns.Pushdowns) object to `get_tasks` describing the work your source can skip. Every field is optional:

| Field | Description |
| --- | --- |
| `columns` | Columns the query needs. Reading only these saves I/O. |
| `filters` | Predicate applied to rows. Use it to prune files, row groups, or tasks. |
| `partition_filters` | Predicate resolved against partition values. |
| `limit` | Maximum number of rows the query needs. |
| `aggregation` | Count aggregation, for sources that implement count pushdowns. |

Pushdowns are advisory: Daft always applies projections, filters, and limits after reading, so honoring a subset (or ignoring them entirely) never changes results. It only changes how much work your source does. Any pruning must therefore be conservative: skip a task only when the filter proves it cannot contain a matching row.

Filters are regular Daft [`Expression`s](../api/expressions.md), so you can inspect them with [`ExpressionVisitor`](../api/expressions.md#daft.expressions.visitor.ExpressionVisitor). The visitor dispatches each node to a `visit_*` method and falls back to `visit_function(name, args)`. This visitor extracts `column == literal` conjuncts, which is enough to prune parts by an equality predicate:

=== "🐍 Python"
```python
from typing import Any

from daft.expressions import Expression, ExpressionVisitor


class EqualityConstraints(ExpressionVisitor[dict[str, Any]]):
    """Collect ``column == literal`` conjuncts from a filter expression."""

    def visit_col(self, name: str) -> dict[str, Any]:
        return {}

    def visit_lit(self, value: Any) -> dict[str, Any]:
        return {}

    def visit_alias(self, expr: Expression, alias: str) -> dict[str, Any]:
        return self.visit(expr)

    def visit_cast(self, expr: Expression, dtype: Any) -> dict[str, Any]:
        return {}

    def visit_coalesce(self, args: list[Expression]) -> dict[str, Any]:
        return {}

    def visit_function(self, name: str, args: list[Expression]) -> dict[str, Any]:
        if name == "and" and len(args) == 2:
            return {**self.visit(args[0]), **self.visit(args[1])}
        if name == "equal" and len(args) == 2:
            left, right = args
            if (column := left.column_name()) is not None and right.column_name() is None:
                return {column: right.as_py()}
            if (column := right.column_name()) is not None and left.column_name() is None:
                return {column: left.as_py()}
        return {}
```

When projecting, remember that a filter may reference a column the query does not return; `pushdowns.filter_required_column_names()` lists those columns so they are not dropped. Putting it together, this in-memory source prunes parts, projects columns, and truncates each task:

=== "🐍 Python"
```python
from collections.abc import AsyncIterator

import pyarrow as pa

from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema


class InMemoryTableTask(DataSourceTask):
    def __init__(self, table: pa.Table, columns: list[str] | None, limit: int | None):
        self._table = table
        self._columns = columns
        self._limit = limit

    @property
    def schema(self) -> Schema:
        table = self._table if self._columns is None else self._table.select(self._columns)
        return Schema.from_pyarrow_schema(table.schema)

    async def read(self) -> AsyncIterator[RecordBatch]:
        table = self._table
        if self._columns is not None:
            table = table.select(self._columns)
        if self._limit is not None:
            table = table.slice(0, self._limit)
        yield RecordBatch.from_arrow_table(table)


class InMemoryTableSource(DataSource):
    """A source over pyarrow tables keyed by a `year` column."""

    def __init__(self, parts: dict[int, pa.Table]):
        self._parts = parts

    @property
    def name(self) -> str:
        return "in-memory table source"

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(next(iter(self._parts.values())).schema)

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        columns = pushdowns.columns
        if columns is not None:
            # Keep columns that the filter needs even if the query does not return them.
            columns = list(dict.fromkeys([*columns, *pushdowns.filter_required_column_names()]))
        constraints = EqualityConstraints().visit(pushdowns.filters) if pushdowns.filters is not None else {}
        for year, table in self._parts.items():
            # Only skip a part when the filter proves its `year` cannot match.
            if constraints.get("year", year) != year:
                continue
            yield InMemoryTableTask(table, columns, pushdowns.limit)
```

=== "🐍 Python"
```python
import pyarrow as pa
from daft import col

parts = {
    2023: pa.table({"year": [2023] * 100, "id": list(range(100))}),
    2024: pa.table({"year": [2024] * 100, "id": list(range(100, 200))}),
}

df = (
    InMemoryTableSource(parts)
    .read()
    .where(col("year") == 2024)  # reaches get_tasks, so the 2023 part is never read
    .select("id")
    .limit(3)
)
df.show()
```

**Output:**

```bash
╭───────╮
│ id    │
│ ---   │
│ Int64 │
╞═══════╡
│ 100   │
├╌╌╌╌╌╌╌┤
│ 101   │
├╌╌╌╌╌╌╌┤
│ 102   │
╰───────╯

(Showing first 3 of 3 rows)
```

### Optional: Declaring Clustering to Skip Shuffles

Clustering is an *execution-time* property: it describes how rows are distributed across the
in-memory partitions a query runs over. It is distinct from *storage* partitioning (the on-disk
layout declared via `get_partition_fields()`, e.g. Hive/Iceberg directories) — a source can be laid
out one way on disk yet emit partitions clustered another way.

Two clustering hints are available:

#### Hash clustering

If your source already emits data that is hash-partitioned by some keys — for example, each
[`DataSourceTask`](../api/io.md#daft.io.source.DataSourceTask) corresponds to exactly one
`(producer, hour)` group — you can tell Daft by overriding `get_clustering_keys()`. Daft then
skips the shuffle it would otherwise insert before a downstream `groupby`, `Window.partition_by`,
or `distinct` whose keys are *covered by* (equal to, or a superset of) the declared clustering.

```python
from daft import col
from daft.io.clustering import ClusteringKeys
from daft.io.source import DataSource


class ClusteredSource(DataSource):
    # ... name / schema / get_tasks as above ...

    def get_clustering_keys(self) -> ClusteringKeys | None:
        # Each task emits exactly one (a, b) group, so the output is hash-partitioned by (a, b).
        return ClusteringKeys.hash("a", "b")
```

Keys may be column names or arbitrary [`Expression`](../api/expressions.md)s. An expression-valued
key follows a projection that materializes it as a derived column, so the clustering is preserved
even after a `with_column`:

```python
def hour_bucket(ts: "daft.Expression") -> "daft.Expression":
    # bucket a unix-epoch timestamp (in seconds) into hourly buckets
    return ts // 3600


class EventSource(DataSource):
    def get_clustering_keys(self) -> ClusteringKeys | None:
        return ClusteringKeys.hash(col("producer"), hour_bucket(col("ts")))


df = (
    EventSource().read()
    .with_column("hour", hour_bucket(col("ts")))  # materialize the expression key as a column
    .groupby("producer", "hour")                   # covered by the declared clustering => no shuffle
    .sum("value")
)
```

#### Range clustering

If your source already emits data where each task covers a non-overlapping range of values for
those columns, you can tell Daft by returning `ClusteringKeys.range()`. Pass `descending=True`
if partitions are ordered from highest to lowest values. If any key values are null, declare
where those rows live with `nulls_first` (default: nulls in the last partition when ascending,
the first when descending, matching sort semantics).

```python
class TimeSeriesSource(DataSource):
    # ... name / schema / get_tasks as above ...

    def get_clustering_keys(self) -> ClusteringKeys | None:
        # Each task holds a non-overlapping ts range (ascending partition order).
        return ClusteringKeys.range("ts")

    # Or, if partitions are ordered high-to-low:
    # return ClusteringKeys.range("ts", descending=True)
```

!!! note "Shuffle elision applies to the distributed runner"

    Daft only inserts these shuffles when running distributed (e.g. on Ray); the single-node
    runner already executes the operators locally. The declaration is therefore a no-op for local
    execution and only changes plans for distributed runs.

!!! warning "Clustering must hold for every task"

    Daft trusts the declaration. For hash clustering, every row with the same hash of the declared
    keys must be produced within a single task. For range clustering, each task must cover a
    genuinely non-overlapping range of values for the declared columns. Incorrectly overriding
    `get_clustering_keys()` will likely lead to incorrect results.

## Writing to a Custom Data Sink

### Step 1: Implement the `DataSink` Interface

Create a class that inherits from [`DataSink`](../api/io.md#daft.io.sink.DataSink) and implements the required methods. Here's a simple example doing this with a custom local file writer.

=== "🐍 Python"
```python
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from daft.datatype import DataType
from daft.io import DataSink
from daft.io.sink import WriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema


class LocalFileDataSink(DataSink[dict]):
    """A simple data sink that writes data to local files."""

    def __init__(
        self,
        output_dir: str | Path,
        filename_prefix: str = "data",
        max_rows_per_file: int = 10
    ):
        """Initialize the local file data sink.

        Args:
            output_dir: Directory where files will be written
            filename_prefix: Prefix for generated filenames
            max_rows_per_file: Maximum rows to write per file
        """
        self.output_dir = Path(output_dir)
        self.filename_prefix = filename_prefix
        self.max_rows_per_file = max_rows_per_file

        self.output_dir.mkdir(parents=True, exist_ok=True)

        self._result_schema = Schema._from_field_name_and_types([
            ("files_written", DataType.int64()),
            ("total_rows", DataType.int64()),
            ("total_bytes", DataType.int64()),
            ("output_directory", DataType.string()),
        ])

    def name(self) -> str:
        """Return a descriptive name for this sink."""
        return "Local File Data Sink"

    def schema(self) -> Schema:
        """Return the schema for the results of finalize()."""
        return self._result_schema

    def start(self) -> None:
        """Called once at the beginning of the write process.

        This is a good place to initialize resources, create directories,
        start a transaction, etc.
        """
        print(f"Starting write to {self.output_dir}")
        print(f"Max rows per file: {self.max_rows_per_file}")

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[dict]]:
        """Process each micropartition and write to local files.

        This method is called for each micropartition and should yield
        WriteResults. When run in a distributed setting, this method is called
        in parallel on each worker.

        Args:
            micropartitions: Iterator of micropartitions to process

        Yields:
            WriteResult: Information about each write operation
        """
        for micropartition in micropartitions:
            # Convert micropartition to a format we can write.
            data = self._prepare_data_for_writing(micropartition)

            # Write data to files (potentially multiple files if data is large).
            # Split data into chunks based on max_rows_per_file.
            for i in range(0, len(data), self.max_rows_per_file):
                chunk = data[i:i + self.max_rows_per_file]
                write_result = self._write_data_to_files(chunk)
                 # Yield results for each file written.
                yield WriteResult(
                    result=write_result,
                    bytes_written=write_result["bytes_written"],
                    rows_written=write_result["rows_written"]
                )

    def finalize(self, write_results: list[WriteResult[dict]]) -> MicroPartition:
        """Aggregate all write results into a final summary.

        This method is called after all writes complete and should
        return a single MicroPartition with summary information.

        This is a good place to commit a transaction, clean up resources, etc.

        Args:
            write_results: List of all WriteResult objects from write()

        Returns:
            MicroPartition: Summary of all write operations
        """
        if not write_results:
            return MicroPartition.empty(self._result_schema)

        total_files = len(write_results)
        total_rows = sum(wr.rows_written for wr in write_results)
        total_bytes = sum(wr.bytes_written for wr in write_results)

        print(f"Write completed: {total_files} files, {total_rows} rows, {total_bytes} bytes")

        return MicroPartition.from_pydict({
            "files_written": [total_files],
            "total_rows": [total_rows],
            "total_bytes": [total_bytes],
            "output_directory": [str(self.output_dir)],
        })

    def _prepare_data_for_writing(self, micropartition: MicroPartition) -> list[dict]:
        """Convert a micropartition to a list of dictionaries for writing.

        Args:
            micropartition: The data to prepare

        Returns:
            List of dictionaries representing the data
        """
        return micropartition.to_pylist()

    def _write_data_to_files(self, data: list[Any]) -> dict:
        """Write data to one or more files based on size limits.

        Args:
            data: List of data to write

        Returns:
            List of write result dictionaries
        """
        filepath = str(self.output_dir) + "/" + self.filename_prefix + "_" + str(uuid.uuid4()) + ".txt"
        with open(filepath, 'w') as f:
            for row in data:
                f.write(str(row) + '\n')

        return {
            "result": "success",
            "rows_written": len(data),
            "bytes_written": Path(filepath).stat().st_size,
        }
```

### Step 2: Use Your Custom Data Sink

=== "🐍 Python"
```python
import daft
from custom_datasink_example import LocalFileDataSink


data = {
    "id": list(range(1, 1001)),
    "name": [f"User_{i}" for i in range(1, 1001)],
}

local_file_data_sink = LocalFileDataSink(
    output_dir="./output_folder",
    filename_prefix="users",
    max_rows_per_file=10
)

(
    daft.from_pydict(data)
    .write_sink(local_file_data_sink)
    .show()
)
```

**Sample File Output:**

```
{'id': 111, 'name': 'User_111'}
{'id': 112, 'name': 'User_112'}
{'id': 113, 'name': 'User_113'}
{'id': 114, 'name': 'User_114'}
{'id': 115, 'name': 'User_115'}
{'id': 116, 'name': 'User_116'}
{'id': 117, 'name': 'User_117'}
{'id': 118, 'name': 'User_118'}
{'id': 119, 'name': 'User_119'}
{'id': 120, 'name': 'User_120'}
```

## More Examples

For further reference, feel free to check out some of our notable data connectors that are implemented using the `DataSource` and `DataSink` interfaces:

- [Reads from Video Frames](https://github.com/Eventual-Inc/Daft/blob/main/daft/io/av/_read_video_frames.py)
- [Reads from LanceDB](https://github.com/daft-engine/daft-lance/blob/main/daft_lance/lance_scan.py)
- [Writes to Hugging Face 🤗](https://github.com/Eventual-Inc/Daft/blob/main/daft/io/huggingface/sink.py)
- [Writes to ClickHouse](https://github.com/Eventual-Inc/Daft/blob/main/daft/io/clickhouse/clickhouse_data_sink.py)
- [Writes to Turbopuffer](https://github.com/Eventual-Inc/Daft/blob/main/daft/io/turbopuffer/turbopuffer_data_sink.py)
- [Writes to LanceDB](https://github.com/daft-engine/daft-lance/blob/main/daft_lance/lance_data_sink.py)
