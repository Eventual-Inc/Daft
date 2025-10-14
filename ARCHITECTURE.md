# Daft Architecture Report

**Generated:** October 2025
**Version:** 0.3.0-dev0
**Audience:** Developers working on the Daft codebase

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Architecture Overview](#architecture-overview)
3. [Python API Layer](#python-api-layer)
4. [Rust Implementation Layer](#rust-implementation-layer)
5. [Python-Rust Interface (PyO3)](#python-rust-interface-pyo3)
6. [Data Flow and Query Execution](#data-flow-and-query-execution)
7. [Key Abstractions](#key-abstractions)
8. [Developer Guide](#developer-guide)

---

## Executive Summary

**Daft** is a distributed dataframe library for large-scale data processing in Python, with its execution engine implemented entirely in Rust. The architecture follows a clean separation between user-facing Python APIs and high-performance Rust implementation, connected via PyO3 bindings.

### Key Architectural Characteristics

- **Lazy Evaluation**: Operations build a logical query plan, executed only when materialized
- **Query Optimization**: Rule-based optimizer rewrites plans for efficiency
- **Multimodal Support**: First-class support for images, tensors, embeddings, and nested data
- **Dual Execution**: Native (multi-threaded) and Ray (distributed) runners
- **MicroPartition-based**: Data distributed as MicroPartitions (collections of RecordBatches)
- **Apache Arrow**: Memory representation based on Arrow for zero-copy interchange

### Code Organization

```
Daft/
├── daft/                    # Python API (~53 modules)
│   ├── dataframe/           # DataFrame API
│   ├── expressions/         # Expression DSL
│   ├── io/                  # Data I/O functions
│   ├── series.py            # Series API
│   └── ...
├── src/                     # Rust implementation (~55 crates)
│   ├── daft-core/           # Core data structures
│   ├── daft-dsl/            # Expression DSL
│   ├── daft-logical-plan/   # Logical query plans
│   ├── daft-local-execution/# Execution engine
│   └── ...
└── tests/                   # Python tests
```

### Repository Statistics

- **Rust crates**: 55
- **Python modules**: 53
- **Lines of Python API**: ~20,000 (estimated)
- **Lines of Rust code**: ~300,000 (estimated)

---

## Architecture Overview

### Layered Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    PYTHON API LAYER                         │
│  DataFrame | Expression | Series | I/O | UDF | Context     │
│  (User-facing API with Pandas/Polars-like interface)       │
└─────────────────────────────────────────────────────────────┘
                            ↓ PyO3 Bindings
┌─────────────────────────────────────────────────────────────┐
│                  LOGICAL PLAN LAYER                         │
│  LogicalPlanBuilder → LogicalPlan → Optimizer → Optimized  │
│  (Query construction and optimization)                      │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                 PHYSICAL PLAN LAYER                         │
│  PhysicalPlan → LocalPlan                                   │
│  (Execution strategy and algorithm selection)               │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                  EXECUTION LAYER                            │
│  LocalExecution Engine | Ray Runner                         │
│  (Streaming pipeline execution)                             │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                     DATA LAYER                              │
│  MicroPartition → RecordBatch → Series → DataArray         │
│  (In-memory data representation)                            │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                      I/O LAYER                              │
│  daft-io | daft-scan | daft-{parquet,csv,json,...}         │
│  (File formats and cloud storage)                           │
└─────────────────────────────────────────────────────────────┘
```

---

## Python API Layer

The Python API is the user-facing interface, exposing a familiar dataframe API similar to Pandas and Polars.

### 1. DataFrame API

**Location**: `daft/dataframe/dataframe.py` (~4,836 lines)

**Purpose**: Main user interface for data manipulation and query construction.

**Internal State**:
```python
class DataFrame:
    __builder: LogicalPlanBuilder      # Wrapped Rust logical plan
    _result_cache: Optional[PartitionCacheEntry]  # Execution cache
    _preview: Preview                  # Display preview
```

**Key Method Groups**:

1. **Construction** (lines 107-129):
   - Takes a `LogicalPlanBuilder` as core component
   - Stores execution cache for memoization

2. **Data Selection** (lines 330-650):
   - `select(*columns)` - Column projection
   - `filter(predicate)` - Row filtering
   - `with_column(name, expr)` - Add/replace column
   - `with_columns(dict)` - Multiple column operations
   - `exclude(*columns)` - Column removal

3. **Transformations** (lines 650-1200):
   - `explode(*columns)` - Unnest lists/structs
   - `sort(*keys)` - Sorting
   - `limit(n)` - Limit rows
   - `distinct()` - Deduplication
   - `unpivot()` - Wide to long transformation

4. **Aggregations** (lines 1200-1800):
   - `groupby(*keys).agg(...)` - Group-by aggregation
   - `pivot(group, pivot, value, agg)` - Pivot tables
   - `agg(*exprs)` - Global aggregation

5. **Joins & Set Ops** (lines 1800-2400):
   - `join(other, on, how)` - Join operations
   - `concat(other)` - Vertical concatenation
   - `union(other)` - Set union
   - `intersect(other)` - Set intersection

6. **Execution & Output** (lines 2400-3500):
   - `collect()` - Materialize to memory
   - `show(n)` - Display preview
   - `iter_rows()`, `iter_partitions()` - Streaming iteration
   - `write_parquet()`, `write_csv()`, etc. - File output
   - `to_pandas()`, `to_arrow()` - Conversion

7. **Metadata** (lines 3500-4000):
   - `schema` property - Get output schema
   - `explain(show_all)` - Query plan visualization
   - `num_partitions()` - Partition count

**Python-Rust Boundary**:
- All operations return new `DataFrame` with updated `LogicalPlanBuilder`
- No computation happens until `collect()`, `show()`, or `write_*()`
- Execution delegated to `Runner.run(builder)`

**Example Usage**:
```python
df = daft.read_parquet("s3://bucket/data/*.parquet")
result = (df
    .filter(df["age"] > 18)
    .groupby("country")
    .agg(df["salary"].mean())
    .sort("country")
    .collect())  # Execution happens here
```

---

### 2. Expression API

**Location**: `daft/expressions/expressions.py` (~3,812 lines)

**Purpose**: Domain-specific language (DSL) for expressing column operations and computations.

**Internal State**:
```python
class Expression:
    _expr: _PyExpr  # Wrapped Rust PyExpr
```

**Factory Functions** (lines 45-174):

```python
col(name: str) -> Expression               # Column reference
lit(value: Any) -> Expression              # Literal value
element() -> Expression                    # List element reference
interval(value, unit) -> Expression        # Temporal interval
coalesce(*exprs) -> Expression             # Null coalescing
struct(dict) -> Expression                 # Struct construction
list_(items) -> Expression                 # List construction
```

**Expression Namespaces**:

Daft organizes methods by data type using namespace properties:

1. **`.str`** - String operations (ExpressionStringNamespace):
   - `contains()`, `startswith()`, `endswith()`
   - `upper()`, `lower()`, `capitalize()`
   - `split()`, `replace()`, `extract()`
   - `length()`, `substring()`

2. **`.dt`** - Datetime operations (ExpressionDatetimeNamespace):
   - `day()`, `month()`, `year()`, `hour()`, `minute()`
   - `truncate()`, `date()`, `time()`
   - `strftime()`, `strptime()`

3. **`.list`** - List operations (ExpressionListNamespace):
   - `get()`, `join()`, `lengths()`
   - `explode()`, `slice()`

4. **`.struct`** - Struct operations (ExpressionStructNamespace):
   - `get()` - Extract field by name

5. **`.image`** - Image operations (ExpressionImageNamespace):
   - `decode()`, `encode()`, `resize()`
   - `crop()`, `to_mode()`

6. **`.url`** - URL operations (ExpressionUrlNamespace):
   - `download()` - Download content from URLs

7. **`.float`** - Float operations (ExpressionFloatNamespace):
   - `is_nan()`, `is_inf()`, `fill_nan()`

8. **`.embedding`** - Embedding operations (ExpressionEmbeddingNamespace):
   - `cosine_distance()`, `dot()`

9. **`.partitioning`** - Partitioning operations (ExpressionPartitioningNamespace):
   - `days()`, `hours()`, `months()`, `years()`, `iceberg_bucket()`, `iceberg_truncate()`

**Operators** (lines 800-1200):

Expressions support Python operators:
```python
df["price"] * 1.1                    # Arithmetic
df["age"] > 18                       # Comparison
df["name"].is_null()                 # Null checking
df["value"].cast(DataType.float64()) # Type casting
df["amount"].alias("total")          # Renaming
```

**UDF Support** (lines 265-290):

```python
@udf(return_dtype=DataType.int64())
def custom_logic(x: int) -> int:
    return x * 2

df.with_column("doubled", custom_logic(df["value"]))
```

**Python-Rust Boundary**:
- All expression operations create new `Expression` wrapping updated `_PyExpr`
- Expression trees built in Rust for efficient evaluation
- Imports from `daft.daft`: `PyExpr`, `lit`, `udf`, `CountMode`

---

### 3. Series API

**Location**: `daft/series.py` (~1,105 lines)

**Purpose**: One-dimensional array (single column) with type-aware operations.

**Internal State**:
```python
class Series:
    _series: PySeries  # Wrapped Rust PySeries
```

**Factory Methods** (lines 37-179):

```python
Series.from_arrow(arr: pa.Array, name: str = "series") -> Series
Series.from_pylist(data: list, name: str = "series",
                   pyobj: str = "disallow") -> Series
Series.from_numpy(arr: np.ndarray, name: str = "series") -> Series
Series.from_pandas(s: pd.Series) -> Series
```

**Operations**:

1. **Arithmetic** (lines 444-472):
   - `__add__`, `__sub__`, `__mul__`, `__truediv__`, `__mod__`
   - `__floordiv__`, `__pow__`

2. **Comparison** (lines 474-508):
   - `__eq__`, `__ne__`, `__gt__`, `__lt__`, `__ge__`, `__le__`

3. **Logical** (lines 510-524):
   - `__and__`, `__or__`, `__xor__`, `__invert__`

4. **Aggregations** (lines 550-620):
   - `count()`, `sum()`, `mean()`, `min()`, `max()`
   - `stddev()`, `variance()`, `any()`, `all()`

5. **Math Functions** (lines 332-442):
   - `sqrt()`, `sin()`, `cos()`, `tan()`, `arcsin()`, `arccos()`, `arctan()`
   - `exp()`, `log()`, `log2()`, `log10()`
   - `ceil()`, `floor()`, `round()`, `abs()`

**Series Namespaces** (similar to Expression namespaces):
- `.str`, `.dt`, `.list`, `.map`, `.image`, `.float`, `.partitioning`

**Conversion Methods** (lines 217-290):
```python
series.to_arrow() -> pa.Array
series.to_pylist() -> list
series.to_numpy() -> np.ndarray
series.to_pandas() -> pd.Series
```

**Python-Rust Boundary**:
- All operations delegate directly to Rust `PySeries`
- Imports from `daft.daft`: `PySeries`, `PySeriesIterator`, `native`

---

### 4. Schema & DataType System

#### Schema

**Location**: `daft/schema.py` (lines 63-320)

**Purpose**: Collection of named, typed fields describing DataFrame structure.

**Classes**:

1. **`Field`** (lines 29-61):
   ```python
   class Field:
       _field: _PyField  # Rust PyField

       @staticmethod
       def create(name: str, dtype: DataType,
                  metadata: dict = None) -> Field

       @property
       def name(self) -> str

       @property
       def dtype(self) -> DataType
   ```

2. **`Schema`** (lines 63-320):
   ```python
   class Schema:
       _schema: _PySchema  # Rust PySchema

       @staticmethod
       def from_pyarrow_schema(arrow_schema: pa.Schema) -> Schema

       @staticmethod
       def from_field_name_and_types(fields: list[tuple]) -> Schema

       @staticmethod
       def from_parquet(path: str, io_config: IOConfig = None) -> Schema

       def column_names(self) -> list[str]
       def __getitem__(self, key: str) -> Field
       def union(self, other: Schema) -> Schema
   ```

**Python-Rust Boundary**:
- Imports: `_PyField`, `_PySchema` from `daft.daft`
- Schema inference functions: `read_parquet_schema`, `read_csv_schema`, `read_json_schema`

#### DataType

**Location**: `daft/datatype.py` (lines 94-1465)

**Purpose**: Type system supporting Arrow types, logical types, and multimodal data.

**Type Categories**:

1. **Primitive Types** (lines 429-476):
   ```python
   DataType.int8(), DataType.int16(), DataType.int32(), DataType.int64()
   DataType.uint8(), DataType.uint16(), DataType.uint32(), DataType.uint64()
   DataType.float32(), DataType.float64()
   DataType.bool()
   ```

2. **String & Binary** (lines 479-498):
   ```python
   DataType.string()
   DataType.binary()
   DataType.fixed_size_binary(size: int)
   ```

3. **Temporal Types** (lines 511-539):
   ```python
   DataType.date()
   DataType.time(unit: TimeUnit = TimeUnit.us())
   DataType.timestamp(unit: TimeUnit = TimeUnit.us(), tz: str = None)
   DataType.duration(unit: TimeUnit = TimeUnit.us())
   DataType.interval()
   ```

4. **Nested Types** (lines 542-582):
   ```python
   DataType.list(dtype: DataType)
   DataType.fixed_size_list(dtype: DataType, size: int)
   DataType.struct(fields: dict[str, DataType])
   DataType.map(key_type: DataType, value_type: DataType)
   ```

5. **Multimodal Types** (lines 601-662):
   ```python
   DataType.image(mode: ImageMode = None, height: int = None, width: int = None)
   DataType.tensor(dtype: DataType, shape: tuple = None)
   DataType.sparse_tensor(dtype: DataType, shape: tuple = None)
   DataType.embedding(dtype: DataType, size: int)
   DataType.file(mode: str = "rb")
   ```

6. **Special Types**:
   ```python
   DataType.null()
   DataType.python()  # PyObject
   ```

**Type Inference** (lines 106-400):

```python
# From Python type hints
DataType.infer_from_type(int) -> DataType.int64()
DataType.infer_from_type(list[str]) -> DataType.list(DataType.string())

# From objects
DataType.infer_from_object([1, 2, 3]) -> DataType.list(DataType.int64())

# From TypedDict
class MyRecord(TypedDict):
    name: str
    age: int

DataType.infer_from_type(MyRecord) -> DataType.struct({"name": string, "age": int64})

# From Pydantic models
class Person(BaseModel):
    name: str
    age: int

DataType.infer_from_type(Person) -> DataType.struct({"name": string, "age": int64})
```

**Conversion** (lines 697-826):
```python
DataType.from_arrow_type(pa_type: pa.DataType) -> DataType
dtype.to_arrow_dtype() -> pa.DataType
DataType.from_numpy_dtype(np_dtype) -> DataType
```

**Type Checking** (lines 828-1217):
```python
dtype.is_null(), dtype.is_boolean(), dtype.is_integer()
dtype.is_numeric(), dtype.is_temporal(), dtype.is_nested()
dtype.is_image(), dtype.is_tensor(), dtype.is_embedding()
```

---

### 5. I/O API

**Location**: `daft/io/` (multiple modules)

**Purpose**: Reading data from various sources and file formats.

**Read Functions**:

1. **File Formats**:
   ```python
   daft.read_parquet(path, row_groups=None, schema=None,
                     io_config=None, ...)
   daft.read_csv(path, delimiter=",", header=True,
                 schema=None, io_config=None, ...)
   daft.read_json(path, schema=None, io_config=None, ...)
   ```

2. **Table Formats**:
   ```python
   daft.read_iceberg(table_uri, io_config=None, ...)
   daft.read_deltalake(table_uri, io_config=None, ...)
   daft.read_hudi(table_uri, io_config=None, ...)
   daft.read_lance(uri, io_config=None, ...)
   ```

3. **Special Sources**:
   ```python
   daft.read_sql(query, conn_string, io_config=None)
   daft.read_huggingface(dataset_name, split=None, ...)
   daft.read_video_frames(path, io_config=None)
   daft.read_warc(path, io_config=None)
   daft.read_mcap(path, io_config=None)
   ```

4. **Utilities**:
   ```python
   daft.from_glob_path(pattern, io_config=None)
   daft.range(start, end, step=1)  # Range data source
   ```

**Configuration**: `IOConfig`

```python
from daft.io import IOConfig, S3Config

io_config = IOConfig(
    s3=S3Config(
        region_name="us-west-2",
        max_connections=64,
        retry_initial_backoff_ms=1000,
    )
)

df = daft.read_parquet("s3://bucket/data/*.parquet", io_config=io_config)
```

**Cloud Storage Configs**:
- `S3Config`, `S3Credentials` - AWS S3
- `AzureConfig` - Azure Blob Storage
- `GCSConfig` - Google Cloud Storage
- `HTTPConfig` - HTTP/HTTPS endpoints

**Implementation Pattern** (`daft/io/_parquet.py` lines 20-93):

```python
def read_parquet(path, ...):
    # 1. Get IOConfig from context if not provided
    io_config = io_config or get_context().daft_planning_config.default_io_config

    # 2. Create FileFormatConfig
    file_format_config = FileFormatConfig.Parquet(...)

    # 3. Create StorageConfig
    storage_config = StorageConfig.Native(...)

    # 4. Build scan logical plan
    builder = get_tabular_files_scan(
        path=path,
        file_format_config=file_format_config,
        storage_config=storage_config,
        ...
    )

    # 5. Return DataFrame
    return DataFrame(builder)
```

---

### 6. UDF (User-Defined Functions)

**Location**: `daft/udf/__init__.py` (lines 1-471)

**Purpose**: Allow users to extend Daft with custom Python functions.

**Decorator Types**:

#### 1. `@daft.func` - Row-wise UDFs

```python
from daft import func, col
import daft

# Row-wise (1 row in, 1 row out)
@func
def double(x: int) -> int:
    return x * 2

df.with_column("doubled", double(col("value")))

# With explicit return type
@func(return_dtype=daft.DataType.string())
def format_name(first: str, last: str) -> str:
    return f"{first} {last}"

# Async support
@func
async def fetch_data(url: str) -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()

# Generator (1 row in, N rows out)
@func
def expand(items: list[int]) -> Iterator[int]:
    for item in items:
        yield item
        yield item * 2
```

#### 2. `@func.batch` - Batch UDFs

```python
import numpy as np

@func.batch(return_dtype=daft.DataType.float64())
def normalize(values: daft.Series) -> np.ndarray:
    arr = values.to_numpy()
    return (arr - arr.mean()) / arr.std()

df.with_column("normalized", normalize(col("values")))
```

**Parameters**:
- `batch_size`: Max rows per batch (optional)
- `return_dtype`: Return type (required)
- `unnest`: Flatten struct fields (default: False)

#### 3. `@daft.cls` - Stateful UDFs

```python
from daft import cls, method
import torch

@cls(gpus=1)
class ImageClassifier:
    def __init__(self):
        # Expensive initialization - runs once per worker
        self.model = torch.load("model.pth")
        self.model.cuda()

    @method
    def classify(self, image_bytes: bytes) -> str:
        img = decode_image(image_bytes)
        logits = self.model(img)
        return get_label(logits.argmax())

classifier = ImageClassifier()
df.with_column("label", classifier.classify(col("image")))
```

**Parameters for `@cls`**:
- `gpus`: Number of GPUs required (default: 0)
- `use_process`: Run in separate process (optional)
- `max_concurrency`: Max concurrent instances (optional)

#### 4. `@method` - Methods in UDF classes

```python
@cls
class TextProcessor:
    def __init__(self):
        self.tokenizer = load_tokenizer()

    @method
    def tokenize(self, text: str) -> list[str]:
        return self.tokenizer(text)

    @method.batch(return_dtype=daft.DataType.list(daft.DataType.string()))
    def tokenize_batch(self, texts: daft.Series) -> list[list[str]]:
        return self.tokenizer.batch(texts.to_pylist())
```

**Resource Management**:
```python
@cls(gpus=1, max_concurrency=4)
class GPUProcessor:
    def __init__(self):
        self.device = torch.device("cuda")

    @method
    def process(self, data: np.ndarray) -> np.ndarray:
        tensor = torch.from_numpy(data).to(self.device)
        result = self.model(tensor)
        return result.cpu().numpy()
```

---

### 7. Runner Abstraction

**Location**: `daft/runners/` (multiple modules)

**Purpose**: Pluggable execution backends (local vs. distributed).

**Base Interface** (`runner.py` lines 25-71):

```python
from abc import ABC, abstractmethod

class Runner(Generic[PartitionT], ABC):
    name: ClassVar[Literal["ray", "native"]]

    @abstractmethod
    def run(self, builder: LogicalPlanBuilder) -> PartitionCacheEntry:
        """Execute plan and return all results"""
        pass

    @abstractmethod
    def run_iter(self, builder: LogicalPlanBuilder,
                 results_buffer_size: int | None = None
                ) -> Iterator[MaterializedResult[PartitionT]]:
        """Execute plan and stream results"""
        pass

    @abstractmethod
    def run_iter_tables(self, builder: LogicalPlanBuilder,
                        results_buffer_size: int | None = None
                       ) -> Iterator[MicroPartition]:
        """Execute plan and stream MicroPartitions"""
        pass
```

**Implementations**:

1. **NativeRunner**: Multi-threaded local execution
   - Uses Rust `daft-local-execution` engine
   - Streaming pipeline with async operators
   - Best for single-machine workloads

2. **RayRunner**: Distributed execution on Ray
   - Partitions distributed across Ray cluster
   - Supports GPU workloads
   - Best for large-scale, multi-node workloads

**Global Functions** (`__init__.py`):

```python
import daft

# Get or create current runner
runner = daft.runners.get_or_create_runner()

# Switch runners
daft.set_runner_native()  # Use local multi-threaded
daft.set_runner_ray()     # Use Ray distributed

# Infer from environment
runner_type = daft.runners.get_or_infer_runner_type()
```

**Environment Variables**:
- `DAFT_RUNNER=native` - Use native runner
- `DAFT_RUNNER=ray` - Use Ray runner

---

### 8. Context & Configuration

**Location**: `daft/context.py` (lines 1-350)

**Purpose**: Global configuration for planning and execution.

#### DaftContext (Singleton)

```python
from daft.context import get_context

ctx = get_context()
ctx.daft_execution_config  # Execution settings
ctx.daft_planning_config   # Planning settings
```

#### Planning Configuration

```python
daft.set_planning_config(
    default_io_config=io_config,
    enable_strict_filter_pushdown=True,
)
```

**Context Manager**:
```python
with daft.planning_config_ctx(default_io_config=custom_config):
    df = daft.read_parquet("s3://bucket/data.parquet")
    # Uses custom_config
```

#### Execution Configuration

```python
daft.set_execution_config(
    # Scan configuration
    scan_tasks_min_size_bytes=256 * 1024 * 1024,  # 256 MB
    scan_tasks_max_size_bytes=512 * 1024 * 1024,  # 512 MB

    # Join configuration
    broadcast_join_size_bytes_threshold=10 * 1024 * 1024,  # 10 MB

    # Display configuration
    num_preview_rows=8,

    # Output configuration
    parquet_target_filesize=512 * 1024 * 1024,  # 512 MB
    csv_target_filesize=512 * 1024 * 1024,

    # Optimization
    enable_aqe=True,  # Adaptive Query Execution

    # Execution tuning
    default_morsel_size=131_072,  # Batch size for local executor
    shuffle_algorithm="hash",

    # Writer configuration
    native_parquet_writer=True,  # Use native vs PyArrow writer
)
```

**Context Manager**:
```python
with daft.execution_config_ctx(num_preview_rows=20):
    df.show()  # Shows 20 rows instead of default
```

---

### 9. Additional APIs

#### Catalog Integration

**Location**: `daft/catalog/`

```python
from daft.catalog import Catalog

# Create catalog
catalog = Catalog.create(name="my_catalog", provider="unity")

# Register table
catalog.attach_table("my_table", df)

# Query table
df = catalog.get_table("my_table")
```

#### SQL Interface

**Location**: `daft/sql.py`

```python
import daft

# Execute SQL query
result = daft.sql("SELECT * FROM my_table WHERE age > 18")

# SQL expressions
expr = daft.sql_expr("CAST(age AS STRING)")
df.with_column("age_str", expr)
```

#### Window Functions

**Location**: `daft/window.py`

```python
from daft import Window

# Define window
window = Window.partitionBy("category").orderBy("date")

# Use window function
df.with_column(
    "row_num",
    daft.functions.row_number().over(window)
)
```

---

## Rust Implementation Layer

The Rust implementation provides the high-performance execution engine underneath the Python API.

### Crate Organization

Daft's Rust code is organized into ~55 crates in `src/`, grouped by functionality:

```
src/
├── common/              # Shared utilities (15 crates)
├── daft-*/              # Core crates (35+ crates)
├── arrow2/              # Vendored Arrow2 implementation
├── parquet2/            # Vendored Parquet2 implementation
└── generated/           # Generated code (Spark Connect)
```

---

### Core Data Structures

#### 1. daft-schema

**Location**: `src/daft-schema/`

**Purpose**: Type system and schema definitions.

**Key Files**:
- `src/dtype.rs` (lines 1-1015): `DataType` enum with ~30 variants
- `src/field.rs`: `Field` struct (name + dtype)
- `src/schema.rs`: `Schema` struct (collection of fields)

**Key Type**: `DataType` enum (lines 13-160 of dtype.rs)

```rust
pub enum DataType {
    // Primitive types
    Null,
    Boolean,
    Int8, Int16, Int32, Int64,
    UInt8, UInt16, UInt32, UInt64,
    Float32, Float64,

    // String and binary
    Utf8, Binary, FixedSizeBinary(usize),

    // Temporal types
    Date,
    Time(TimeUnit),
    Timestamp(TimeUnit, Option<String>),  // tz optional
    Duration(TimeUnit),
    Interval,

    // Nested types
    List(Box<DataType>),
    FixedSizeList(Box<DataType>, usize),
    Struct(Vec<Field>),
    Map { key: Box<DataType>, value: Box<DataType> },

    // Extension/logical types
    Decimal128(usize, usize),  // precision, scale
    Extension(String, Box<DataType>, Option<String>),

    // Multimodal types (Daft-specific)
    Image(Option<ImageMode>, Option<u32>, Option<u32>),
    Tensor(Box<DataType>, Option<Vec<u64>>),
    SparseTensor(Box<DataType>, Option<Vec<u64>>),
    Embedding(Box<DataType>, usize),
    File(Option<String>),

    // Special
    Python,  // PyObject
    Unknown,
}
```

**Schema Operations**:
```rust
impl Schema {
    pub fn new(fields: Vec<Field>) -> Result<Self>
    pub fn empty() -> Self
    pub fn union(&self, other: &Schema) -> Result<Self>
    pub fn get_field(&self, name: &str) -> Option<&Field>
    pub fn column_names(&self) -> Vec<&str>
}
```

**Dependencies**: Minimal - `arrow2`, `serde`, `bincode`

---

#### 2. daft-core

**Location**: `src/daft-core/`

**Purpose**: Core array and series implementations with computational kernels.

**Key Files**:
- `src/series/mod.rs` (lines 1-255): `Series` struct
- `src/array/`: Per-dtype `DataArray<T>` implementations
- `src/series/ops/`: Operation implementations

**Key Type**: `Series` (line 34 of series/mod.rs)

```rust
pub struct Series {
    inner: Arc<dyn SeriesLike>,  // Trait object for polymorphism
}

pub trait SeriesLike: Send + Sync {
    fn data_type(&self) -> &DataType;
    fn name(&self) -> &str;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool { self.len() == 0 }

    // Casting
    fn cast(&self, dtype: &DataType) -> DaftResult<Series>;

    // Filtering/slicing
    fn filter(&self, mask: &BooleanArray) -> DaftResult<Series>;
    fn take(&self, indices: &UInt64Array) -> DaftResult<Series>;
    fn slice(&self, start: usize, end: usize) -> DaftResult<Series>;

    // Comparisons
    fn equal(&self, other: &Series) -> DaftResult<BooleanArray>;
    fn not_equal(&self, other: &Series) -> DaftResult<BooleanArray>;
    fn lt(&self, other: &Series) -> DaftResult<BooleanArray>;
    // ... more comparisons

    // Aggregations
    fn sum(&self) -> DaftResult<Series>;
    fn mean(&self) -> DaftResult<Series>;
    fn min(&self) -> DaftResult<Series>;
    fn max(&self) -> DaftResult<Series>;
    // ... more aggregations

    // Sorting
    fn sort(&self, descending: bool) -> DaftResult<Series>;
    fn argsort(&self, descending: bool) -> DaftResult<UInt64Array>;

    // Hashing (for joins/groupby)
    fn hash(&self, seed: u64) -> DaftResult<UInt64Array>;
}
```

**DataArray Implementation**:
```rust
pub struct DataArray<T> {
    field: Arc<Field>,
    data: Box<dyn arrow2::array::Array>,
    _phantom: PhantomData<T>,
}
```

Each data type has specialized implementations:
- `Int64Array`, `Float64Array`, `Utf8Array`, etc.
- Type-specific operations optimized per dtype

**Key Operations** (in `src/series/ops/`):
- `sort.rs`: Sorting algorithms
- `hash.rs`: Hashing for joins/groupby
- `filter.rs`: Boolean filtering
- `cast.rs`: Type conversions
- `comparison.rs`: Comparison operators
- `aggregation.rs`: Aggregation functions

**Dependencies**:
- `daft-schema` for types
- `arrow2` for underlying arrays
- `daft-hash`, `daft-minhash` for hashing
- `daft-sketch`, `hyperloglog` for approximate aggregations

---

#### 3. daft-dsl

**Location**: `src/daft-dsl/`

**Purpose**: Expression DSL (Domain-Specific Language) for operations.

**Key Files**:
- `src/expr/mod.rs` (lines 1-2253): Core expression types
- `src/functions/`: Function implementations

**Key Type**: `Expr` enum (lines 219-299 of expr/mod.rs)

```rust
pub enum Expr {
    // Column references
    Column(Column),  // Can be Unresolved/Resolved/Bound

    // Literals
    Literal(LiteralValue),

    // Binary operations
    BinaryOp {
        op: Operator,
        left: Arc<Expr>,
        right: Arc<Expr>,
    },

    // Unary operations
    Not(Arc<Expr>),
    IsNull(Arc<Expr>),
    NotNull(Arc<Expr>),

    // Type operations
    Cast {
        expr: Arc<Expr>,
        dtype: DataType,
    },

    // Aggregations
    Agg(AggExpr),

    // Functions
    Function {
        func: FunctionExpr,
        inputs: Vec<Arc<Expr>>,
    },

    // Conditional
    IfElse {
        if_true: Arc<Expr>,
        if_false: Arc<Expr>,
        predicate: Arc<Expr>,
    },

    // Window functions
    Over {
        expr: Arc<Expr>,
        window_spec: WindowSpec,
    },

    // Subqueries
    Subquery(SubqueryExpr),

    // Aliases
    Alias(Arc<Expr>, Arc<str>),

    // ... and more
}
```

**Column Resolution** (lines 113-180):

```rust
pub enum Column {
    // Before schema binding
    UnresolvedColumn { name: Arc<str> },

    // After logical plan binding
    ResolvedColumn(ResolvedColumn),

    // After physical plan binding
    BoundColumn { ordinal: usize, name: Arc<str> },
}

pub enum ResolvedColumn {
    Basic { name: Arc<str>, field: Arc<Field> },
    JoinSide { name: Arc<str>, side: JoinSide, field: Arc<Field> },
    OuterRef { name: Arc<str>, depth: usize, field: Arc<Field> },
}
```

**Aggregation Expressions** (lines 308-369):

```rust
pub enum AggExpr {
    Count { expr: Arc<Expr>, mode: CountMode },
    Sum(Arc<Expr>),
    Mean(Arc<Expr>),
    Min(Arc<Expr>),
    Max(Arc<Expr>),
    First(Arc<Expr>),
    Last(Arc<Expr>),
    List(Arc<Expr>),
    Concat(Arc<Expr>),
    ApproxPercentile { expr: Arc<Expr>, percentile: f64, force_list: bool },
    ApproxCountDistinct(Arc<Expr>),
    ApproxSketch(Arc<Expr>),
    // ... and more
}
```

**Window Functions** (lines 371-395):

```rust
pub enum WindowExpr {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile { num_buckets: usize },
    Offset { offset: i64, default: Option<Arc<Expr>> },
}
```

**Expression Evaluation**:
- Expressions are trees evaluated recursively
- Evaluation happens in `daft-recordbatch` using `daft-core` operations
- Result is always a `Series`

**Dependencies**:
- `daft-core` for data operations
- `daft-schema` for types
- `common-treenode` for tree traversal
- `daft-sketch` for approximate aggregations

---

#### 4. daft-recordbatch

**Location**: `src/daft-recordbatch/`

**Purpose**: RecordBatch - collection of Series (columnar table).

**Key Files**:
- `src/lib.rs` (lines 61-1225): Main implementation

**Key Type**: `RecordBatch` (lines 61-66)

```rust
pub struct RecordBatch {
    pub schema: SchemaRef,         // Arc<Schema>
    columns: Arc<Vec<Series>>,
    num_rows: usize,
}
```

**Construction** (lines 100-250):

```rust
impl RecordBatch {
    // From columns with validation
    pub fn new(schema: SchemaRef, columns: Vec<Series>) -> DaftResult<Self>

    // From columns without validation (unsafe, fast)
    pub fn new_unchecked(schema: SchemaRef, columns: Vec<Series>,
                          num_rows: usize) -> Self

    // From nonempty columns (infer schema)
    pub fn from_nonempty_columns(columns: Vec<Series>) -> DaftResult<Self>

    // With broadcasting for scalar columns
    pub fn new_with_broadcast(schema: SchemaRef, columns: Vec<Series>)
        -> DaftResult<Self>

    // Empty batch with schema
    pub fn new_with_size(schema: SchemaRef, num_rows: usize)
        -> DaftResult<Self>
}
```

**Filtering & Selection** (lines 414-470):

```rust
impl RecordBatch {
    // Boolean filtering
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self>
    pub fn mask_filter(&self, mask: &BooleanArray) -> DaftResult<Self>

    // Index-based selection
    pub fn take(&self, indices: &UInt64Array) -> DaftResult<Self>

    // Slicing
    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self>

    // Head/tail
    pub fn head(&self, n: usize) -> DaftResult<Self>
    pub fn tail(&self, n: usize) -> DaftResult<Self>
}
```

**Expression Evaluation** (lines 668-867):

This is where the magic happens - expressions get evaluated on data!

```rust
impl RecordBatch {
    // Evaluate single expression
    pub fn eval_expression(&self, expr: &Expr) -> DaftResult<Series>

    // Evaluate multiple expressions
    pub fn eval_expression_list(&self, exprs: &[Expr]) -> DaftResult<Self>

    // Helper: evaluate to boolean for filtering
    pub fn eval_boolean_expression(&self, expr: &Expr)
        -> DaftResult<BooleanArray>
}
```

**Aggregation** (lines 567-666):

```rust
impl RecordBatch {
    // Evaluate aggregation expression
    pub fn eval_agg_expression(&self, agg_expr: &AggExpr) -> DaftResult<Series>

    // Multi-column aggregation
    pub fn eval_agg_list(&self, agg_exprs: &[AggExpr]) -> DaftResult<Self>
}
```

**Sorting** (lines 472-550):

```rust
impl RecordBatch {
    // Sort by multiple columns
    pub fn sort(&self, sort_keys: &[Expr], descending: &[bool])
        -> DaftResult<Self>

    // Get sort indices without materializing
    pub fn argsort(&self, sort_keys: &[Expr], descending: &[bool])
        -> DaftResult<UInt64Array>
}
```

**Joins** (lines 868-1068):

```rust
impl RecordBatch {
    // Hash join (returns left/right indices)
    pub fn hash_join(&self, right: &RecordBatch,
                     left_on: &[Expr], right_on: &[Expr],
                     how: JoinType) -> DaftResult<(UInt64Array, UInt64Array)>

    // Sort-merge join
    pub fn sort_merge_join(&self, right: &RecordBatch,
                           left_on: &[Expr], right_on: &[Expr],
                           how: JoinType) -> DaftResult<(UInt64Array, UInt64Array)>
}
```

**I/O** (lines 1070-1225):

```rust
impl RecordBatch {
    // Arrow IPC serialization
    pub fn to_ipc_stream(&self) -> DaftResult<Vec<u8>>
    pub fn from_ipc_stream(data: &[u8]) -> DaftResult<Self>

    // Arrow conversion
    pub fn to_arrow(&self) -> arrow2::chunk::Chunk<Box<dyn Array>>
    pub fn from_arrow(arrow: arrow2::chunk::Chunk<...>, schema: SchemaRef)
        -> DaftResult<Self>
}
```

**Dependencies**:
- `daft-core` for Series operations
- `daft-dsl` for expression evaluation
- `daft-functions-*` for function implementations
- `daft-image` for image operations
- `daft-hash` for join hashing

**Architecture Role**: RecordBatch is the workhorse - it's where expressions are evaluated and operations are performed on batches of rows.

---

#### 5. daft-micropartition

**Location**: `src/daft-micropartition/`

**Purpose**: MicroPartition - Daft's fundamental unit of parallelism.

**Key Files**:
- `src/micropartition.rs` (lines 1-1504): Main implementation
- `src/lib.rs` (lines 1-64): Public API

**Key Type**: `MicroPartition` (lines 69-92 of micropartition.rs)

```rust
pub struct MicroPartition {
    pub(crate) schema: SchemaRef,
    pub(crate) state: Mutex<TableState>,
    pub(crate) metadata: TableMetadata,
    pub(crate) statistics: Option<TableStatistics>,
}

pub(crate) enum TableState {
    Unloaded(Arc<ScanTask>),           // Lazy - not yet materialized
    Loaded(Arc<Vec<RecordBatch>>),     // Eager - in memory
}

pub struct TableMetadata {
    pub length: usize,  // Total row count
}

pub struct TableStatistics {
    pub columns: HashMap<String, ColumnRangeStatistics>,
}
```

**Lazy Loading Pattern**:

```rust
impl MicroPartition {
    // Create unloaded (lazy) partition
    pub fn new_unloaded(
        schema: SchemaRef,
        scan_task: Arc<ScanTask>,
        metadata: TableMetadata,
        statistics: Option<TableStatistics>,
    ) -> Self

    // Create loaded (eager) partition
    pub fn new_loaded(
        schema: SchemaRef,
        tables: Arc<Vec<RecordBatch>>,
        statistics: Option<TableStatistics>,
    ) -> Self

    // Smart constructor - lazy if metadata available, else eager load
    pub fn from_scan_task(
        scan_task: Arc<ScanTask>,
        io_stats: Option<IOStatsContext>,
    ) -> DaftResult<Self>

    // Transparent materialization
    pub(crate) fn tables_or_read(
        &self,
        io_stats: Option<IOStatsContext>,
    ) -> DaftResult<Arc<Vec<RecordBatch>>>
}
```

**Why Lazy Loading?**

1. **Metadata Operations**: Can do schema inference, partition pruning without reading data
2. **Memory Efficiency**: Only load partitions needed for computation
3. **Statistics-Based Optimization**: Can skip partitions based on min/max statistics

**Materialization** (lines 102-345):

```rust
impl MicroPartition {
    fn materialize_scan_task(
        scan_task: &ScanTask,
        io_stats: Option<IOStatsContext>,
    ) -> DaftResult<Vec<RecordBatch>> {
        match scan_task.file_format_config {
            FileFormatConfig::Parquet(_) => {
                // Dispatch to daft-parquet
                read_parquet_bulk(scan_task, io_stats)
            }
            FileFormatConfig::Csv(_) => {
                // Dispatch to daft-csv
                read_csv(scan_task, io_stats)
            }
            FileFormatConfig::Json(_) => {
                // Dispatch to daft-json
                read_json(scan_task, io_stats)
            }
            // ... other formats
        }
    }
}
```

**Operations on MicroPartition**:

All RecordBatch operations are supported, operating on all batches:

```rust
impl MicroPartition {
    // Filtering
    pub fn filter(&self, exprs: &[Expr]) -> DaftResult<Self>

    // Projection
    pub fn select(&self, exprs: &[Expr]) -> DaftResult<Self>

    // Sorting
    pub fn sort(&self, sort_keys: &[Expr], descending: &[bool])
        -> DaftResult<Self>

    // Slicing
    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self>
    pub fn head(&self, n: usize) -> DaftResult<Self>

    // Aggregation
    pub fn agg(&self, agg_exprs: &[AggExpr], group_by: &[Expr])
        -> DaftResult<Self>

    // Joins
    pub fn hash_join(&self, right: &Self, left_on: &[Expr],
                     right_on: &[Expr], how: JoinType) -> DaftResult<Self>

    // Concatenation
    pub fn concat(parts: Vec<&Self>) -> DaftResult<Self>

    // Statistics
    pub fn statistics(&self) -> Option<&TableStatistics>
    pub fn size_bytes(&self) -> DaftResult<usize>
}
```

**Dependencies**:
- `daft-core`, `daft-dsl`, `daft-recordbatch` for data structures
- `daft-scan` for ScanTask
- `daft-csv`, `daft-json`, `daft-parquet`, `daft-warc` for readers
- `daft-io` for storage access
- `daft-stats` for statistics

**Architecture Role**: MicroPartition is the execution quantum. All parallel operations work at MicroPartition granularity. The lazy loading capability is crucial for efficient query execution.

---

### Query Planning & Execution

#### 6. daft-logical-plan

**Location**: `src/daft-logical-plan/`

**Purpose**: Logical query plan representation and optimization.

**Key Files**:
- `src/logical_plan.rs` (lines 1-1033): Core LogicalPlan enum
- `src/builder.rs`: Fluent builder API
- `src/ops/`: Individual operator implementations (25+ files)
- `src/optimization/`: Optimization rules (15+ rules)

**Key Type**: `LogicalPlan` enum (lines 24-51)

```rust
pub enum LogicalPlan {
    // Data sources
    Source(Source),
    Shard(Shard),

    // Projections
    Project(Project),
    UDFProject(UDFProject),

    // Filtering
    Filter(Filter),

    // Ordering
    Sort(Sort),
    TopN(TopN),

    // Limiting
    Limit(Limit),
    Offset(Offset),
    Sample(Sample),

    // Reshaping
    Explode(Explode),
    Unpivot(Unpivot),

    // Aggregation
    Aggregate(Aggregate),
    Pivot(Pivot),

    // Windowing
    Window(Window),

    // Joins
    Join(Join),

    // Set operations
    Concat(Concat),
    Union(Union),
    Intersect(Intersect),

    // Deduplication
    Distinct(Distinct),

    // Output
    Sink(Sink),

    // Partitioning
    Repartition(Repartition),
    IntoBatches(IntoBatches),

    // Metadata
    SubqueryAlias(SubqueryAlias),
    MonotonicallyIncreasingId(MonotonicallyIncreasingId),
}
```

**Operator Details** (in `src/ops/`):

Each operator is a separate file with struct definition:

```rust
// Example: src/ops/filter.rs
pub struct Filter {
    pub input: Arc<LogicalPlan>,
    pub predicate: Arc<Expr>,
}

// Example: src/ops/project.rs
pub struct Project {
    pub input: Arc<LogicalPlan>,
    pub projection: Vec<Arc<Expr>>,
    pub resource_request: ResourceRequest,
}

// Example: src/ops/aggregate.rs
pub struct Aggregate {
    pub input: Arc<LogicalPlan>,
    pub aggregations: Vec<Arc<AggExpr>>,
    pub groupby: Vec<Arc<Expr>>,
}

// Example: src/ops/join.rs
pub struct Join {
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub left_on: Vec<Arc<Expr>>,
    pub right_on: Vec<Arc<Expr>>,
    pub join_type: JoinType,
    pub join_strategy: Option<JoinStrategy>,
}
```

**Tree Operations** (lines 127-697):

```rust
impl LogicalPlan {
    // Get output schema
    pub fn schema(&self) -> &SchemaRef

    // Get required input columns for pruning
    pub fn required_columns(&self) -> DaftResult<HashSet<String>>

    // Tree navigation
    pub fn children(&self) -> Vec<&Arc<LogicalPlan>>
    pub fn with_new_children(&self, children: Vec<Arc<LogicalPlan>>)
        -> DaftResult<LogicalPlan>

    // Display
    pub fn multiline_display(&self) -> Vec<String>
}
```

**Builder API** (`src/builder.rs`):

```rust
pub struct LogicalPlanBuilder {
    plan: Arc<LogicalPlan>,
}

impl LogicalPlanBuilder {
    pub fn new(plan: Arc<LogicalPlan>) -> Self

    // Transformations
    pub fn select(&self, exprs: Vec<Expr>) -> DaftResult<Self>
    pub fn filter(&self, predicate: Expr) -> DaftResult<Self>
    pub fn limit(&self, n: i64) -> DaftResult<Self>
    pub fn sort(&self, sort_by: Vec<Expr>, descending: Vec<bool>)
        -> DaftResult<Self>

    // Aggregations
    pub fn aggregate(&self, agg_exprs: Vec<AggExpr>, group_by: Vec<Expr>)
        -> DaftResult<Self>

    // Joins
    pub fn join(&self, right: &Self, left_on: Vec<Expr>,
                right_on: Vec<Expr>, how: JoinType) -> DaftResult<Self>

    // Optimization
    pub fn optimize(&self) -> DaftResult<Self>

    // Conversion
    pub fn build(&self) -> Arc<LogicalPlan>
}
```

**Optimization** (in `src/optimization/`):

Daft has a rule-based optimizer with ~15 optimization rules:

1. **Pushdowns**:
   - `PushDownFilter` - Push filters closer to data sources
   - `PushDownProjection` - Push projections to reduce data early
   - `PushDownLimit` - Push limits to reduce rows early

2. **Pruning**:
   - `ColumnPruning` - Remove unused columns
   - `PartitionPruning` - Skip partitions based on predicates

3. **Simplification**:
   - `SimplifyExpressions` - Simplify expression trees
   - `EliminateUnusedColumns` - Remove dead columns

4. **Join Optimization**:
   - `JoinReordering` - Reorder joins for efficiency
   - `BroadcastJoinSelection` - Choose broadcast vs. shuffle join

5. **Other**:
   - `ConstantFolding` - Evaluate constants at compile time
   - `PredicatePushdown` - Advanced predicate pushdown
   - `LimitPushdown` - Push limits through operations

**Optimization Application**:

```rust
pub fn optimize(plan: Arc<LogicalPlan>) -> DaftResult<Arc<LogicalPlan>> {
    let mut current = plan;

    // Apply rules in sequence
    for rule in OPTIMIZATION_RULES {
        current = rule.apply(current)?;
    }

    Ok(current)
}
```

**Dependencies**:
- `daft-core`, `daft-dsl`, `daft-schema` for IR
- `daft-algebra` for optimization framework
- `common-treenode` for tree transformations
- `daft-scan` for scan operations

---

#### 7. daft-physical-plan

**Location**: `src/daft-physical-plan/`

**Purpose**: Physical execution plan with concrete algorithms and partitioning strategies.

**Key Differences from Logical Plan**:
- Adds physical properties (partitioning, ordering)
- Chooses concrete algorithms (hash join vs. sort-merge join)
- Determines data distribution strategy

**Example Physical Operators**:

```rust
pub enum PhysicalPlan {
    // Scans
    TabularScan(TabularScan),

    // Local operations
    Filter(Filter),
    Project(Project),

    // Shuffles
    HashShuffle(HashShuffle),
    BroadcastShuffle(BroadcastShuffle),

    // Joins
    HashJoin(HashJoin),        // Hash-based join
    SortMergeJoin(SortMergeJoin),  // Sort-merge join
    BroadcastJoin(BroadcastJoin),  // Broadcast small side

    // Aggregations
    HashAggregate(HashAggregate),

    // Sorting
    Sort(Sort),

    // ... more operators
}
```

**Translation from Logical to Physical**:

```rust
impl LogicalPlan {
    pub fn to_physical(&self, context: &PhysicalPlanContext)
        -> DaftResult<PhysicalPlan> {
        match self {
            LogicalPlan::Join(join) => {
                // Choose join algorithm based on statistics
                if join.right_size_bytes < BROADCAST_THRESHOLD {
                    PhysicalPlan::BroadcastJoin(...)
                } else {
                    PhysicalPlan::HashJoin(...)
                }
            }
            // ... other translations
        }
    }
}
```

---

#### 8. daft-local-plan & daft-local-execution

**Locations**:
- `src/daft-local-plan/` - Plan representation
- `src/daft-local-execution/` - Execution engine

**Purpose**: Single-node streaming execution engine.

**Architecture**: Streaming pipeline with async operators.

**Key Concepts**:

1. **Sources**: Produce MicroPartitions
2. **Intermediate Operators**: Transform MicroPartitions (filter, project, etc.)
3. **Sinks**: Collect all input before producing output (aggregate, sort, join)

**Pipeline Structure**:

```
Source → Filter → Project → HashJoin → Aggregate → Sink
  ↓        ↓        ↓          ↓          ↓        ↓
  MP       MP       MP         MP         MP     Result
```

Where MP = MicroPartition

**Key Files** (in `src/daft-local-execution/`):

- `src/sources/`: Data sources
  - `scan.rs` - File scanning
  - `in_memory.rs` - In-memory data

- `src/sinks/`: Blocking operators
  - `aggregate.rs` - Hash aggregation
  - `sort.rs` - External sort
  - `hash_join.rs` - Hash join build+probe
  - `write.rs` - File writing
  - `pivot.rs` - Pivot tables
  - `window.rs` - Window functions

- `src/intermediate_ops/`: Streaming operators
  - `filter.rs` - Row filtering
  - `project.rs` - Column projection
  - `explode.rs` - Unnesting

- `src/channel.rs`: Pipeline communication
- `src/resource_manager.rs`: Resource tracking

**Execution Flow**:

```rust
// 1. Convert LocalPlan to pipeline
let pipeline = build_pipeline(local_plan)?;

// 2. Set up channels between operators
let (sender, receiver) = create_channel(buffer_size);

// 3. Start operators as async tasks
tokio::spawn(async move {
    while let Some(morsel) = receiver.recv().await {
        let result = operator.execute(morsel).await?;
        sender.send(result).await?;
    }
});

// 4. Collect results
let results = collect_from_sink(final_sink).await?;
```

**Resource Management**:

```rust
pub struct ResourceManager {
    memory_budget: usize,
    memory_used: AtomicUsize,
    cpu_permits: Semaphore,
}

impl ResourceManager {
    pub async fn acquire_memory(&self, bytes: usize) -> ResourceGuard
    pub async fn acquire_cpu(&self) -> CpuPermit
}
```

**Morsel-Driven Parallelism**:

- Data flows as "morsels" (small batches of MicroPartitions)
- Default morsel size: 131,072 rows
- Operators process morsels independently
- Enables pipelining and parallelism

**Dependencies**:
- `daft-micropartition` for data flow
- `daft-local-plan` for plan representation
- `daft-csv`, `daft-json`, `daft-parquet` for I/O
- `daft-writers` for output
- `tokio`, `futures` for async execution
- `kanal` for channels

---

### I/O & File Formats

#### 9. daft-io

**Location**: `src/daft-io/`

**Purpose**: Unified I/O layer for cloud storage and local filesystem.

**Key Components**:

1. **IOClient**: Main entry point
   ```rust
   pub struct IOClient {
       config: Arc<IOConfig>,
       s3_client: Option<S3Client>,
       azure_client: Option<AzureClient>,
       gcs_client: Option<GCSClient>,
       http_client: reqwest::Client,
   }

   impl IOClient {
       pub async fn read(&self, path: &str) -> DaftResult<Bytes>
       pub async fn read_range(&self, path: &str, start: u64, end: u64)
           -> DaftResult<Bytes>
       pub async fn list(&self, prefix: &str) -> DaftResult<Vec<FileMetadata>>
       pub async fn get_size(&self, path: &str) -> DaftResult<u64>
   }
   ```

2. **Storage Backends**:
   - S3: AWS S3 via `aws-sdk-s3`
   - Azure: Azure Blob Storage via `azure_storage_blobs`
   - GCS: Google Cloud Storage via `google-cloud-storage`
   - HTTP/HTTPS: Via `reqwest`
   - Local: Standard filesystem

3. **Features**:
   - Async streaming reads
   - Range requests (for column pruning in Parquet)
   - Retry logic with exponential backoff
   - Connection pooling
   - Credential management

**Configuration**:

```rust
pub struct IOConfig {
    pub s3: S3Config,
    pub azure: AzureConfig,
    pub gcs: GCSConfig,
    pub http: HTTPConfig,
}

pub struct S3Config {
    pub region_name: Option<String>,
    pub endpoint_url: Option<String>,
    pub key_id: Option<String>,
    pub access_key: Option<String>,
    pub session_token: Option<String>,
    pub max_connections: usize,
    pub retry_initial_backoff_ms: u64,
    pub connect_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub num_tries: usize,
}
```

---

#### 10. daft-scan

**Location**: `src/daft-scan/`

**Purpose**: Scan planning and task representation.

**Key Types**:

```rust
pub struct ScanTask {
    pub file_path: String,
    pub file_size: Option<u64>,
    pub file_format_config: FileFormatConfig,
    pub storage_config: StorageConfig,
    pub pushdowns: Pushdowns,
    pub partition_spec: Option<PartitionSpec>,
    pub metadata: Option<TableMetadata>,
    pub statistics: Option<TableStatistics>,
}

pub struct Pushdowns {
    pub filters: Option<Vec<Expr>>,
    pub columns: Option<Vec<String>>,
    pub limit: Option<usize>,
}

pub enum FileFormatConfig {
    Parquet(ParquetSourceConfig),
    Csv(CsvSourceConfig),
    Json(JsonSourceConfig),
    // ... more formats
}
```

**Scan Task Creation**:

```rust
pub fn create_scan_tasks(
    file_paths: Vec<String>,
    file_format: FileFormat,
    schema: SchemaRef,
    pushdowns: Pushdowns,
    split_size: usize,
) -> DaftResult<Vec<ScanTask>> {
    // 1. Get file metadata (sizes, row counts)
    // 2. Split into tasks based on split_size
    // 3. Apply pushdowns to each task
    // 4. Return tasks
}
```

**Partition Pruning**:

```rust
pub fn prune_scan_tasks(
    tasks: Vec<ScanTask>,
    predicate: &Expr,
) -> DaftResult<Vec<ScanTask>> {
    tasks.into_iter()
        .filter(|task| {
            // Check if predicate can be satisfied by task statistics
            task.statistics.as_ref()
                .map(|stats| can_satisfy(stats, predicate))
                .unwrap_or(true)  // Keep task if no stats
        })
        .collect()
}
```

---

#### 11. daft-parquet

**Location**: `src/daft-parquet/`

**Purpose**: High-performance Parquet reading.

**Key Features**:

1. **Bulk Parallel Reading**:
   ```rust
   pub async fn read_parquet_bulk(
       scan_task: &ScanTask,
       io_stats: Option<IOStatsContext>,
   ) -> DaftResult<Vec<RecordBatch>> {
       // 1. Open file and read metadata
       let metadata = read_parquet_metadata(&scan_task.file_path).await?;

       // 2. Determine which row groups to read
       let row_groups = prune_row_groups(&metadata, &scan_task.pushdowns)?;

       // 3. Determine which columns to read
       let columns = get_columns_to_read(&scan_task.pushdowns)?;

       // 4. Read row groups in parallel
       let batches = read_row_groups_parallel(
           &scan_task.file_path,
           row_groups,
           columns,
       ).await?;

       // 5. Apply remaining pushdowns (filters, limit)
       apply_pushdowns(batches, &scan_task.pushdowns)
   }
   ```

2. **Row Group Pruning**:
   ```rust
   fn prune_row_groups(
       metadata: &FileMetadata,
       pushdowns: &Pushdowns,
   ) -> DaftResult<Vec<usize>> {
       let Some(filters) = &pushdowns.filters else {
           return Ok((0..metadata.num_row_groups).collect());
       };

       metadata.row_groups.iter()
           .enumerate()
           .filter(|(_, rg)| {
               // Check if any filter can be satisfied by statistics
               filters.iter().any(|f| can_satisfy(&rg.statistics, f))
           })
           .map(|(i, _)| i)
           .collect()
   }
   ```

3. **Column Pruning**: Only read requested columns

4. **Predicate Pushdown**: Apply filters during read when possible

5. **Statistics**: Extract min/max/null_count from Parquet metadata

**Dependencies**:
- `parquet2` for Parquet reading (arrow2-based)
- `arrow2` for Arrow interop
- `daft-io` for file I/O
- `daft-stats` for statistics

---

#### 12. daft-csv & daft-json

**daft-csv** (`src/daft-csv/`):
- CSV/TSV file reading
- Header detection
- Delimiter/quote configuration
- Schema inference
- Streaming for large files
- Compression support (gzip, bzip2, etc.)

**daft-json** (`src/daft-json/`):
- JSON/JSONL file reading
- Schema inference with type promotion
- Nested object support
- Streaming for large files

---

### Functions

#### 13. daft-functions & daft-functions-*

**Location**: `src/daft-functions**/` (9 crates)

**Purpose**: Scalar function implementations.

**Function Registry Pattern**:

```rust
// In daft-functions/src/lib.rs
pub trait FunctionRegistry {
    fn register_functions() -> HashMap<String, FunctionImpl>;
}

pub struct NumericFunctions;

impl FunctionRegistry for NumericFunctions {
    fn register_functions() -> HashMap<String, FunctionImpl> {
        let mut map = HashMap::new();
        map.insert("abs".to_string(), FunctionImpl::new(abs_impl));
        map.insert("sqrt".to_string(), FunctionImpl::new(sqrt_impl));
        // ... more functions
        map
    }
}
```

**Function Implementation**:

```rust
pub struct FunctionImpl {
    pub name: String,
    pub evaluator: fn(&[Series]) -> DaftResult<Series>,
    pub return_dtype: fn(&[DataType]) -> DaftResult<DataType>,
}

// Example: sqrt function
fn sqrt_impl(args: &[Series]) -> DaftResult<Series> {
    let arr = args[0].f64()?;  // Cast to Float64Array
    let result = arr.apply(|x| x.sqrt());
    Ok(Series::from(result))
}
```

**Function Crates**:

1. **daft-functions**: Core numeric, comparison, hash functions
2. **daft-functions-utf8**: String operations (contains, split, replace, etc.)
3. **daft-functions-list**: List operations (get, join, explode, etc.)
4. **daft-functions-json**: JSON operations (query, extract, etc.)
5. **daft-functions-uri**: URI/URL operations (parse, download, etc.)
6. **daft-functions-binary**: Binary data operations
7. **daft-functions-temporal**: Date/time operations
8. **daft-functions-tokenize**: Text tokenization
9. **daft-functions-serde**: Serialization/deserialization

**Registration** (in `src/lib.rs` lines 160-183):

```rust
#[pymodule]
fn daft(m: &Bound<PyModule>) -> PyResult<()> {
    // ... other registrations ...

    let mut registry = FUNCTION_REGISTRY.write()?;
    registry.register::<NumericFunctions>();
    registry.register::<Utf8Functions>();
    registry.register::<ListFunctions>();
    // ... more registrations
}
```

---

### Supporting Crates

#### 14. Common Utilities

**common-treenode** (`src/common/treenode/`):
- Tree traversal framework for ASTs
- Visitor pattern implementation
- Rewrite rules support

```rust
pub trait TreeNode: Sized {
    fn children(&self) -> Vec<&Self>;
    fn with_new_children(&self, children: Vec<Self>) -> Result<Self>;

    // Provided methods
    fn apply<F>(&self, f: F) -> Result<()>
        where F: FnMut(&Self) -> Result<()>;

    fn transform<F>(&self, f: F) -> Result<Self>
        where F: FnMut(&Self) -> Result<Option<Self>>;
}
```

**common-error** (`src/common/error/`):
- Error types and Result type
- Error chaining
- SNAFU integration

**common-daft-config** (`src/common/daft-config/`):
- Global configuration
- Feature flags
- Environment variable handling

**common-io-config** (`src/common/io-config/`):
- I/O configuration types
- Serialization support

**common-scan-info** (`src/common/scan-info/`):
- Scan metadata types
- File info structures

**common-resource-request** (`src/common/resource-request/`):
- Resource request types (CPU, memory, GPU)

**common-metrics** (`src/common/metrics/`):
- Metrics collection
- Performance tracking

**common-tracing** (`src/common/tracing/`):
- OpenTelemetry tracing integration
- Distributed tracing support

---

#### 15. Specialized Data Types

**daft-image** (`src/daft-image/`):
- Image type implementation
- Image operations (decode, encode, resize, crop)
- Multiple image modes (RGB, RGBA, L, etc.)

**daft-hash** (`src/daft-hash/`):
- Hash functions for joins and groupby
- Multiple hash algorithms (xxhash, murmurhash3)

**daft-minhash** (`src/daft-minhash/`):
- MinHash for similarity detection
- LSH (Locality-Sensitive Hashing)

**daft-sketch** (`src/daft-sketch/`):
- DDSketch for approximate quantiles
- Mergeable sketches for distributed aggregation

**hyperloglog** (`src/hyperloglog/`):
- HyperLogLog for approximate distinct counts
- Sparse and dense representations

---

#### 16. Higher-Level Features

**daft-sql** (`src/daft-sql/`):
- SQL parser (using `sqlparser`)
- SQL to LogicalPlan translation
- SQL expression support

**daft-catalog** (`src/daft-catalog/`):
- Catalog integration
- Iceberg, Delta Lake, Hudi support
- Table discovery and metadata

**daft-session** (`src/daft-session/`):
- Query session management
- Catalog state
- Configuration

**daft-context** (`src/daft-context/`):
- Execution context
- Global state management

**daft-distributed** (`src/daft-distributed/`):
- Distributed execution coordination
- Ray integration

**daft-runners** (`src/daft-runners/`):
- Runner implementations
- Native and Ray backends

**daft-connect** (`src/daft-connect/`):
- Spark Connect protocol support
- gRPC server implementation

**daft-dashboard** (`src/daft-dashboard/`):
- Query visualization dashboard
- Metrics and monitoring

---

## Python-Rust Interface (PyO3)

### PyO3 Binding Architecture

Daft uses PyO3 to expose Rust types and functions to Python.

**Root Module** (`src/lib.rs`):

```rust
use pyo3::prelude::*;

#[pymodule]
fn daft(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    // Register all submodules
    common_daft_config::register_modules(m)?;
    daft_core::register_modules(m)?;
    daft_dsl::register_modules(m)?;
    daft_logical_plan::register_modules(m)?;
    // ... 30+ more crate registrations ...

    // Register function implementations
    let mut registry = daft_dsl::functions::FUNCTION_REGISTRY.write()?;
    registry.register::<daft_functions::numeric::NumericFunctions>();
    registry.register::<daft_functions_utf8::Utf8Functions>();
    // ... 15+ function group registrations ...

    Ok(())
}
```

### Pattern: Wrapping Rust Types

Each Rust type exposed to Python follows this pattern:

**Rust Side**:

```rust
// In daft-core/src/python.rs
use pyo3::prelude::*;

#[pyclass(name = "Series")]
pub struct PySeries {
    pub series: crate::series::Series,  // Wrapped Rust type
}

#[pymethods]
impl PySeries {
    #[new]
    fn new(name: &str, values: PyObject) -> PyResult<Self> {
        // Convert Python object to Rust Series
        let series = python_to_series(name, values)?;
        Ok(PySeries { series })
    }

    fn __len__(&self) -> usize {
        self.series.len()
    }

    fn __repr__(&self) -> String {
        format!("{}", self.series)
    }

    fn cast(&self, dtype: PyDataType) -> PyResult<Self> {
        let new_series = self.series.cast(&dtype.dtype)?;
        Ok(PySeries { series: new_series })
    }

    // ... many more methods
}
```

**Python Side**:

```python
# In daft/series.py
from daft.daft import PySeries  # Import from Rust module

class Series:
    def __init__(self, name: str = "series"):
        self._series = PySeries(name, [])  # Wrapped Rust type

    def __len__(self) -> int:
        return self._series.__len__()

    def cast(self, dtype: DataType) -> Series:
        new_series = self._series.cast(dtype._dtype)
        return Series._from_pyseries(new_series)
```

### Type Mapping

**Rust → Python**:

| Rust Type | Python Type | Notes |
|-----------|-------------|-------|
| `i64`, `u64` | `int` | - |
| `f64` | `float` | - |
| `String`, `&str` | `str` | - |
| `bool` | `bool` | - |
| `Vec<T>` | `list[T]` | - |
| `HashMap<K, V>` | `dict[K, V]` | - |
| `Option<T>` | `T \| None` | - |
| `Result<T, E>` | `T` | Raises exception on Err |
| `Arc<T>` | `T` | Transparent |

### PyO3 Macros

**`#[pyclass]`**: Expose Rust struct to Python
```rust
#[pyclass(name = "DataFrame")]
pub struct PyDataFrame {
    builder: LogicalPlanBuilder,
}
```

**`#[pymethods]`**: Expose methods
```rust
#[pymethods]
impl PyDataFrame {
    fn select(&self, exprs: Vec<PyExpr>) -> PyResult<Self> {
        // ...
    }
}
```

**`#[pyfunction]`**: Expose standalone function
```rust
#[pyfunction]
fn read_parquet(path: String) -> PyResult<PyDataFrame> {
    // ...
}
```

**`#[pymodule]`**: Define module
```rust
#[pymodule]
fn daft(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<PyDataFrame>()?;
    m.add_function(wrap_pyfunction!(read_parquet, m)?)?;
    Ok(())
}
```

### Registration Pattern

Each crate with Python bindings exports a `register_modules` function:

```rust
// In daft-core/src/python/mod.rs
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PySeries>()?;
    parent.add_class::<PyRecordBatch>()?;
    parent.add_class::<PyMicroPartition>()?;
    // ... more classes
    Ok(())
}
```

### Memory Management

**Reference Counting**:
- Rust uses `Arc<T>` for shared ownership
- Python uses reference counting
- PyO3 bridges the two:
  - Rust `Arc<T>` → Python object (refcount++)
  - Python object dropped → Rust `Arc` dropped (refcount--)

**Zero-Copy**:
- Arrow arrays shared between Python and Rust via Arrow C Data Interface
- No serialization overhead for pandas/arrow conversions

### Error Handling

**Rust errors converted to Python exceptions**:

```rust
// Rust
pub fn risky_operation() -> DaftResult<i64> {
    if condition {
        Ok(42)
    } else {
        Err(DaftError::ValueError("Bad input".to_string()))
    }
}

// Python binding
#[pyfunction]
fn risky_operation_py() -> PyResult<i64> {
    risky_operation().map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))
}

# Python
try:
    result = daft.daft.risky_operation_py()
except ValueError as e:
    print(f"Error: {e}")
```

---

## Data Flow and Query Execution

### End-to-End Query Flow

Let's trace a complete query from Python API to execution:

```python
import daft

df = daft.read_parquet("s3://bucket/data/*.parquet")
result = (df
    .filter(df["age"] > 18)
    .groupby("country")
    .agg(df["salary"].mean())
    .sort("country")
    .collect())
```

**Step-by-Step Flow**:

1. **read_parquet** (`daft/io/_parquet.py`):
   ```python
   def read_parquet(path, ...):
       builder = get_tabular_files_scan(...)  # Creates LogicalPlanBuilder
       return DataFrame(builder)
   ```
   - Creates `Source` logical plan node
   - DataFrame wraps LogicalPlanBuilder
   - **No I/O happens yet** (lazy)

2. **filter** (`daft/dataframe/dataframe.py`):
   ```python
   def filter(self, predicate: Expression):
       new_builder = self._builder.filter(predicate._expr)
       return DataFrame(new_builder)
   ```
   - Adds `Filter` node to logical plan
   - Returns new DataFrame
   - Still no computation

3. **groupby** (`daft/dataframe/dataframe.py`):
   ```python
   def groupby(self, *group_by):
       return GroupedDataFrame(self, group_by)
   ```
   - Returns `GroupedDataFrame` wrapper
   - Waits for `.agg()` call

4. **agg** (`daft/dataframe/dataframe.py`):
   ```python
   def agg(self, *agg_exprs):
       new_builder = self._df._builder.aggregate(
           agg_exprs, self._group_by
       )
       return DataFrame(new_builder)
   ```
   - Adds `Aggregate` node to logical plan
   - Returns new DataFrame

5. **sort** (`daft/dataframe/dataframe.py`):
   ```python
   def sort(self, *sort_by):
       new_builder = self._builder.sort(sort_by, [False] * len(sort_by))
       return DataFrame(new_builder)
   ```
   - Adds `Sort` node to logical plan
   - Returns new DataFrame

6. **collect** (`daft/dataframe/dataframe.py`):
   ```python
   def collect(self):
       runner = get_or_create_runner()
       result = runner.run(self._builder)  # EXECUTION HAPPENS HERE
       return DataFrame._from_tables(result.values())
   ```
   - Triggers execution!
   - Gets current runner (Native or Ray)
   - Passes LogicalPlanBuilder to runner

**At this point, the logical plan looks like**:

```
Sort(by=country)
  ↓
Aggregate(group_by=[country], agg=[mean(salary)])
  ↓
Filter(age > 18)
  ↓
Source(s3://bucket/data/*.parquet)
```

7. **Optimization** (in Rust):
   ```rust
   let optimized_plan = optimize(logical_plan)?;
   ```
   - Applies optimization rules
   - Possible transformations:
     - Push filter into Source (partition pruning)
     - Push column projection into Source (column pruning)
     - Choose join strategies

   **Optimized plan**:
   ```
   Sort(by=country)
     ↓
   Aggregate(group_by=[country], agg=[mean(salary)])
     ↓
   Source(s3://bucket/data/*.parquet,
          filter=age>18,           # PUSHED DOWN
          columns=[country, age, salary])  # PRUNED
   ```

8. **Physical Planning**:
   ```rust
   let physical_plan = logical_plan.to_physical()?;
   ```
   - Converts to PhysicalPlan
   - Chooses concrete algorithms
   - Determines partitioning strategy

9. **Local Plan Generation**:
   ```rust
   let local_plan = physical_plan.to_local_plan()?;
   ```
   - Simplifies for single-node execution
   - Removes distributed coordination

10. **Execution** (in `daft-local-execution`):
    ```rust
    let pipeline = build_pipeline(local_plan)?;
    let results = execute_pipeline(pipeline).await?;
    ```

    **Pipeline stages**:

    a. **Scan Source**:
    ```
    ScanSource → [MicroPartition, MicroPartition, ...]
    ```
    - Lists files: `data-1.parquet`, `data-2.parquet`, ...
    - Creates ScanTasks with pushdowns
    - Reads files in parallel
    - Applies filter during read (if possible)
    - Produces MicroPartitions

    b. **Filter** (if not fully pushed down):
    ```
    Filter → [MP(filtered), MP(filtered), ...]
    ```
    - Evaluates filter expression on each MicroPartition
    - Removes filtered MicroPartitions

    c. **Aggregate**:
    ```
    HashAggregate(Sink) → [MP(aggregated)]
    ```
    - Collects ALL MicroPartitions (blocking)
    - Builds hash table: `country → (sum, count)`
    - Computes final aggregates
    - Produces single MicroPartition

    d. **Sort**:
    ```
    Sort(Sink) → [MP(sorted)]
    ```
    - Collects all rows (blocking)
    - Sorts by country
    - Produces single sorted MicroPartition

11. **Return to Python**:
    ```rust
    // Rust returns PyMicroPartition to Python
    let py_result = PyMicroPartition::from(result);
    ```

    ```python
    # Python receives result
    result = DataFrame._from_tables([py_result])
    ```

12. **Display**:
    ```python
    print(result)
    ```
    - Converts to Arrow tables
    - Pretty prints with comfy-table

### Data Movement

**MicroPartition Flow**:

```
Storage → RecordBatch → MicroPartition → Operations → Result
```

**Details**:

1. **Storage → RecordBatch** (`daft-parquet`):
   ```rust
   let batches = read_parquet_bulk(scan_task)?;
   // Vec<RecordBatch>
   ```

2. **RecordBatch → MicroPartition** (`daft-micropartition`):
   ```rust
   let mp = MicroPartition::new_loaded(schema, Arc::new(batches), stats);
   ```

3. **Operations** (filter, project, etc.):
   ```rust
   let filtered = mp.filter(&[filter_expr])?;
   let projected = filtered.select(&[col_exprs])?;
   ```

4. **Aggregation** (collect to sink):
   ```rust
   let mut hash_table = HashMap::new();
   for mp in micropartitions {
       for row in mp.iter_rows() {
           let key = eval_group_by(&row);
           hash_table.entry(key).or_insert_with(|| Accumulator::new());
           hash_table.get_mut(&key).unwrap().update(&row);
       }
   }
   let result_mp = hash_table_to_micropartition(hash_table)?;
   ```

### Parallelism

**Native Runner Parallelism**:

1. **Task Parallelism**: Multiple ScanTasks read in parallel
2. **Morsel Parallelism**: Pipeline stages process morsels concurrently
3. **Thread Pool**: Tokio runtime with work-stealing scheduler

**Ray Runner Parallelism**:

1. **Distributed Tasks**: ScanTasks distributed across cluster
2. **Actor Parallelism**: Multiple actors per node
3. **Shuffle**: Data redistributed for joins/aggregations

### Memory Management

**Memory Budget**:
```rust
pub struct ResourceManager {
    memory_budget: usize,  // e.g., 80% of system memory
    memory_used: AtomicUsize,
}
```

**Memory Tracking**:
- Each MicroPartition tracks its size
- Operators acquire memory before processing
- Backpressure when budget exceeded
- Spilling for sorts/aggregations (TODO)

---

## Key Abstractions

### MicroPartition

**What**: Collection of RecordBatches with lazy loading support.

**Why**:
- Execution quantum for parallelism
- Enables metadata-only operations
- Statistics for optimization

**Where Used**:
- Data flow between operators
- Scan results
- Intermediate results
- Final results

**Key Properties**:
- Can be `Unloaded` (lazy) or `Loaded` (materialized)
- Carries schema, metadata, statistics
- Supports all RecordBatch operations

**Example**:
```rust
// Create lazy MicroPartition
let mp = MicroPartition::from_scan_task(scan_task, io_stats)?;

// Operations work on lazy partitions
let filtered = mp.filter(&[expr])?;  // Still lazy if metadata-only

// Materialize when needed
let tables = mp.tables_or_read(io_stats)?;  // Now loaded
```

---

### RecordBatch

**What**: Columnar table (collection of Series) with schema.

**Why**:
- Efficient columnar operations
- Expression evaluation unit
- Arrow interop

**Where Used**:
- Inside MicroPartitions
- Expression evaluation
- Function implementations

**Key Properties**:
- Fixed schema
- Columnar layout (Series per column)
- Supports filtering, projection, joins, aggregations

**Example**:
```rust
let rb = RecordBatch::new(schema, vec![series1, series2])?;
let filtered = rb.filter(&mask)?;
let projected = rb.eval_expression_list(&[expr1, expr2])?;
```

---

### Series

**What**: Single column (typed array).

**Why**:
- Type-aware operations
- Efficient kernels per dtype
- Arrow compatibility

**Where Used**:
- Columns in RecordBatch
- Expression evaluation results
- Function arguments/returns

**Key Properties**:
- Single data type
- Variable length
- Trait-based polymorphism (`SeriesLike`)

**Example**:
```rust
let series = Series::from_vec("age", vec![20, 30, 40])?;
let doubled = series.multiply(&Series::from(2))?;
let sum = series.sum()?;
```

---

### Expression (Expr)

**What**: AST (Abstract Syntax Tree) representing computation.

**Why**:
- Separates logic from execution
- Enables optimization
- Type checking before execution

**Where Used**:
- User queries (filters, projections, etc.)
- Logical plan nodes
- Expression evaluation

**Key Properties**:
- Tree structure
- Lazy (not evaluated until executed)
- Three resolution states: Unresolved → Resolved → Bound

**Example**:
```rust
let expr = col("age")
    .gt(lit(18))
    .and(col("country").eq(lit("US")));

// Evaluate on RecordBatch
let result = rb.eval_expression(&expr)?;  // Returns BooleanArray
```

---

### LogicalPlan

**What**: Tree of relational operators.

**Why**:
- Represents user query
- Optimization target
- Platform-independent

**Where Used**:
- Query construction
- Optimization passes
- Physical plan generation

**Key Properties**:
- Tree structure (parent-child relationships)
- Each node = operator (Filter, Project, Join, etc.)
- Carries schema information

**Example**:
```rust
let plan = LogicalPlan::Source(source)
    .filter(predicate)?
    .project(columns)?
    .sort(sort_keys)?;

let optimized = optimize(plan)?;
```

---

## Developer Guide

This section provides practical guidance for extending and modifying Daft.

### Project Structure

```
Daft/
├── daft/                    # Python API
│   ├── dataframe/           # DataFrame implementation
│   ├── expressions/         # Expression API
│   ├── io/                  # I/O functions
│   └── ...
├── src/                     # Rust implementation
│   ├── daft-core/           # Core data structures
│   ├── daft-dsl/            # Expression DSL
│   ├── daft-logical-plan/   # Query planning
│   ├── daft-local-execution/# Execution engine
│   └── ...
├── tests/                   # Python tests
├── Cargo.toml               # Rust dependencies
├── pyproject.toml           # Python dependencies
└── Makefile                 # Build commands
```

### Development Workflow

1. **Setup** (once):
   ```bash
   make .venv  # Create Python environment
   ```

2. **Build** (after Rust changes):
   ```bash
   make build  # Compile Rust → Python extension
   ```

3. **Test** (after changes):
   ```bash
   DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/path/to/test.py"
   ```

4. **Doctest** (for API docs):
   ```bash
   make doctests
   ```

### Common Development Tasks

#### 1. Adding a New Function

**Example**: Add `capitalize` string function

**Step 1**: Implement in Rust (`src/daft-functions-utf8/src/capitalize.rs`):

```rust
use daft_core::prelude::*;
use daft_dsl::ExprRef;

pub fn capitalize(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: FunctionExpr::Utf8(Utf8Expr::Capitalize),
        inputs: vec![input],
    }.into()
}

// Implementation
pub fn eval_capitalize(series: &Series) -> DaftResult<Series> {
    let utf8_array = series.utf8()?;
    let result = utf8_array.apply(|s| {
        let mut chars = s.chars();
        match chars.next() {
            None => String::new(),
            Some(first) => first.to_uppercase().chain(chars).collect(),
        }
    });
    Ok(Series::from(result))
}
```

**Step 2**: Register in function registry (`src/daft-functions-utf8/src/lib.rs`):

```rust
impl FunctionRegistry for Utf8Functions {
    fn register_functions() -> HashMap<String, FunctionImpl> {
        let mut map = HashMap::new();
        // ... existing functions ...
        map.insert("capitalize".to_string(), FunctionImpl {
            name: "capitalize".to_string(),
            evaluator: eval_capitalize,
            return_dtype: |inputs| Ok(DataType::Utf8),
        });
        map
    }
}
```

**Step 3**: Expose in Python (`daft/expressions/expressions.py`):

```python
class ExpressionStringNamespace:
    # ... existing methods ...

    def capitalize(self) -> Expression:
        """
        Capitalize the first character of each string.

        Example:
            >>> df.with_column("name_cap", df["name"].str.capitalize())
        """
        return Expression._from_pyexpr(
            native.utf8_capitalize(self._expr._expr)
        )
```

**Step 4**: Test (`tests/expressions/test_string_functions.py`):

```python
def test_string_capitalize():
    df = daft.from_pydict({"names": ["alice", "bob", "CHARLIE"]})
    result = df.select(df["names"].str.capitalize()).collect()
    assert result.to_pydict() == {"names": ["Alice", "Bob", "CHARLIE"]}
```

---

#### 2. Adding a New Logical Plan Operator

**Example**: Add `SAMPLE` operator for sampling rows

**Step 1**: Define operator struct (`src/daft-logical-plan/src/ops/sample.rs`):

```rust
use std::sync::Arc;
use daft_schema::SchemaRef;
use crate::LogicalPlan;

#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub input: Arc<LogicalPlan>,
    pub fraction: f64,
    pub with_replacement: bool,
    pub seed: Option<u64>,
}

impl Sample {
    pub fn new(
        input: Arc<LogicalPlan>,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> Self {
        Self { input, fraction, with_replacement, seed }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.input.schema()  // Schema unchanged
    }
}
```

**Step 2**: Add to LogicalPlan enum (`src/daft-logical-plan/src/logical_plan.rs`):

```rust
pub enum LogicalPlan {
    // ... existing variants ...
    Sample(Sample),
}

impl LogicalPlan {
    pub fn schema(&self) -> &SchemaRef {
        match self {
            // ... existing cases ...
            LogicalPlan::Sample(sample) => sample.schema(),
        }
    }

    pub fn children(&self) -> Vec<&Arc<LogicalPlan>> {
        match self {
            // ... existing cases ...
            LogicalPlan::Sample(sample) => vec![&sample.input],
        }
    }
}
```

**Step 3**: Add builder method (`src/daft-logical-plan/src/builder.rs`):

```rust
impl LogicalPlanBuilder {
    pub fn sample(
        &self,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> DaftResult<Self> {
        let sample = LogicalPlan::Sample(Sample::new(
            self.plan.clone(),
            fraction,
            with_replacement,
            seed,
        ));
        Ok(LogicalPlanBuilder::new(Arc::new(sample)))
    }
}
```

**Step 4**: Implement physical planning (`src/daft-physical-plan/src/translate.rs`):

```rust
impl LogicalPlan {
    pub fn to_physical(&self) -> DaftResult<PhysicalPlan> {
        match self {
            // ... existing cases ...
            LogicalPlan::Sample(sample) => {
                let input_plan = sample.input.to_physical()?;
                Ok(PhysicalPlan::Sample(PhysicalSample {
                    input: Arc::new(input_plan),
                    fraction: sample.fraction,
                    with_replacement: sample.with_replacement,
                    seed: sample.seed,
                }))
            }
        }
    }
}
```

**Step 5**: Implement execution (`src/daft-local-execution/src/intermediate_ops/sample.rs`):

```rust
pub struct SampleOperator {
    fraction: f64,
    with_replacement: bool,
    seed: u64,
}

impl StreamingOperator for SampleOperator {
    async fn execute(&self, input: MicroPartition) -> DaftResult<MicroPartition> {
        let num_rows = input.len();
        let num_sample = (num_rows as f64 * self.fraction) as usize;

        let mut rng = StdRng::seed_from_u64(self.seed);
        let indices = if self.with_replacement {
            (0..num_sample).map(|_| rng.gen_range(0..num_rows)).collect()
        } else {
            rand::seq::index::sample(&mut rng, num_rows, num_sample).into_vec()
        };

        input.take(&indices)
    }
}
```

**Step 6**: Expose in Python (`daft/dataframe/dataframe.py`):

```python
class DataFrame:
    def sample(
        self,
        fraction: float,
        with_replacement: bool = False,
        seed: int | None = None,
    ) -> DataFrame:
        """
        Sample rows from the DataFrame.

        Args:
            fraction: Fraction of rows to sample (0.0 to 1.0)
            with_replacement: Sample with replacement
            seed: Random seed for reproducibility

        Example:
            >>> df.sample(fraction=0.1, seed=42)
        """
        builder = self._builder.sample(fraction, with_replacement, seed)
        return DataFrame(builder)
```

---

#### 3. Adding a New File Format

**Example**: Add Avro format support

**Step 1**: Create new crate:

```bash
mkdir -p src/daft-avro
cd src/daft-avro
cargo init --lib
```

**Step 2**: Define dependencies (`src/daft-avro/Cargo.toml`):

```toml
[dependencies]
daft-core = { path = "../daft-core" }
daft-io = { path = "../daft-io" }
daft-schema = { path = "../daft-schema" }
apache-avro = "0.14"
arrow2 = { workspace = true }
```

**Step 3**: Implement reader (`src/daft-avro/src/lib.rs`):

```rust
use daft_core::prelude::*;
use daft_io::IOClient;
use apache_avro::{Reader, Schema as AvroSchema};

pub async fn read_avro(
    path: &str,
    io_client: Arc<IOClient>,
) -> DaftResult<Vec<RecordBatch>> {
    // 1. Read file
    let data = io_client.read(path).await?;

    // 2. Parse Avro
    let reader = Reader::new(&data[..])?;
    let avro_schema = reader.writer_schema();

    // 3. Convert schema
    let schema = avro_schema_to_daft(avro_schema)?;

    // 4. Read records
    let mut batches = vec![];
    for record in reader {
        let record = record?;
        // Convert Avro record to RecordBatch
        let batch = avro_record_to_recordbatch(record, &schema)?;
        batches.push(batch);
    }

    Ok(batches)
}
```

**Step 4**: Add to FileFormatConfig (`src/common/file-formats/src/lib.rs`):

```rust
pub enum FileFormatConfig {
    Parquet(ParquetSourceConfig),
    Csv(CsvSourceConfig),
    Json(JsonSourceConfig),
    Avro(AvroSourceConfig),  // NEW
}
```

**Step 5**: Update materialization (`src/daft-micropartition/src/micropartition.rs`):

```rust
fn materialize_scan_task(scan_task: &ScanTask) -> DaftResult<Vec<RecordBatch>> {
    match scan_task.file_format_config {
        // ... existing formats ...
        FileFormatConfig::Avro(_) => {
            daft_avro::read_avro(&scan_task.file_path, io_client).await
        }
    }
}
```

**Step 6**: Expose in Python (`daft/io/_avro.py`):

```python
def read_avro(
    path: str | list[str],
    io_config: IOConfig | None = None,
) -> DataFrame:
    """
    Read Avro files into a DataFrame.

    Args:
        path: File path(s) or glob pattern
        io_config: I/O configuration

    Example:
        >>> df = daft.read_avro("s3://bucket/data/*.avro")
    """
    io_config = io_config or get_context().daft_planning_config.default_io_config

    file_format_config = FileFormatConfig.Avro()
    storage_config = StorageConfig.Native(
        io_config=io_config,
        multithreaded_io=True,
    )

    builder = get_tabular_files_scan(
        path=path,
        file_format_config=file_format_config,
        storage_config=storage_config,
    )

    return DataFrame(builder)
```

---

#### 4. Optimizing Query Plans

**Where**: `src/daft-logical-plan/src/optimization/`

**Example**: Add rule to eliminate redundant projections

**Step 1**: Create rule (`src/daft-logical-plan/src/optimization/rules/eliminate_redundant_project.rs`):

```rust
use crate::LogicalPlan;
use crate::optimization::OptimizerRule;

pub struct EliminateRedundantProject;

impl OptimizerRule for EliminateRedundantProject {
    fn apply(&self, plan: Arc<LogicalPlan>) -> DaftResult<Arc<LogicalPlan>> {
        match plan.as_ref() {
            LogicalPlan::Project(project) => {
                // Check if projection is identity (all columns in order)
                if is_identity_projection(project) {
                    // Eliminate projection
                    return Ok(project.input.clone());
                }

                // Check if child is also projection
                if let LogicalPlan::Project(child_project) = project.input.as_ref() {
                    // Merge projections
                    return merge_projections(project, child_project);
                }
            }
            _ => {}
        }

        // Recursively apply to children
        let new_children = plan.children()
            .iter()
            .map(|child| self.apply(child.clone()))
            .collect::<DaftResult<Vec<_>>>()?;

        plan.with_new_children(new_children)
    }
}
```

**Step 2**: Register rule (`src/daft-logical-plan/src/optimization/mod.rs`):

```rust
pub fn optimize(plan: Arc<LogicalPlan>) -> DaftResult<Arc<LogicalPlan>> {
    let rules: Vec<Box<dyn OptimizerRule>> = vec![
        Box::new(PushDownFilter),
        Box::new(PushDownProjection),
        Box::new(EliminateRedundantProject),  // NEW
        // ... more rules
    ];

    let mut current = plan;
    for rule in rules {
        current = rule.apply(current)?;
    }

    Ok(current)
}
```

---

### Testing Guidelines

**Unit Tests** (Rust):
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_series_add() {
        let s1 = Series::from_vec("a", vec![1, 2, 3]).unwrap();
        let s2 = Series::from_vec("b", vec![4, 5, 6]).unwrap();
        let result = s1.add(&s2).unwrap();
        assert_eq!(result.i64().unwrap().as_slice(), &[5, 7, 9]);
    }
}
```

**Integration Tests** (Python):
```python
def test_filter_and_project():
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
    result = df.filter(df["a"] > 1).select(df["b"]).collect()
    assert result.to_pydict() == {"b": [5, 6]}
```

**Doctests** (Python):
```python
def capitalize(self) -> Expression:
    """
    Capitalize first character.

    Example:
        >>> import daft
        >>> df = daft.from_pydict({"names": ["alice", "bob"]})
        >>> df.select(df["names"].str.capitalize()).collect()
        ╭─────────╮
        │ names   │
        │ ---     │
        │ Utf8    │
        ╞═════════╡
        │ Alice   │
        ├╌╌╌╌╌╌╌╌╌┤
        │ Bob     │
        ╰─────────╯
    """
```

### Performance Tips

1. **Use RecordBatch operations**: Batch operations are faster than row-by-row
2. **Minimize copying**: Use `Arc` for shared data
3. **Leverage Arrow**: Use Arrow kernels when available
4. **Profile**: Use `cargo flamegraph` for Rust profiling
5. **Benchmark**: Use criterion for Rust benchmarks

### Common Pitfalls

1. **Not building after Rust changes**: Run `make build`
2. **Schema mismatches**: Validate schemas carefully
3. **Memory leaks**: Ensure proper cleanup in PyO3 bindings
4. **Type mismatches**: Check DataType compatibility
5. **Null handling**: Always consider null values

---

## Conclusion

Daft is a sophisticated dataframe library with a clean architecture:

- **Python API**: User-friendly, familiar interface
- **Rust Engine**: High-performance execution
- **Lazy Evaluation**: Build plans, optimize, execute
- **MicroPartition-based**: Efficient parallelism
- **Multimodal**: First-class support for complex data types

The modular crate structure enables extensibility while maintaining clear separation of concerns. The PyO3 bindings provide seamless Python-Rust integration with minimal overhead.

**Key Takeaways for Developers**:

1. **Start with Python**: Understand user-facing API first
2. **Follow the data**: Trace data through Series → RecordBatch → MicroPartition
3. **Understand expressions**: Core to all operations
4. **Know the layers**: API → Logical → Physical → Execution
5. **Use existing patterns**: Follow established conventions

**Where to Start**:

- **Add functions**: Start with `daft-functions-*` crates
- **Optimize**: Add optimization rules in `daft-logical-plan`
- **File formats**: Add readers in new `daft-{format}` crates
- **Python API**: Extend DataFrame/Expression classes

**Resources**:

- **Docs**: https://docs.daft.ai
- **Contributing**: CONTRIBUTING.md
- **Code**: https://github.com/Eventual-Inc/Daft
- **Issues**: Good first issues labeled in GitHub

Happy hacking! 🚀
