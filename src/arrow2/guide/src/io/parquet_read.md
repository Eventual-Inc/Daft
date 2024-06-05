# Read parquet

When compiled with feature `io_parquet`, this crate can be used to read parquet files
to arrow.
It makes minimal assumptions on how you to decompose CPU and IO intensive tasks.

First, some notation:

* `page`: part of a column (e.g. similar to a slice of an `Array`)
* `column chunk`: composed of multiple pages (similar to an `Array`)
* `row group`: a group of columns with the same length (similar to a `Chunk`)

Here is how to read a single column chunk from a single row group:

```rust
{{#include ../../../examples/parquet_read.rs}}
```

The example above minimizes memory usage at the expense of mixing IO and CPU tasks
on the same thread, which may hurt performance if one of them is a bottleneck.

### Parallelism decoupling of CPU from IO

One important aspect of the pages created by the iterator above is that they can cross
thread boundaries. Consequently, the thread reading pages from a file (IO-bounded)
does not have to be the same thread performing CPU-bounded work (decompressing,
decoding, etc.).

The example below assumes that CPU starves the consumption of pages,
and that it is advantageous to have a single thread performing all IO-intensive work,
by delegating all CPU-intensive tasks to separate threads.

```rust
{{#include ../../../examples/parquet_read_parallel/src/main.rs}}
```

This can of course be reversed; in configurations where IO is bounded (e.g. when a
network is involved), we can use multiple producers of pages, potentially divided
in file readers, and a single consumer that performs all CPU-intensive work.

## Apache Arrow <-> Apache Parquet

Arrow and Parquet are two different formats that declare different physical and logical types.
When reading Parquet, we must _infer_ to which types we are reading the data to.
This inference is based on Parquet's physical, logical and converted types.

When a logical type is defined, we use it as follows:

| `Parquet`         | `Parquet logical` | `DataType`    |
| ----------------- | ----------------- | ------------- |
| Int32             | Int8              | Int8          |
| Int32             | Int16             | Int16         |
| Int32             | Int32             | Int32         |
| Int32             | UInt8             | UInt8         |
| Int32             | UInt16            | UInt16        |
| Int32             | UInt32            | UInt32        |
| Int32             | Decimal           | Decimal       |
| Int32             | Date              | Date32        |
| Int32             | Time(ms)          | Time32(ms)    |
| Int64             | Int64             | Int64         |
| Int64             | UInt64            | UInt64        |
| Int64             | Time(us)          | Time64(us)    |
| Int64             | Time(ns)          | Time64(ns)    |
| Int64             | Timestamp(\_)     | Timestamp(\_) |
| Int64             | Decimal           | Decimal       |
| ByteArray         | Utf8              | Utf8          |
| ByteArray         | JSON              | Binary        |
| ByteArray         | BSON              | Binary        |
| ByteArray         | ENUM              | Binary        |
| ByteArray         | Decimal           | Decimal       |
| FixedLenByteArray | Decimal           | Decimal       |

When a logical type is not defined but a converted type is defined, we use
the equivalent conversion as above, mutatis mutandis.

When neither is defined, we fall back to the physical representation:

| `Parquet`         | `DataType`      |
| ----------------- | --------------- |
| Boolean           | Boolean         |
| Int32             | Int32           |
| Int64             | Int64           |
| Int96             | Timestamp(ns)   |
| Float             | Float32         |
| Double            | Float64         |
| ByteArray         | Binary          |
| FixedLenByteArray | FixedSizeBinary |
