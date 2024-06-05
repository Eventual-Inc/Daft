# Write to Parquet

When compiled with feature `io_parquet`, this crate can be used to write parquet files
from arrow.
It makes minimal assumptions on how you to decompose CPU and IO intensive tasks, as well
as an higher-level API to abstract away some of this work into an easy to use API.

First, some notation:

* `page`: part of a column (e.g. similar to a slice of an `Array`)
* `column chunk`: composed of multiple pages (similar to an `Array`)
* `row group`: a group of columns with the same length (similar to a `Chunk` in Arrow)

## Single threaded

Here is an example of how to write a single chunk:

```rust
{{#include ../../../examples/parquet_write.rs}}
```

## Multi-threaded writing

As user of this crate, you will need to decide how you would like to parallelize,
and whether order is important. Below you can find an example where we
use [`rayon`](https://crates.io/crates/rayon) to perform the heavy lift of
encoding and compression.
This operation is [embarrassingly parallel](https://en.wikipedia.org/wiki/Embarrassingly_parallel)
and results in a speed up equal to minimum between the number of cores
and number of columns in the record.

```rust
{{#include ../../../examples/parquet_write_parallel/src/main.rs}}
```
