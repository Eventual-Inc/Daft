# Write CSV

When compiled with feature `io_csv`, you can use this crate to write CSV files.

This crate relies on [the crate csv](https://crates.io/crates/csv) to write well-formed CSV files, which your code should also depend on.

The following example writes a batch as a CSV file with the default configuration:

```rust
{{#include ../../../examples/csv_write.rs}}
```

## Parallelism

This crate exposes functionality to decouple serialization from writing.

In the example above, the serialization and writing to a file is done synchronously.
However, these typically deal with different bounds: serialization is often CPU bounded, while writing is often IO bounded. We can trade-off these through a higher memory usage.

Suppose that we know that we are getting CPU-bounded at serialization, and would like to offload that workload to other threads, at the cost of a higher memory usage. We would achieve this as follows (two batches for simplicity):

```rust
{{#include ../../../examples/csv_write_parallel.rs}}
```
