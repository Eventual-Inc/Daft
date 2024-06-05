# CSV reader

When compiled with feature `io_csv`, you can use this crate to read CSV files.
This crate makes minimal assumptions on how you want to read a CSV, and offers a large degree of customization to it, along with a useful default.

## Background

There are two CPU-intensive tasks in reading a CSV file:
* split the CSV file into rows, which includes parsing quotes and delimiters, and is necessary to `seek` to a given row.
* parse a set of CSV rows (bytes) into a `Array`s.

Parsing bytes into values is more expensive than interpreting lines. As such, it is generally advantageous to have multiple readers of a single file that scan different parts of the file (within IO constraints).

This crate relies on [the crate `csv`](https://crates.io/crates/csv) to scan and seek CSV files, and your code also needs such a dependency. With that said, `arrow2` makes no assumptions as to how to efficiently read the CSV: as a single reader per file or multiple readers.

As an example, the following infers the schema and reads a CSV by re-using the same reader:

```rust
{{#include ../../../examples/csv_read.rs}}
```

## Orchestration and parallelization

Because `csv`'s API is synchronous, the functions above represent the "minimal
unit of synchronous work", IO and CPU. Note that `rows` above are `Send`,
which implies that it is possible to run `parse` on a separate thread,
thereby maximizing IO throughput. The example below shows how to do just that:

```rust
{{#include ../../../examples/csv_read_parallel.rs}}
```

## Async

This crate also supports reading from a CSV asynchronously through the `csv-async` crate.
The example below demonstrates this:

```rust
{{#include ../../../examples/csv_read_async.rs}}
```

Note that the deserialization _should_ be performed on a separate thread to not
block (see also [here](https://ryhl.io/blog/async-what-is-blocking/)), which this
example does not show.

## Customization

In the code above, `parser` and `infer` allow for customization: they declare
how rows of bytes should be inferred (into a logical type), and processed (into a value of said type).
They offer good default options, but you can customize the inference and parsing to your own needs.
You can also of course decide to parse everything into memory as `Utf8Array` and
delay any data transformation.
