# JSON read

When compiled with feature `io_json`, you can use this crate to read NDJSON files:

```rust
{{#include ../../../examples/ndjson_read.rs}}
```

Note how deserialization can be performed on a separate thread pool to avoid
blocking the runtime (see also [here](https://ryhl.io/blog/async-what-is-blocking/)).

This crate also supports reading JSON, at the expense of being unable to read the file in chunks.

```rust
{{#include ../../../examples/json_read.rs}}
```

## Metadata and inference

This crate uses the following mapping between Arrow's data type and JSON:

| `JSON` | `DataType` |
| ------ | ---------- |
| Bool   | Boolean    |
| Int    | Int64      |
| Float  | Float64    |
| String | Utf8       |
| List   | List       |
| Object | Struct     |
