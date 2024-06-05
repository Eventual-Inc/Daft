# Write Arrow

When compiled with feature `io_ipc`, this crate can be used to write Arrow files.

An Arrow file is composed by a header, a footer, and blocks of `RecordBatch`es.

The example below shows how to write `RecordBatch`es:

```rust
{{#include ../../../examples/ipc_file_write.rs}}
```
