# Read Arrow

When compiled with feature `io_ipc`, this crate can be used to read Arrow files.

An Arrow file is composed by a header, a footer, and blocks of `Array`s.
Reading it generally consists of:

1. read metadata, containing the block positions in the file
2. seek to each block and read it

The example below shows how to read them into `Chunk`es:

```rust
{{#include ../../../examples/ipc_file_read.rs}}
```
