# Read Arrow

When compiled with feature `io_ipc`, this crate can be used to memory map IPC Arrow files
into arrays.

The example below shows how to memory map an IPC Arrow file into `Chunk`es:

```rust
{{#include ../../../examples/ipc_file_mmap.rs}}
```
