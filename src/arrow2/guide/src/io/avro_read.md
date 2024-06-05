# Avro read

When compiled with feature `io_avro_async`, you can use this crate to read Avro files
asynchronously.

```rust
{{#include ../../../examples/avro_read_async.rs}}
```

Note how both decompression and deserialization is performed on a separate thread pool to not
block (see also [here](https://ryhl.io/blog/async-what-is-blocking/)).
