# Read Arrow streams

When compiled with feature `io_ipc`, this crate can be used to read Arrow streams.

The example below shows how to read from a stream:

```rust
{{#include ../../../examples/ipc_pyarrow/src/main.rs}}
```

e.g. written by pyarrow:

```python,ignore
{{#include ../../../examples/ipc_pyarrow/main.py}}
```

via

```bash,ignore
{{#include ../../../examples/ipc_pyarrow/run.sh}}
```
