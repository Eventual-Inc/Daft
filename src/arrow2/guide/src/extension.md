# Extension types

This crate supports Arrows' ["extension type"](https://arrow.apache.org/docs/format/Columnar.html#extension-types), to declare, use, and share custom logical types.

An extension type is just a `DataType` with a name and some metadata.
In particular, its physical representation is equal to its inner `DataType`, which implies
that all functionality in this crate works as if it was the inner `DataType`.

The following example shows how to declare one:

```rust
{{#include ../../examples/extension.rs}}
```
