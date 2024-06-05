# Compute API

When compiled with the feature `compute`, this crate offers a wide range of functions
to perform both vertical (e.g. add two arrays) and horizontal
(compute the sum of an array) operations.

The overall design of the `compute` module is that it offers two APIs:

* statically typed, such as `sum_primitive<T>(&PrimitiveArray<T>) -> Option<T>`
* dynamically typed, such as `sum(&dyn Array) -> Box<dyn Scalar>`

the dynamically typed API usually has a function `can_*(&DataType) -> bool` denoting whether
the operation is defined for the particular logical type.

Overview of the implemented functionality:

* arithmetics, checked, saturating, etc.
* `sum`, `min` and `max`
* `unary`, `binary`, etc.
* `comparison`
* `cast`
* `take`, `filter`, `concat`
* `sort`, `hash`, `merge-sort`
* `if-then-else`
* `nullif`
* `length` (of string)
* `hour`, `year`, `month`, `iso_week` (of temporal logical types)
* `regex`
* (list) `contains`

and an example of how to use them:

```rust
{{#include ../../examples/arithmetics.rs}}
```
