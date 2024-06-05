# Metadata

```rust
{{#include ../../examples/metadata.rs}}
```

## `DataType` (Logical types)

The Arrow specification contains a set of logical types, an enumeration of the different
semantical types defined in Arrow.

In Arrow2, logical types are declared as variants of the `enum` `arrow2::datatypes::DataType`.
For example, `DataType::Int32` represents a signed integer of 32 bits.

Each `DataType` has an associated `enum PhysicalType` (many-to-one) representing the
particular in-memory representation, and is associated to a specific semantics.
For example, both `DataType::Date32` and `DataType::Int32` have the same `PhysicalType`
(`PhysicalType::Primitive(PrimitiveType::Int32)`) but `Date32` represents the number of
days since UNIX epoch.

Logical types are metadata: they annotate physical types with extra information about data.

## `Field` (column metadata)

Besides logical types, the arrow format supports other relevant metadata to the format.
An important one is `Field` broadly corresponding to a column in traditional columnar formats.
A `Field` is composed by a name (`String`), a logical type (`DataType`), whether it is
nullable (`bool`), and optional metadata.

## `Schema` (table metadata)

The most common use of `Field` is to declare a `arrow2::datatypes::Schema`, a sequence of `Field`s
with optional metadata.

`Schema` is essentially metadata of a "table": it has a sequence of named columns and their metadata (`Field`s) with optional metadata.
