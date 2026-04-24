# PyIceberg Expression Reference

Developer reference for the pyiceberg expression types used by `IcebergPredicateVisitor`.

## Type Hierarchy

```
BooleanExpression (abstract)
├── And(left, right)
├── Or(left, right)
├── Not(child)
├── AlwaysTrue()                          — singleton
├── AlwaysFalse()                         — singleton
├── BoundPredicate(Bound, BooleanExpression)   ← what we construct
│   ├── BoundUnaryPredicate(term: BoundTerm)
│   │   ├── BoundIsNull
│   │   ├── BoundNotNull
│   │   ├── BoundIsNaN
│   │   └── BoundNotNaN
│   ├── BoundSetPredicate(term: BoundTerm, literals: set[Literal])
│   │   ├── BoundIn
│   │   └── BoundNotIn
│   └── BoundLiteralPredicate(term: BoundTerm, literal: Literal)
│       ├── BoundEqualTo
│       ├── BoundNotEqualTo
│       ├── BoundLessThan
│       ├── BoundLessThanOrEqual
│       ├── BoundGreaterThan
│       ├── BoundGreaterThanOrEqual
│       ├── BoundStartsWith
│       └── BoundNotStartsWith
└── UnboundPredicate(Unbound, BooleanExpression)  ← NOT used by visitor
    └── ...

Term
├── BoundTerm(Term, Bound)
│   └── BoundReference(field: NestedField, accessor: Accessor)
└── UnboundTerm(Term, Unbound)
    └── Reference(name: str)

Literal[L] (abstract)
├── BooleanLiteral(bool)
├── LongLiteral(int)
├── FloatLiteral(float)
├── DoubleLiteral(float)
├── DateLiteral(int)          — days since epoch
├── TimeLiteral(int)          — microseconds since midnight
├── TimestampLiteral(int)     — microseconds since epoch
├── DecimalLiteral(Decimal)
├── StringLiteral(str)
├── BinaryLiteral(bytes)
├── UUIDLiteral(bytes)
├── AboveMax / BelowMin       — sentinels for out-of-range conversion
```

## Bound vs Unbound

| Aspect | Unbound | Bound |
|--------|---------|-------|
| Schema aware | No | Yes |
| Term type | `Reference(name)` | `BoundReference(field, accessor)` |
| Created by | `EqualTo("col", 42)` | `BoundEqualTo(bound_ref, literal)` |
| Binding | `.bind(schema)` → Bound | Already bound |

Our visitor constructs **Bound** predicates directly because we have the Iceberg schema at visitor construction time. This avoids a separate bind pass and gives us type-aware literal conversion up front.

## Constructing Bound Expressions

### BoundReference (from column name)

```python
from pyiceberg.expressions import Reference
ref: BoundReference = Reference("column_name").bind(schema)
# ref.field → NestedField (has .field_type for literal conversion)
# ref.ref() → self
```

### Literals (from Python values)

```python
from pyiceberg.expressions.literals import literal
lit = literal(42)          # → LongLiteral(42)
lit = literal("hello")    # → StringLiteral("hello")
lit = literal(3.14)       # → DoubleLiteral(3.14)

# Type conversion to match field type:
typed_lit = lit.to(ref.ref().field.field_type)
```

### Bound Predicates

```python
from pyiceberg.expressions import (
    BoundEqualTo, BoundLessThan, BoundIn, BoundIsNull,
    And, Or, Not,
)

# Comparison: BoundLiteralPredicate(term: BoundTerm, literal: Literal)
BoundEqualTo(ref, typed_lit)
BoundLessThan(ref, typed_lit)

# Null check: BoundUnaryPredicate(term: BoundTerm)
BoundIsNull(ref)

# Set: BoundSetPredicate(term: BoundTerm, literals: set[Literal])
BoundIn(ref, {lit1, lit2, lit3})

# Logical combinators (NOT Bound, but are BooleanExpression)
And(left_expr, right_expr)
Or(left_expr, right_expr)
Not(child_expr)
```

## Smart Constructor Simplifications

PyIceberg constructors automatically simplify:

- `And(AlwaysFalse(), X)` → `AlwaysFalse()`
- `And(AlwaysTrue(), X)` → `X`
- `Or(AlwaysTrue(), X)` → `AlwaysTrue()`
- `Or(AlwaysFalse(), X)` → `X`
- `Not(Not(X))` → `X`
- `BoundIsNull(required_field)` → `AlwaysFalse()`
- `BoundNotNull(required_field)` → `AlwaysTrue()`
- `BoundIn(ref, set())` → `AlwaysFalse()`
- `BoundIn(ref, {single})` → `BoundEqualTo(ref, single)`

## Literal Type Conversion

`Literal.to(IcebergType)` converts between types. Returns `AboveMax`/`BelowMin` sentinels when out of range, which predicate constructors use to simplify (e.g., `LessThan(x, AboveMax)` → `AlwaysTrue()`).

Key conversions:
- `LongLiteral` → IntegerType, FloatType, DoubleType, DateType, TimestampType, DecimalType
- `DoubleLiteral` → FloatType, DecimalType
- `StringLiteral` → most types (parsed)
