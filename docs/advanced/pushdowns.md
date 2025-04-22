# Pushdowns

!!! warning "Warning"

    These APIs are considered experimental.

Daft supports predicate, projection, and limit pushdowns in its scan operators.

## Usage

Daft does not *officially* support user-defined scan operators, but it is
possible today by extending the [`ScanOperator`][daft.io.scan.ScanOperator].

The [`to_scan_tasks`][daft.io.scan.ScanOperator.to_scan_tasks] method receives a
pushdowns object which contains information the optimizer determined can be
pushed down into the scan. This is a wrapper over a Rust type, and its members
cannot be easily interpreted from python. To solve this, we use a python
[`Pushdowns`][daft.io.pushdowns.Pushdowns] class holding
[`Term`][daft.io.pushdowns.Term] objects which are a representation of the
pushed-down expressions. You can construct a
[`ScanPushdowns`][daft.io.scan.ScanPushdowns] object using `_from_pypushdowns(pushdowns, schema)`.

Once you have the pushdown terms, you will need to apply them based upon
your own context. One way to operate on a term tree is using the
[`TermVisitor`][daft.io.pushdowns.TermVisitor].

## Term Reference

!!! warn "Warning!"

    This list is not comprehensive, and it is recommended to either inspect
    a term tree or test your

### Comparison Predicates

| Procedure | Arguments  | Description              |
|-----------|------------|--------------------------|
| `=`       | `lhs, rhs` | Equal to                 |
| `!=`      | `lhs, rhs` | Not equal to             |
| `<`       | `lhs, rhs` | Less than                |
| `<=`      | `lhs, rhs` | Less than or equal to    |
| `>`       | `lhs, rhs` | Greater than             |
| `>=`      | `lhs, rhs` | Greater than or equal to |

### Logical Predicates

| Procedure | Arguments  | Description |
|-----------|------------|-------------|
| `and`     | `lhs, rhs` | Logical AND |
| `or`      | `lhs, rhs` | Logical OR  |
| `not`     | `expr`     | Logical NOT |

### Other Predicates

| Procedure  | Arguments            | Description                                    |
|------------|----------------------|------------------------------------------------|
| `is_null`  | `expr`               | Check if expression is NULL                    |
| `is_nan`   | `expr`               | Check if expression is NaN                     |
| `not_null` | `expr`               | Check if expression is not NULL                |
| `not_nan`  | `expr`               | Check if expression is not NaN                 |
| `between`  | `expr, lower, upper` | Check if expression is between lower and upper |
| `is_in`    | `item, list`         | Check if item is in list                       |

### Expressions

Terms may represent any arbitrary expression, not just predicates!

| Procedure | Arguments  | Description                 |
|-----------|------------|-----------------------------|
| `+`       | `lhs, rhs` | Arithmetic addition         |
| `-`       | `lhs, rhs` | Arithmetic subtraction      |
| `*`       | `lhs, rhs` | Arithmetic multiplication   |
| `/`       | `lhs, rhs` | Arithmetic division         |
| `mod`     | `lhs, rhs` | Modulus                     |
| `lshift`  | `lhs, rhs` | Bitshift left               |
| `rshift`  | `lhs, rhs` | Bitshift right              |
| `list`    | `items...` | List constructor or literal |

### Functions

Terms for functions are uniformly represented with the function name as the
procedure name followed by the function's arguments. For example, here is
how the Iceberg partition transforms are represented as terms.

| Function           | Arguments     | Description                          |
|--------------------|---------------|--------------------------------------|
| `years`            | `expr`        | Extract years from a date/timestamp  |
| `months`           | `expr`        | Extract months from a date/timestamp |
| `days`             | `expr`        | Extract days from a date/timestamp   |
| `hours`            | `expr`        | Extract hours from a timestamp       |
| `iceberg_bucket`   | `expr, n`     | Iceberg bucketing function           |
| `iceberg_truncate` | `expr, width` | Iceberg truncate function            |
