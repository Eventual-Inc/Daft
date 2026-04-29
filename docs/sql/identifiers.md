# Identifiers

Daft's SQL identifiers are **case-sensitive by default**, but support a case-insensitive and a case-normalize mode. For both the case-insensitive and case-sensitive modes, identifiers are case-preserved and are matched based upon the mode. For the case-normalize mode, unquoted (regular) identifiers are normalized to lowercase and double-quoted (delimited) identifiers are case-preserved. You can configure these modes via the `SessionOptions` when creating a session. These modes apply when resolving attached catalogs, attached tables, and columns via the session.

!!! warning "Warning"

    Catalogs such as Iceberg and Unity have incompatible resolution rules which means we cannot guarantee consistency across catalog implementations.

It is currently not feasible to ensure consistent casing semantics across catalogs, so we recommend using the different modes to find which best
fits your preferences and current systems. When working across multiple systems, using all lowercase names for namespace and tables with `identifier_mode = 'normalize'` provides the most consistent experience.

## Syntax

```sql
-- regular identifier
abc
```

```sql
-- delimited identifier
"abc"
```

```sql
-- qualified identifier
abc.xyz
```

```sql
-- qualified identifier with mixed parts
a."b".c
```

```sql
-- delimited identifier with special characters
SELECT "🍺" FROM "🍻"
```

**Rules**

* Identifiers may be unquoted (regular) or double-quoted (delimited).
* Identifiers must be double-quoted if the text is a keyword or the text contains special characters.
* Regular identifiers must start with either an [alphabetic character](https://www.unicode.org/Public/UCD/latest/ucd/DerivedCoreProperties.txt) or an underscore `'_'` character.
* Regular identifiers must contain only alphanumeric, `'$'` and `'_'` characters.

## Modes

!!! tip ""

    We recommend trying these settings to determine which works best for your workloads.

| Mode          | Behavior                                                                     | Compatibility                     |
|---------------|------------------------------------------------------------------------------|-----------------------------------|
| `insensitive` | All identifiers are matched case-insensitively and names are case-preserved. | `duckdb`, `spark`, `unity`        |
| `sensitive`   | All identifiers are matched case-sensitively and names are case-preserved.   | `python`, `iceberg`               |
| `normalize`   | Unquoted (regular) identifiers and names are case-normalized to lowercase.   | `trino`, `postgres`, `datafusion` |



## Configuration

You can configure the mode using the `identifier_mode` option when creating a session:

```python
from daft import Session
from daft.session import SessionOptions, IdentifierMode

# Create a session with case-insensitive identifiers
opts = SessionOptions(identifier_mode=IdentifierMode.INSENSITIVE)
sess = Session(opts)

# Or use case-normalize mode
opts = SessionOptions(identifier_mode=IdentifierMode.NORMALIZE)
sess = Session(opts)
```
