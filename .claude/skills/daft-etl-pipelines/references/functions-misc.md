# functions misc

## cast

```python
cast(expr: Expression, dtype: DataTypeLike) -> Expression
```

Casts an expression to the given datatype if possible.

See the [casting matrix](https://docs.daft.ai/en/stable/api/datatypes/casting/) for supported casts.

Returns:
    Expression: Expression with the specified new datatype

Note:
    - If a string is provided, it will use the sql engine to parse the string into a data type. See the [SQL Reference](https://docs.daft.ai/en/stable/sql/datatypes/) for supported datatypes.
    - a python `type` can also be provided, in which case the corresponding Daft data type will be used.

Tip: See Also
    [`Expression.cast`](https://docs.daft.ai/en/stable/api/expressions/#daft.expressions.Expression.cast)

## coalesce

```python
coalesce(*args: Expression) -> Expression
```

Returns the first non-null value in a list of expressions. If all inputs are null, returns null.

Args:
    *args: Two or more expressions to coalesce

Returns:
    Expression: Expression containing first non-null value encountered when evaluating arguments in order

## concat

```python
concat(left: Expression | str | bytes, right: Expression | str | bytes) -> Expression
```

Concatenates two string or binary values.

Args:
    left ((String or Binary Expression) | str | bytes): the left value to concatenate
    right ((String or Binary Expression) | str | bytes): the right value to concatenate

Returns:
    Expression: an expression with the same type as the inputs

## eq_null_safe

```python
eq_null_safe(left: Expression, right: Expression) -> Expression
```

Performs a null-safe equality comparison between two expressions.

Unlike regular equality (==), null-safe equality (<=> or IS NOT DISTINCT FROM):
- Returns True when comparing NULL <=> NULL
- Returns False when comparing NULL <=> any_value
- Behaves like regular equality for non-NULL values

Returns:
    Expression (Boolean Expression): A boolean expression indicating if the values are equal

## fill_null

```python
fill_null(expr: Expression, fill_value: Expression) -> Expression
```

Fills null values in the Expression with the provided fill_value.

Returns:
    Expression: Expression with null values filled with the provided fill_value

## get

```python
get(expr: Expression, key: int | str | Expression, default: Any=None) -> Expression
```

Get an index from a list expression or a field from a struct expression.

Args:
    expr (List or Struct Expression): to get value from
    key: integer index for list or string field for struct. List index can be negative to index from the end of the list.
    default: default value if out of bounds. Only supported for list get

Returns:
    An expression with the inner type of the input expression.

Note:
    `expr.get(x)` can also be written as `expr[x]`

Note:
    `expr.get("*")` is equivalent to `expr.unnest()`

## hash

```python
hash(*exprs: Expression, seed: Any | None=None, hash_function: Literal['xxhash', 'xxhash32', 'xxhash64', 'xxhash3_64', 'murmurhash3', 'sha1'] | None='xxhash') -> Expression
```

Hashes the values in the Expression.

Uses the specified hash function to hash the values in the expression. Default to [XXH3_64bits](https://xxhash.com/) non-cryptographic hash function.

Args:
    exprs: One or more expressions (or column names/wildcards) to hash.
    seed (optional): Seed used for generating the hash. Defaults to 0.
    hash_function (optional): Hash function to use. One of "xxhash" (alias for "xxhash3_64"), "xxhash32", "xxhash64", "xxhash3_64", "murmurhash3", or "sha1". Defaults to "xxhash" (alias for "xxhash3_64").

Returns:
    Expression (UInt64 Expression): The hashed expression.

Note:
    Null values will produce a hash value instead of being propagated as null.

## is_in

```python
is_in(expr: Expression, other: Iterable[Any] | Expression) -> Expression
```

Checks if values in the Expression are in the provided iterable.

Args:
    expr: The expression to check
    other: An iterable (list, set, tuple, etc.), Expression, or array-like object containing the values to check against

Returns:
    Expression (Boolean Expression): expression indicating whether values are in the provided iterable

## is_null

```python
is_null(expr: Expression) -> Expression
```

Checks if values in the Expression are Null (a special value indicating missing data).

Returns:
    Expression (Boolean Expression): expression indicating whether values are missing

## length

```python
length(expr: Expression) -> Expression
```

Retrieves the length of the given expression.

Args:
    expr (List or Binary or String Expression): expression to compute the length of.

The behavior depends on the input type:
- For strings, returns the number of characters.
- For binary, returns the number of bytes.
- For lists, returns the number of elements.

Returns:
    Expression (UInt64 Expression): an expression with the length

## map_get

```python
map_get(expr: Expression, key: Expression) -> Expression
```

Retrieves the value for a key in a map column.

Args:
    expr: the map expression to get from
    key: the key to retrieve

Returns:
    Expression: the value expression

## map_keys

```python
map_keys(expr: Expression) -> Expression
```

Returns a list of all keys in the map.

Args:
    expr: the map expression to get from

Returns:
    Expression: the keys list expression

## md5

```python
md5(expr: Expression) -> Expression
```

Computes the MD5 digest for the input expression.

Supports all Daft data types. For lists, elements are sorted before hashing to enable
order-insensitive comparison.

Args:
    expr (Expression): The expression to hash. Can be any data type: string, numeric, list, struct, map, etc.

Returns:
    Expression (Utf8 Expression): A 32-character hexadecimal MD5 hash string. Returns None for null inputs.

Note:
    - For Lists: Elements are sorted before hashing, so [1,2,3] and [3,2,1] produce the same MD5.
    - For Struct/Map: Keys are sorted alphabetically for deterministic hashing.
    - Null values propagate as None (null input -> null output).

## minhash

```python
minhash(text: Expression, *, num_hashes: int, ngram_size: int, seed: int=1, hash_function: Literal['murmurhash3', 'xxhash', 'xxhash32', 'xxhash64', 'xxhash3_64', 'sha1']='murmurhash3') -> Expression
```

Runs the MinHash algorithm on the series.

For a string, calculates the minimum hash over all its ngrams,
repeating with `num_hashes` permutations. Returns as a list of 32-bit unsigned integers.

Tokens for the ngrams are delimited by spaces.
The strings are not normalized or pre-processed, so it is recommended
to normalize the strings yourself.

Args:
    text (String Expression): The expression to hash.
    num_hashes (int): The number of hash permutations to compute.
    ngram_size (int): The number of tokens in each shingle/ngram.
    seed (int, default=1): Seed used for generating permutations and the initial string hashes. Defaults to 1.
    hash_function (str, default="murmurhash3"): Hash function to use for initial string hashing. One of "murmurhash3", "xxhash" (alias for "xxhash3_64"), "xxhash32", "xxhash64", "xxhash3_64", or "sha1". Defaults to "murmurhash3".

Returns:
    Expression (FixedSizedList[UInt32, num_hashes] Expression):
        expression representing the MinHash values.

## monotonically_increasing_id

```python
monotonically_increasing_id() -> Expression
```

Generates a column of monotonically increasing unique ids.

The implementation puts the partition number in the upper 28 bits, and the row number in each partition
in the lower 36 bits. This allows for 2^28 ≈ 268 million partitions and 2^36 ≈ 68 billion rows per partition.

Returns:
    Expression (UInt64 Expression): An expression that generates monotonically increasing IDs

## not_null

```python
not_null(expr: Expression) -> Expression
```

Checks if values in the Expression are not Null (a special value indicating missing data).

Returns:
    Expression (Boolean Expression): expression indicating whether values are not missing

## random_int

```python
random_int(low: int, high: int, seed: int | None=None) -> Expression
```

Generates a column of random integer values.

Values are generated uniformly and independently in the closed interval ``[low, high]``.

Passing a ``seed`` makes generation best-effort stable for the same evaluated row layout,
but it is not guaranteed to be reproducible across repartitioning or other layout changes.

Args:
    low: Inclusive lower bound.
    high: Inclusive upper bound.
    seed: Optional seed for best-effort stable generation.

Returns:
    Expression (Int64 Expression): An expression that generates random integers.

## simhash

```python
simhash(text: Expression, *, ngram_size: int=3, hash_function: Literal['murmurhash3', 'xxhash', 'xxhash32', 'xxhash64', 'xxhash3_64', 'sha1']='xxhash3_64') -> Expression
```

Compute a SimHash fingerprint of the input text.

SimHash produces a 64-bit locality-sensitive hash from character n-grams.
Similar texts produce fingerprints with small bitwise Hamming distance,
making it useful for near-duplicate detection.

Args:
    text (String Expression): The expression to hash.
    ngram_size (int, default=3): Character n-gram size. Defaults to 3.
    hash_function (str, default="xxhash3_64"): Hash function for n-grams. One of "murmurhash3", "xxhash" (alias for "xxhash3_64"), "xxhash32", "xxhash64", "xxhash3_64", or "sha1". Defaults to "xxhash3_64".

Returns:
    Expression (UInt64 Expression): SimHash fingerprint.

## slice

```python
slice(expr: Expression, start: int | Expression, end: int | Expression | None=None) -> Expression
```

Get a subset of each list or binary value.

Args:
    expr: List or binary expression to slice.
    start: Index or column of indices. The slice will include elements starting from this index. If `start` is negative, it represents an offset from the end
    end: Index or column of indices. The slice will not include elements from this index onwards. If `end` is negative, it represents an offset from the end. If not provided, the slice will include elements up to the end of the list. If start > end, an empty slice is produced.

Returns:
    Expression: an expression with the same type as the input.

Note:
    `expr[start:stop]` is also equivalent to `expr.slice(start, stop)`

## try_cast

```python
try_cast(expr: Expression, dtype: DataTypeLike) -> Expression
```

Attempts to cast an expression to the given datatype, returning null on failure.

Unlike `cast`, this function does not raise an error when the conversion fails.
Instead, it returns null for values that cannot be converted.

Returns:
    Expression: Expression with the specified new datatype, with null for failed conversions

Note:
    - If a string is provided, it will use the sql engine to parse the string into a data type.
    - A python `type` can also be provided, in which case the corresponding Daft data type will be used.

## uuid

```python
uuid(version: Literal['v4', 'v7']='v4') -> Expression
```

Generates a column of UUID strings.

Each call to `uuid()` generates a fresh UUID per row. Multiple calls in the same query
(e.g. two separate columns) are independent and will produce different values.

Use the ``version`` argument to choose the UUID version:

- ``uuid()`` or ``uuid(version="v4")`` generates random UUIDv4 values.
- ``uuid(version="v7")`` generates time-ordered UUIDv7 values.

Args:
    version: UUID version to generate. Supported values are ``"v4"`` and ``"v7"``.

Returns:
    Expression (UUID Expression): An expression that generates UUID values.

## when

```python
when(condition: Expression | bool, then: Expression | Any) -> WhenExpr
```

Start a conditional expression, similar to SQL CASE WHEN.

If the condition is true, the `then` value will be returned. Otherwise, the next `when` condition will be evaluated.
If no conditions are true, the value will be set to the value provided in the `otherwise` clause, or null if not provided.

Args:
    condition: The Boolean expression to evaluate
    then: Expression to return when the condition is true

Returns:
    A WhenExpr that can be chained with more `when` clauses and ended with `otherwise`
