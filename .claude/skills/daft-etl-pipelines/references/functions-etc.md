# functions etc

## bitwise_and

```python
bitwise_and(left: Expression, right: Expression) -> Expression
```

Bitwise AND of two integer expressions.

## bitwise_or

```python
bitwise_or(left: Expression, right: Expression) -> Expression
```

Bitwise OR of two integer expressions.

## bitwise_xor

```python
bitwise_xor(left: Expression, right: Expression) -> Expression
```

Bitwise XOR of two integer expressions.

## columns_avg

```python
columns_avg(*exprs: Expression | str) -> Expression
```

Average values across columns. Akin to `columns_mean`.

Args:
    exprs: The columns to average across.

## columns_max

```python
columns_max(*exprs: Expression | str) -> Expression
```

Find the maximum value across columns.

Args:
    exprs: The columns to find the maximum of.

## columns_mean

```python
columns_mean(*exprs: Expression | str) -> Expression
```

Average values across columns. Akin to `columns_avg`.

Args:
    exprs: The columns to average.

## columns_min

```python
columns_min(*exprs: Expression | str) -> Expression
```

Find the minimum value across columns.

Args:
    exprs: The columns to find the minimum of.

## columns_sum

```python
columns_sum(*exprs: Expression | str) -> Expression
```

Sum values across columns.

Args:
    exprs: The columns to sum.

## compress

```python
compress(expr: Expression, codec: COMPRESSION_CODEC) -> Expression
```

Compress binary or string values using the specified codec.

Args:
    expr (String | Binary Expression): The expression to compress.
    codec (str) The compression codec (deflate, gzip, or zlib)

Returns:
    Expression (Binary Expression): A binary expression with the compressed value.

## cosine_distance

```python
cosine_distance(left: Expression, right: Expression) -> Expression
```

Compute the cosine distance between two embeddings.

Args:
    left (FixedSizeList or Embedding Expression): The left vector
    right (FixedSizeList or Embedding Expression): The right vector

Returns:
    Expression (Float64 Expression): an expression with the cosine distance between the two vectors.

## cosine_similarity

```python
cosine_similarity(left: Expression, right: Expression) -> Expression
```

Compute the cosine similarity between two embeddings.

Args:
    left (FixedSizeList or Embedding Expression): The left vector
    right (FixedSizeList or Embedding Expression): The right vector

Returns:
    Expression (Float64 Expression): the cosine similarity between the two vectors.

## decode

```python
decode(bytes: Expression, charset: ENCODING_CHARSET) -> Expression
```

Decodes binary values using the specified character set.

Note that if the charset is "utf-8" or "utf8", then this is equivalent
to cast(bytes, daft.DataType.string())

If an invalid encoding is encountered, an error will be raised.
To handle invalid encodings, use `try_decode` instead.

Args:
    bytes (Binary Expression): The expression to decode.
    charset (str): The decoding character set (utf-8, base64).

Returns:
    Expression (Binary Expression): A binary expression with the decoded values.

## decompress

```python
decompress(bytes: Expression, codec: COMPRESSION_CODEC) -> Expression
```

Decompress binary values using the specified codec.

Args:
    bytes (Binray Expression): The binary expression to decompress.
    codec (str): The decompression codec (deflate, gzip, zlib)

Returns:
    Expression: A binary expression with the decoded values.

## dot_product

```python
dot_product(left: Expression, right: Expression) -> Expression
```

Compute the dot product between two embeddings.

Args:
    left (FixedSizeList or Embedding Expression): The left vector
    right (FixedSizeList or Embedding Expression): The right vector

Returns:
    Expression (Float64 Expression): the dot product between the two vectors.

## encode

```python
encode(expr: Expression, charset: ENCODING_CHARSET) -> Expression
```

Encode binary or string values using the specified character set.

If an invalid encoding is encountered, an error will be raised.
To handle invalid encodings, use `try_encode` instead.

Args:
    expr (Binary or String Expression): The expression to encode.
    charset (str): The encoding character set (utf-8, base64).

Returns:
    Expression (Binary Expression): A binary expression with the encoded value.

Note:
    This inputs either a string or binary and returns a binary.
    If the input value is a string and 'utf-8' is the character set, then it's just a cast to binary.
    If the input value is a binary and 'utf-8' is the character set, we verify the bytes are valid utf-8.

## euclidean_distance

```python
euclidean_distance(left: Expression, right: Expression) -> Expression
```

Compute the Euclidean distance between two embeddings.

Args:
    left (FixedSizeList or Embedding Expression): The left vector
    right (FixedSizeList or Embedding Expression): The right vector

Returns:
    Expression (Float64 Expression): the Euclidean distance between the two vectors.

## extract_day_uuid7

```python
extract_day_uuid7(expr: Expression) -> Expression
```

Partitioning Transform that extracts the number of days since epoch (1970-01-01) from a UUIDv7.

A UUIDv7 embeds a 48-bit Unix-millisecond timestamp in its first 6 bytes. The input must be a
Uuid or a FixedSizeBinary of 16 bytes (128 bits).

Args:
    expr: a Uuid or FixedSizeBinary(16) expression of UUIDv7 values

Returns:
    Expression: Int64 Expression with the number of days since epoch

## extract_hour_uuid7

```python
extract_hour_uuid7(expr: Expression) -> Expression
```

Partitioning Transform that extracts the number of hours since epoch (1970-01-01) from a UUIDv7.

A UUIDv7 embeds a 48-bit Unix-millisecond timestamp in its first 6 bytes. The input must be a
Uuid or a FixedSizeBinary of 16 bytes (128 bits).

Args:
    expr: a Uuid or FixedSizeBinary(16) expression of UUIDv7 values

Returns:
    Expression: Int64 Expression with the number of hours since epoch

## extract_minute_uuid7

```python
extract_minute_uuid7(expr: Expression) -> Expression
```

Partitioning Transform that extracts the number of minutes since epoch (1970-01-01) from a UUIDv7.

A UUIDv7 embeds a 48-bit Unix-millisecond timestamp in its first 6 bytes. The input must be a
Uuid or a FixedSizeBinary of 16 bytes (128 bits).

Args:
    expr: a Uuid or FixedSizeBinary(16) expression of UUIDv7 values

Returns:
    Expression: Int64 Expression with the number of minutes since epoch

## extract_month_uuid7

```python
extract_month_uuid7(expr: Expression) -> Expression
```

Partitioning Transform that extracts the number of calendar months since 1970-01 from a UUIDv7.

The result is `(year - 1970) * 12 + (month - 1)`, matching `partition_months`. A UUIDv7 embeds a
48-bit Unix-millisecond timestamp in its first 6 bytes. The input must be a Uuid or a
FixedSizeBinary of 16 bytes (128 bits).

Args:
    expr: a Uuid or FixedSizeBinary(16) expression of UUIDv7 values

Returns:
    Expression: Int64 Expression with the number of months since 1970-01

## hamming_distance

```python
hamming_distance(left: Expression, right: Expression) -> Expression
```

Compute the Hamming distance (number of differing bits) between two hash fingerprints.

Counts the number of differing bits (popcount of XOR).
Supports integer inputs (e.g., UInt64 from simhash) and FixedSizeBinary
inputs (e.g., from image_hash).

Args:
    left (Integer or FixedSizeBinary Expression): The left fingerprint.
    right (Integer or FixedSizeBinary Expression): The right fingerprint (must match left's type).

Returns:
    Expression (UInt32 Expression): Number of differing bits.

## jaccard_similarity

```python
jaccard_similarity(left: Expression, right: Expression) -> Expression
```

Compute the Jaccard similarity between two embeddings.

The Jaccard similarity is computed by treating non-zero elements as set
membership and comparing intersection over union.

Args:
    left (FixedSizeList or Embedding Expression): The left vector
    right (FixedSizeList or Embedding Expression): The right vector

Returns:
    Expression (Float64 Expression): the Jaccard similarity between the two vectors.

## llm_generate

```python
llm_generate(text: Expression, model: str='facebook/opt-125m', provider: Literal['vllm', 'openai']='vllm', concurrency: int=1, batch_size: int | None=None, num_cpus: int | None=None, num_gpus: int | None=None, **generation_config: dict[str, Any]) -> Expression
```

A UDF for running LLM inference over an input column of strings.

This UDF provides a flexible interface for text generation using various LLM providers.
By default, it uses vLLM for efficient local inference.

Args:
    text (String Expression):
        The input text column to generate from
    model (str, default="facebook/opt-125m"):
        The model identifier to use for generation
    provider (str, default="vllm"):
        The LLM provider to use for generation. Supported values: "vllm", "openai"
    concurrency (int, default=1):
        The number of concurrent instances of the model to run
    batch_size (int, default=None):
        The batch size for the UDF. If None, the batch size will be determined by defaults based on the provider.
    num_cpus (int, default=None):
        The number of CPUs to use for the UDF
    num_gpus (int, default=None):
        The number of GPUs to use for the UDF
    generation_config (dict, default={}):
        Configuration parameters for text generation (e.g., temperature, max_tokens)

Returns:
    Expression (String Expression): The generated text column

## named_struct

```python
named_struct(*args: str | Expression) -> Expression
```

Constructs a struct from alternating field name and value pairs.

This mirrors the SQL ``named_struct`` function and accepts alternating string
field names and :class:`~daft.expressions.Expression` values as positional arguments.

Args:
    *args: Alternating ``(field_name, value)`` pairs. Field names must be string
        literals; values must be :class:`~daft.expressions.Expression` objects.

Returns:
    An expression for a struct column with the given fields.

Raises:
    ValueError: If an odd number of arguments is supplied, or if a field name is
        not a string.

## partition_days

```python
partition_days(expr: Expression) -> Expression
```

Partitioning Transform that returns the number of days since epoch (1970-01-01).

Unlike other temporal partitioning expressions, this expression is date type instead of int. This is to conform to the behavior of other implementations of Iceberg partition transforms.

Returns:
    Date Expression

## partition_hours

```python
partition_hours(expr: Expression) -> Expression
```

Partitioning Transform that returns the number of hours since epoch (1970-01-01).

Returns:
    Expression: Int32 Expression in hours

## partition_iceberg_bucket

```python
partition_iceberg_bucket(expr: Expression, n: int) -> Expression
```

Partitioning Transform that returns the Hash Bucket following the Iceberg Specification of murmur3_32_x86.

See <https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements> for more details.

Args:
    expr: the expression to bucket
    n: Number of buckets

Returns:
    Expression: Int32 Expression with the Hash Bucket

## partition_iceberg_truncate

```python
partition_iceberg_truncate(expr: Expression, w: int) -> Expression
```

Partitioning Transform that truncates the input to a standard width `w` following the Iceberg Specification.

See <https://iceberg.apache.org/spec/#truncate-transform-details> for more details.

Args:
    expr: the expression to truncate
    w: width of the truncation

Returns:
    Expression: Expression of the Same Type of the input

## partition_months

```python
partition_months(expr: Expression) -> Expression
```

Partitioning Transform that returns the number of months since epoch (1970-01-01).

Returns:
    Expression: Int32 Expression in months

## partition_years

```python
partition_years(expr: Expression) -> Expression
```

Partitioning Transform that returns the number of years since epoch (1970-01-01).

Returns:
    Expression: Int32 Expression in years

## pearson_correlation

```python
pearson_correlation(left: Expression, right: Expression) -> Expression
```

Compute the Pearson correlation between two embeddings.

Args:
    left (FixedSizeList or Embedding Expression): The left vector
    right (FixedSizeList or Embedding Expression): The right vector

Returns:
    Expression (Float64 Expression): the Pearson correlation between the two vectors.

## run_process

```python
run_process(args: Expression | list[Expression | Any], *, shell: bool=False, on_error: Literal['raise', 'ignore', 'log']='log', return_dtype: DataTypeLike=DataType.string()) -> Expression
```

Returns an expression that runs an external process (optionally via a shell) and exposes its stdout as a column.

This helper wraps a Python UDF around ``subprocess.run`` so it can be used inside DataFrame expressions.

Args:
    args (Expression | list[Expression | Any]):
        The command to execute.
        If ``shell=False`` (default), pass a list of arguments, for example ``["ls", "-a", col("path")]``.
        If ``shell=True``, pass a single string expression or a list that will be joined, for example ``"echo hello"`` or ``["echo", "hello"]``.
    shell (bool, default=False):
        Whether to execute the command via the system shell (equivalent to ``subprocess.run(..., shell=True)``).
        Using the shell enables pipes and redirection but is more vulnerable to injection. Defaults to ``False``.
    on_error (Literal["raise", "ignore", "log"], default="log"):
        Whether to log an error when encountering an error, or log a warning and return a null
    return_dtype: Desired Daft data type for the result column. Defaults to a UTF-8 string column.

Returns:
    Expression: An expression representing the stdout of the process converted to ``return_dtype`` (defaults to a UTF-8 string column).

## shift_left

```python
shift_left(expr: Expression, num_bits: Expression) -> Expression
```

Shifts the bits of an integer expression to the left (``expr << num_bits``).

Args:
    expr: The expression to shift.
    num_bits: The number of bits to shift the expression to the left

## shift_right

```python
shift_right(expr: Expression, num_bits: Expression) -> Expression
```

Shifts the bits of an integer expression to the right (``expr >> num_bits``).

Args:
    expr: The expression to shift.
    num_bits: The number of bits to shift the expression to the right

Note:
    For unsigned integers, this expression perform a logical right shift.
    For signed integers, this expression perform an arithmetic right shift.

## to_struct

```python
to_struct(*fields: Expression, **named_fields: Expression) -> Expression
```

Constructs a struct from the input expressions.

Args:
    fields: Expressions to be set as struct fields, using the expression name as the field name.
    named_fields: Expressions to be set as struct fields, using the keyword arg as the field name.

Returns:
    An expression for a struct column with the input columns as its fields.

## try_compress

```python
try_compress(expr: Expression, codec: COMPRESSION_CODEC) -> Expression
```

Compress or null if unsuccessful.

Tip: See Also
    [`daft.functions.compress`](https://docs.daft.ai/en/stable/api/functions/compress/)

## try_decode

```python
try_decode(bytes: Expression, charset: ENCODING_CHARSET) -> Expression
```

Decode or null if unsuccessful.

Tip: See Also
    [`daft.functions.decode`](https://docs.daft.ai/en/stable/api/functions/decode/)

## try_decompress

```python
try_decompress(expr: Expression, codec: COMPRESSION_CODEC) -> Expression
```

Decompress or null if unsuccessful.

Tip: See Also
    [`daft.functions.decompress`](https://docs.daft.ai/en/stable/api/functions/decompress/)

## try_encode

```python
try_encode(expr: Expression, charset: ENCODING_CHARSET) -> Expression
```

Encode or null if unsuccessful.

Tip: See Also
    [`daft.functions.encode`](https://docs.daft.ai/en/stable/api/functions/encode/)

## unnest

```python
unnest(expr: Expression) -> Expression
```

Flatten the fields of a struct expression into columns in a DataFrame.
