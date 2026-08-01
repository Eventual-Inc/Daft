# expressions

## abs

```python
abs() -> Expression
```

Absolute of a numeric expression.

Tip: See Also
    [`daft.functions.abs`](https://docs.daft.ai/en/stable/api/functions/abs/)

## alias

```python
alias(name: builtins.str) -> Expression
```

Gives the expression a new name.

Args:
    name: New name for expression

Returns:
    Expression: Renamed expression

## any_value

```python
any_value(ignore_nulls: bool=False) -> Expression
```

Returns any value in the expression.

Tip: See Also
    [`daft.functions.any_value`](https://docs.daft.ai/en/stable/api/functions/any_value/)

## apply

```python
apply(func: Callable[..., Any], return_dtype: DataTypeLike) -> Expression
```

Apply a function on each value in a given expression.

Args:
    func: Function to run per value of the expression
    return_dtype: Return datatype of the function that was ran

Returns:
    Expression: New expression after having run the function on the expression

Note:
    This is just syntactic sugar on top of a UDF and is convenient to use when your function only operates
    on a single column, and does not benefit from executing on batches. For either of those other use-cases,
    use a UDF instead.

## approx_count_distinct

```python
approx_count_distinct() -> Expression
```

Calculates the approximate number of non-`NULL` distinct values in the expression.

Tip: See Also
      [`daft.functions.approx_count_distinct`](https://docs.daft.ai/en/stable/api/functions/approx_count_distinct/)

## approx_percentiles

```python
approx_percentiles(percentiles: builtins.float | builtins.list[builtins.float]) -> Expression
```

Calculates the approximate percentile(s) for a column of numeric values.

Tip: See Also
    [`daft.functions.approx_percentiles`](https://docs.daft.ai/en/stable/api/functions/approx_percentiles/)

## arccos

```python
arccos() -> Expression
```

The elementwise arc cosine of a numeric expression.

Tip: See Also
    [`daft.functions.arccos`](https://docs.daft.ai/en/stable/api/functions/arccos/)

## arccosh

```python
arccosh() -> Expression
```

The elementwise inverse hyperbolic cosine of a numeric expression.

Tip: See Also
    [`daft.functions.arccosh`](https://docs.daft.ai/en/stable/api/functions/arccosh/)

## arcsin

```python
arcsin() -> Expression
```

The elementwise arc sine of a numeric expression.

Tip: See Also
    [`daft.functions.arcsin`](https://docs.daft.ai/en/stable/api/functions/arcsin/)

## arcsinh

```python
arcsinh() -> Expression
```

The elementwise inverse hyperbolic sine of a numeric expression.

Tip: See Also
    [`daft.functions.arcsinh`](https://docs.daft.ai/en/stable/api/functions/arcsinh/)

## arctan

```python
arctan() -> Expression
```

The elementwise arc tangent of a numeric expression.

Tip: See Also
    [`daft.functions.arctan`](https://docs.daft.ai/en/stable/api/functions/arctan/)

## arctan2

```python
arctan2(other: Expression) -> Expression
```

Calculates the four quadrant arctangent of coordinates (y, x), in radians.

Tip: See Also
    [`daft.functions.arctan2`](https://docs.daft.ai/en/stable/api/functions/arctan2/)

## arctanh

```python
arctanh() -> Expression
```

The elementwise inverse hyperbolic tangent of a numeric expression.

Tip: See Also
    [`daft.functions.arctanh`](https://docs.daft.ai/en/stable/api/functions/arctanh/)

## as_py

```python
as_py() -> Any
```

Returns this literal expression as a python value, raises a ValueError if this is not a literal expression.

## ascii

```python
ascii() -> Expression
```

Returns the ASCII numeric value of the first character of the string.

Tip: See Also
    [`daft.functions.ascii_func`](https://docs.daft.ai/en/stable/api/functions/ascii_func/)

## avg

```python
avg() -> Expression
```

Alias for `Expression.mean()`. Check [`Expression.mean`](https://docs.daft.ai/en/stable/api/expressions/#daft.expressions.Expression.mean) for more details.

## between

```python
between(lower: int | builtins.float, upper: int | builtins.float) -> Expression
```

Checks if values in the Expression are between lower and upper, inclusive.

Tip: See Also
    [`daft.functions.between`](https://docs.daft.ai/en/stable/api/functions/between/)

## bitwise_and

```python
bitwise_and(other: Expression) -> Expression
```

Bitwise AND of two integer expressions.

Tip: See Also
    [`daft.functions.bitwise_and`](https://docs.daft.ai/en/stable/api/functions/bitwise_and/)

## bitwise_or

```python
bitwise_or(other: Expression) -> Expression
```

Bitwise OR of two integer expressions.

Tip: See Also
    [`daft.functions.bitwise_or`](https://docs.daft.ai/en/stable/api/functions/bitwise_or/)

## bitwise_xor

```python
bitwise_xor(other: Expression) -> Expression
```

Bitwise XOR of two integer expressions.

Tip: See Also
    [`daft.functions.bitwise_xor`](https://docs.daft.ai/en/stable/api/functions/bitwise_xor/)

## bool_and

```python
bool_and() -> Expression
```

Calculates the boolean AND of all values in a list.

Tip: See Also
    [`daft.functions.bool_and`](https://docs.daft.ai/en/stable/api/functions/bool_and/)

## bool_or

```python
bool_or() -> Expression
```

Calculates the boolean OR of all values in a list.

Tip: See Also
    [`daft.functions.bool_or`](https://docs.daft.ai/en/stable/api/functions/bool_or/)

## capitalize

```python
capitalize() -> Expression
```

Capitalize a UTF-8 string.

Tip: See Also
    [`daft.functions.capitalize`](https://docs.daft.ai/en/stable/api/functions/capitalize/)

## cast

```python
cast(dtype: DataTypeLike) -> Expression
```

Casts an expression to the given datatype if possible.

Tip: See Also
    [`daft.functions.cast`](https://docs.daft.ai/en/stable/api/functions/cast/)

## cbrt

```python
cbrt() -> Expression
```

The cube root of a numeric expression.

Tip: See Also
    [`daft.functions.cbrt`](https://docs.daft.ai/en/stable/api/functions/cbrt/)

## ceil

```python
ceil() -> Expression
```

The ceiling of a numeric expression.

Tip: See Also
    [`daft.functions.ceil`](https://docs.daft.ai/en/stable/api/functions/ceil/)

## chunk

```python
chunk(size: int) -> Expression
```

Splits each list into chunks of the given size.

Tip: See Also
    [`daft.functions.chunk`](https://docs.daft.ai/en/stable/api/functions/chunk/)

## clip

```python
clip(min: Expression | None=None, max: Expression | None=None) -> Expression
```

Clips an expression to the given minimum and maximum values.

Tip: See Also
    [`daft.functions.clip`](https://docs.daft.ai/en/stable/api/functions/clip/)

## coalesce

```python
coalesce(*others: Expression) -> Expression
```

Returns the first non-null value among this expression and the provided expressions.

Tip: See Also
    [`daft.functions.coalesce`](https://docs.daft.ai/en/stable/api/functions/coalesce/)

## column_name

```python
column_name() -> builtins.str | None
```

_(no docstring)_

## compress

```python
compress(codec: COMPRESSION_CODEC) -> Expression
```

Compress binary or string values using the specified codec.

Tip: See Also
    [`daft.functions.compress`](https://docs.daft.ai/en/stable/api/functions/compress/)

## concat

```python
concat(other: Expression | builtins.str | bytes) -> Expression
```

Concatenate two string expressions.

Tip: See Also
    [`daft.functions.concat`](https://docs.daft.ai/en/stable/api/functions/concat/)

## contains

```python
contains(substr: builtins.str | Expression) -> Expression
```

Checks whether each string contains the given pattern in a string column.

Tip: See Also
    [`daft.functions.contains`](https://docs.daft.ai/en/stable/api/functions/contains/)

## convert_image

```python
convert_image(mode: builtins.str | ImageMode) -> Expression
```

Convert an image expression to the specified mode.

Tip: See Also
    [`daft.functions.convert_image`](https://docs.daft.ai/en/stable/api/functions/convert_image/)

## convert_time_zone

```python
convert_time_zone(to_timezone: builtins.str, from_timezone: builtins.str | None=None) -> Expression
```

Converts a timestamp to another timezone while preserving the instant in time.

Tip: See Also
    [`daft.functions.convert_time_zone`](https://docs.daft.ai/en/stable/api/functions/convert_time_zone/)

## cos

```python
cos() -> Expression
```

The elementwise cosine of a numeric expression.

Tip: See Also
    [`daft.functions.cos`](https://docs.daft.ai/en/stable/api/functions/cos/)

## cosh

```python
cosh() -> Expression
```

The elementwise hyperbolic cosine of a numeric expression.

Tip: See Also
    [`daft.functions.cosh`](https://docs.daft.ai/en/stable/api/functions/cosh/)

## cosine_distance

```python
cosine_distance(other: Expression) -> Expression
```

Compute the cosine distance between two embeddings.

Tip: See Also
    [`daft.functions.cosine_distance`](https://docs.daft.ai/en/stable/api/functions/cosine_distance/)

## cosine_similarity

```python
cosine_similarity(other: Expression) -> Expression
```

Compute the cosine similarity between two embeddings.

Tip: See Also
    [`daft.functions.cosine_similarity`](https://docs.daft.ai/en/stable/api/functions/cosine_similarity/)

## cot

```python
cot() -> Expression
```

The elementwise cotangent of a numeric expression.

Tip: See Also
    [`daft.functions.cot`](https://docs.daft.ai/en/stable/api/functions/cot/)

## count

```python
count(mode: Literal['all', 'valid', 'null'] | CountMode=CountMode.Valid) -> Expression
```

Counts the number of values in the expression.

Tip: See Also
    [`daft.functions.count`](https://docs.daft.ai/en/stable/api/functions/count)

## count_distinct

```python
count_distinct() -> Expression
```

Counts the number of distinct values in the expression.

Tip: See Also
    [`daft.functions.count_distinct`](https://docs.daft.ai/en/stable/api/functions/count_distinct)

## count_matches

```python
count_matches(patterns: Any, *, whole_words: bool=False, case_sensitive: bool=True) -> Expression
```

Counts the number of times a pattern, or multiple patterns, appear in a string.

Tip: See Also
    [`daft.functions.count_matches`](https://docs.daft.ai/en/stable/api/functions/count_matches/)

## crop

```python
crop(bbox: tuple[int, int, int, int] | Expression) -> Expression
```

Crops images with the provided bounding box.

Tip: See Also
    [`daft.functions.crop`](https://docs.daft.ai/en/stable/api/functions/crop/)

## csc

```python
csc() -> Expression
```

The elementwise cosecant of a numeric expression.

Tip: See Also
    [`daft.functions.csc`](https://docs.daft.ai/en/stable/api/functions/csc/)

## damerau_levenshtein_distance

```python
damerau_levenshtein_distance(other: Expression) -> Expression
```

Compute the Damerau-Levenshtein distance between two strings.

Tip: See Also
    [`daft.functions.damerau_levenshtein_distance`](https://docs.daft.ai/en/stable/api/functions/damerau_levenshtein_distance/)

## date

```python
date() -> Expression
```

Retrieves the date for a datetime column.

## date_trunc

```python
date_trunc(interval: builtins.str, relative_to: Expression | None=None) -> Expression
```

Truncates the datetime column to the specified interval.

Tip: See Also
    [`daft.functions.date_trunc`](https://docs.daft.ai/en/stable/api/functions/date_trunc/)

## day

```python
day() -> Expression
```

Retrieves the day for a datetime column.

Tip: See Also
    [`daft.functions.day`](https://docs.daft.ai/en/stable/api/functions/day/)

## day_of_month

```python
day_of_month() -> Expression
```

Retrieves the day of the month for a datetime column.

Tip: See Also
    [`daft.functions.day_of_month`](https://docs.daft.ai/en/stable/api/functions/day_of_month/)

## day_of_week

```python
day_of_week() -> Expression
```

Retrieves the day of the week for a datetime column, starting at 0 for Monday and ending at 6 for Sunday.

Tip: See Also
    [`daft.functions.day_of_week`](https://docs.daft.ai/en/stable/api/functions/day_of_week/)

## day_of_year

```python
day_of_year() -> Expression
```

Retrieves the ordinal day for a datetime column. Starting at 1 for January 1st and ending at 365 or 366 for December 31st.

Tip: See Also
    [`daft.functions.day_of_year`](https://docs.daft.ai/en/stable/api/functions/day_of_year/)

## decode

```python
decode(charset: ENCODING_CHARSET) -> Expression
```

Decodes binary values using the specified character set.

Tip: See Also
    [`daft.functions.decode`](https://docs.daft.ai/en/stable/api/functions/decode/)

## decode_image

```python
decode_image(on_error: Literal['raise', 'null']='raise', mode: builtins.str | ImageMode | None=ImageMode.RGB) -> Expression
```

Decodes the binary data in this column into images.

Tip: See Also
    [`daft.functions.decode_image`](https://docs.daft.ai/en/stable/api/functions/decode_image/)

## decode_image_file

```python
decode_image_file() -> Expression
```

Decodes an image file into an Image column.

## decompress

```python
decompress(codec: COMPRESSION_CODEC) -> Expression
```

Decompress binary values using the specified codec.

Tip: See Also
    [`daft.functions.decompress`](https://docs.daft.ai/en/stable/api/functions/decompress/)

## degrees

```python
degrees() -> Expression
```

The elementwise degrees of a numeric expression.

Tip: See Also
    [`daft.functions.degrees`](https://docs.daft.ai/en/stable/api/functions/degrees/)

## deserialize

```python
deserialize(format: Literal['json'], dtype: DataTypeLike) -> Expression
```

Deserializes the expression (string) using the specified format and data type.

Tip: See Also
    [`daft.functions.deserialize`](https://docs.daft.ai/en/stable/api/functions/deserialize/)

## dot_product

```python
dot_product(other: Expression) -> Expression
```

Compute the dot product between two embeddings.

Tip: See Also
    [`daft.functions.dot_product`](https://docs.daft.ai/en/stable/api/functions/dot_product/)

## download

```python
download(max_connections: int=32, on_error: Literal['raise', 'null']='raise', io_config: IOConfig | None=None) -> Expression
```

Treats each string as a URL, and downloads the bytes contents as a bytes column.

Tip: See Also
    [`daft.functions.download`](https://docs.daft.ai/en/stable/api/functions/download/)

## encode

```python
encode(charset: ENCODING_CHARSET) -> Expression
```

Encode binary or string values using the specified character set.

Tip: See Also
    [`daft.functions.encode`](https://docs.daft.ai/en/stable/api/functions/encode/)

## encode_image

```python
encode_image(image_format: builtins.str | ImageFormat) -> Expression
```

Encode an image column as the provided image file format, returning a binary column of encoded bytes.

Tip: See Also
    [`daft.functions.encode_image`](https://docs.daft.ai/en/stable/api/functions/encode_image/)

## endswith

```python
endswith(suffix: builtins.str | Expression) -> Expression
```

Checks whether each string ends with the given pattern in a string column.

Tip: See Also
    [`daft.functions.endswith`](https://docs.daft.ai/en/stable/api/functions/endswith/)

## eq_null_safe

```python
eq_null_safe(other: Expression | Any) -> Expression
```

Performs a null-safe equality comparison between two expressions.

Tip: See Also
    [`daft.functions.eq_null_safe`](https://docs.daft.ai/en/stable/api/functions/eq_null_safe/)

## euclidean_distance

```python
euclidean_distance(other: Expression) -> Expression
```

Compute the Euclidean distance between two embeddings.

Tip: See Also
    [`daft.functions.euclidean_distance`](https://docs.daft.ai/en/stable/api/functions/euclidean_distance/)

## exp

```python
exp() -> Expression
```

The e^self of a numeric expression.

Tip: See Also
    [`daft.functions.exp`](https://docs.daft.ai/en/stable/api/functions/exp/)

## explode

```python
explode(ignore_empty_and_null: bool=False) -> Expression
```

Explode a list expression.

Args:
   ignore_empty_and_null: If True, drops rows where the list is empty or null.
       If False (default), empty lists and null values each produce a single row with a null value.

Tip: See Also
    [`daft.functions.explode`](https://docs.daft.ai/en/stable/api/functions/explode/)

## expm1

```python
expm1() -> Expression
```

The e^self - 1 of a numeric expression.

Tip: See Also
    [`daft.functions.expm1`](https://docs.daft.ai/en/stable/api/functions/expm1/)

## file_exists

```python
file_exists() -> Expression
```

Checks whether a file exists.

Tip: See Also
    [`daft.functions.file_exists`](https://docs.daft.ai/en/stable/api/functions/file_exists/)

## file_path

```python
file_path() -> Expression
```

Gets the path (URL) of a file as a string.

Tip: See Also
    [`daft.functions.file_path`](https://docs.daft.ai/en/stable/api/functions/file_path/)

## file_size

```python
file_size() -> Expression
```

Gets the size of a file in bytes.

Tip: See Also
    [`daft.functions.file_size`](https://docs.daft.ai/en/stable/api/functions/file_size/)

## fill_nan

```python
fill_nan(fill_value: Expression) -> Expression
```

Fills NaN values in the Expression with the provided fill_value.

Tip: See Also
    [`daft.functions.fill_nan`](https://docs.daft.ai/en/stable/api/functions/fill_nan/)

## fill_null

```python
fill_null(fill_value: Expression | Any) -> Expression
```

Fills null values in the Expression with the provided fill_value.

Tip: See Also
    [`daft.functions.fill_null`](https://docs.daft.ai/en/stable/api/functions/fill_null/)

## find

```python
find(substr: builtins.str | Expression) -> Expression
```

Returns the index of the first occurrence of the substring in each string.

Tip: See Also
    [`daft.functions.find`](https://docs.daft.ai/en/stable/api/functions/find/)

## first_value

```python
first_value(ignore_nulls: bool=False) -> Expression
```

Returns the first value in the window frame.

When ``ignore_nulls=True``, skips null values and returns the first non-null value.
Must be used with ``over()`` to specify the window.

## floor

```python
floor() -> Expression
```

The floor of a numeric expression.

Tip: See Also
    [`daft.functions.floor`](https://docs.daft.ai/en/stable/api/functions/floor/)

## get

```python
get(index: int | builtins.str | Expression, default: Any=None) -> Expression
```

Get an index from a list expression or a field from a struct expression.

Tip: See Also
    [`daft.functions.get`](https://docs.daft.ai/en/stable/api/functions/get/)

## hamming_distance

```python
hamming_distance(other: Expression) -> Expression
```

Compute the bitwise Hamming distance between two hash fingerprints.

Tip: See Also
    [`daft.functions.hamming_distance`](https://docs.daft.ai/en/stable/api/functions/hamming_distance/)

## hamming_distance_str

```python
hamming_distance_str(other: Expression) -> Expression
```

Compute the character-level Hamming distance between two strings.

Tip: See Also
    [`daft.functions.hamming_distance_str`](https://docs.daft.ai/en/stable/api/functions/hamming_distance_str/)

## hash

```python
hash(seed: Any | None=None, hash_function: Literal['xxhash', 'xxhash32', 'xxhash64', 'xxhash3_64', 'murmurhash3', 'sha1'] | None='xxhash') -> Expression
```

Hashes the values in the Expression.

Tip: See Also
    [`daft.functions.hash`](https://docs.daft.ai/en/stable/api/functions/hash/): use the function for hashing multiple columns together.

## hdf5_attrs

```python
hdf5_attrs(h5path: str='/') -> Expression
```

Read HDF5 attributes for a group or dataset.

Tip: See Also
    [`daft.functions.hdf5_attrs`](https://docs.daft.ai/en/stable/api/functions/hdf5_attrs/)

## hdf5_keys

```python
hdf5_keys(group: str='/') -> Expression
```

List member names directly under an HDF5 group.

Tip: See Also
    [`daft.functions.hdf5_keys`](https://docs.daft.ai/en/stable/api/functions/hdf5_keys/)

## hdf5_metadata

```python
hdf5_metadata(group: str='/') -> Expression
```

Collect metadata for groups and datasets under an HDF5 group.

Tip: See Also
    [`daft.functions.hdf5_metadata`](https://docs.daft.ai/en/stable/api/functions/hdf5_metadata/)

## hour

```python
hour() -> Expression
```

Retrieves the hour for a datetime column.

Tip: See Also
    [`daft.functions.hour`](https://docs.daft.ai/en/stable/api/functions/hour/)

## ilike

```python
ilike(pattern: builtins.str | Expression) -> Expression
```

Checks whether each string matches the given SQL ILIKE pattern, case insensitive.

Tip: See Also
    [`daft.functions.ilike`](https://docs.daft.ai/en/stable/api/functions/ilike/)

## image_attribute

```python
image_attribute(name: Literal['width', 'height', 'channel', 'mode'] | ImageProperty) -> Expression
```

Get a property of the image, such as 'width', 'height', 'channel', or 'mode'.

Tip: See Also
    [`daft.functions.image_attribute`](https://docs.daft.ai/en/stable/api/functions/image_attribute/)

## image_channel

```python
image_channel() -> Expression
```

Gets the number of channels in an image.

Tip: See Also
    [`daft.functions.image_channel`](https://docs.daft.ai/en/stable/api/functions/image_channel/)

## image_file_metadata

```python
image_file_metadata() -> Expression
```

Gets metadata for an image file (width, height, format, mode).

Reads only the file header without decoding pixel data.

## image_hash

```python
image_hash(*, method: Literal['phash', 'phash_simple', 'dhash', 'dhash_vertical', 'ahash', 'whash', 'crop_resistant', 'colorhash']='phash', hash_size: int=8, binbits: int=3) -> Expression
```

Computes a perceptual hash of an image.

Tip: See Also
    [`daft.functions.image_hash`](https://docs.daft.ai/en/stable/api/functions/image_hash/)

## image_height

```python
image_height() -> Expression
```

Gets the height of an image in pixels.

Tip: See Also
    [`daft.functions.image_height`](https://docs.daft.ai/en/stable/api/functions/image_height/)

## image_mode

```python
image_mode() -> Expression
```

Gets the mode of an image as a string.

Tip: See Also
    [`daft.functions.image_mode`](https://docs.daft.ai/en/stable/api/functions/image_mode/)

## image_to_tensor

```python
image_to_tensor() -> Expression
```

Convert an image expression to a tensor, inferring dtype and shape.

Tip: See Also
    [`daft.functions.image_to_tensor`](https://docs.daft.ai/en/stable/api/functions/image_to_tensor/)

## image_width

```python
image_width() -> Expression
```

Gets the width of an image in pixels.

Tip: See Also
    [`daft.functions.image_width`](https://docs.daft.ai/en/stable/api/functions/image_width/)

## is_column

```python
is_column() -> bool
```

_(no docstring)_

## is_in

```python
is_in(other: Iterable[Any] | Expression) -> Expression
```

Checks if values in the Expression are in the provided iterable.

Args:
    other: An iterable (list, set, tuple, etc.), Expression, or array-like object containing the values to check against

Tip: See Also
    [`daft.functions.is_in`](https://docs.daft.ai/en/stable/api/functions/is_in/)

## is_inf

```python
is_inf() -> Expression
```

Checks if values in the Expression are Infinity.

Tip: See Also
    [`daft.functions.is_inf`](https://docs.daft.ai/en/stable/api/functions/is_inf/)

## is_literal

```python
is_literal() -> bool
```

_(no docstring)_

## is_nan

```python
is_nan() -> Expression
```

Checks if values are NaN (a special float value indicating not-a-number).

Tip: See Also
    [`daft.functions.is_nan`](https://docs.daft.ai/en/stable/api/functions/is_nan/)

## is_null

```python
is_null() -> Expression
```

Checks if values in the Expression are Null (a special value indicating missing data).

Tip: See Also
    [`daft.functions.is_null`](https://docs.daft.ai/en/stable/api/functions/is_null/)

## jaccard_similarity

```python
jaccard_similarity(other: Expression) -> Expression
```

Compute the Jaccard similarity between two embeddings.

Tip: See Also
    [`daft.functions.jaccard_similarity`](https://docs.daft.ai/en/stable/api/functions/jaccard_similarity/)

## jaro_similarity

```python
jaro_similarity(other: Expression) -> Expression
```

Compute the Jaro similarity between two strings.

Tip: See Also
    [`daft.functions.jaro_similarity`](https://docs.daft.ai/en/stable/api/functions/jaro_similarity/)

## jaro_winkler_similarity

```python
jaro_winkler_similarity(other: Expression) -> Expression
```

Compute the Jaro-Winkler similarity between two strings.

Tip: See Also
    [`daft.functions.jaro_winkler_similarity`](https://docs.daft.ai/en/stable/api/functions/jaro_winkler_similarity/)

## jq

```python
jq(filter: builtins.str) -> Expression
```

Applies a [jq](https://jqlang.github.io/jq/manual/) filter to the expression (string), returning the results as a string.

Tip: See Also
    [`daft.functions.jq`](https://docs.daft.ai/en/stable/api/functions/jq/)

## lag

```python
lag(offset: int=1, default: Any | None=None) -> Expression
```

Get the value from a previous row within a window partition.

Tip: See Also
      [`daft.functions.lag`](https://docs.daft.ai/en/stable/api/functions/lag/)

## last_value

```python
last_value(ignore_nulls: bool=False) -> Expression
```

Returns the last value in the window frame.

When ``ignore_nulls=True``, skips null values and returns the last non-null value.
Must be used with ``over()`` to specify the window.

## lead

```python
lead(offset: int=1, default: Any | None=None) -> Expression
```

Get the value from a future row within a window partition.

Tip: See Also
      [`daft.functions.lead`](https://docs.daft.ai/en/stable/api/functions/lead/)

## left

```python
left(nchars: int | Expression) -> Expression
```

Gets the n (from nchars) left-most characters of each string.

Tip: See Also
    [`daft.functions.left`](https://docs.daft.ai/en/stable/api/functions/left/)

## length

```python
length() -> Expression
```

Retrieves the length of the given expression.

Tip: See Also
    [`daft.functions.length`](https://docs.daft.ai/en/stable/api/functions/length/)

## length_bytes

```python
length_bytes() -> Expression
```

Retrieves the length for a UTF-8 string column in bytes.

Tip: See Also
    [`daft.functions.length_bytes`](https://docs.daft.ai/en/stable/api/functions/length_bytes/)

## levenshtein_distance

```python
levenshtein_distance(other: Expression) -> Expression
```

Compute the Levenshtein edit distance between two strings.

Tip: See Also
    [`daft.functions.levenshtein_distance`](https://docs.daft.ai/en/stable/api/functions/levenshtein_distance/)

## like

```python
like(pattern: builtins.str | Expression) -> Expression
```

Checks whether each string matches the given SQL LIKE pattern, case sensitive.

Tip: See Also
    [`daft.functions.like`](https://docs.daft.ai/en/stable/api/functions/like/)

## list_agg

```python
list_agg() -> Expression
```

Aggregates the values in the expression into a list.

Tip: See Also
    [`daft.functions.list_agg`](https://docs.daft.ai/en/stable/api/functions/list_agg/)

## list_agg_distinct

```python
list_agg_distinct() -> Expression
```

Aggregates the values in the expression into a list of distinct values (ignoring nulls).

Tip: See Also
    [`daft.functions.list_agg_distinct`](https://docs.daft.ai/en/stable/api/functions/list_agg_distinct/)

## list_append

```python
list_append(other: Expression) -> Expression
```

Appends a value to each list in the column.

Tip: See Also
    [`daft.functions.list_append`](https://docs.daft.ai/en/stable/api/functions/list_append/)

## list_bool_and

```python
list_bool_and() -> Expression
```

Calculates the boolean AND of all values in a list.

Tip: See Also
    [`daft.functions.list_bool_and`](https://docs.daft.ai/en/stable/api/functions/list_bool_and/)

## list_bool_or

```python
list_bool_or() -> Expression
```

Calculates the boolean OR of all values in a list.

Tip: See Also
    [`daft.functions.list_bool_or`](https://docs.daft.ai/en/stable/api/functions/list_bool_or/)

## list_contains

```python
list_contains(item: Expression) -> Expression
```

Checks if each list contains the specified item.

Tip: See Also
    [`daft.functions.list_contains`](https://docs.daft.ai/en/stable/api/functions/list_contains/)

## list_count

```python
list_count(mode: Literal['all', 'valid', 'null'] | CountMode=CountMode.Valid) -> Expression
```

Counts the number of elements in each list.

Tip: See Also
    [`daft.functions.list_count`](https://docs.daft.ai/en/stable/api/functions/list_count/)

## list_distinct

```python
list_distinct() -> Expression
```

Returns a list of unique elements in each list, preserving order of first occurrence and ignoring nulls.

Tip: See Also
    [`daft.functions.list_distinct`](https://docs.daft.ai/en/stable/api/functions/list_distinct/)

## list_filter

```python
list_filter(predicate: Expression) -> Expression
```

Filters elements in the list using a boolean predicate over `daft.element()`.

Tip: See Also
    [`daft.functions.list_filter`](https://docs.daft.ai/en/stable/api/functions/list_filter/)

## list_flatten

```python
list_flatten() -> Expression
```

Flattens one level of nesting in each list.

Outer null rows are preserved as null. Null inner lists are skipped while flattening,
and null leaf values are preserved in the output.

Tip: See Also
    [`daft.functions.list_flatten`](https://docs.daft.ai/en/stable/api/functions/list_flatten/)

## list_join

```python
list_join(delimiter: builtins.str | Expression) -> Expression
```

Joins every element of a list using the specified string delimiter.

Tip: See Also
    [`daft.functions.list_join`](https://docs.daft.ai/en/stable/api/functions/list_join/)

## list_map

```python
list_map(mapper: Expression) -> Expression
```

Evaluates an expression on all elements in the list.

Tip: See Also
    [`daft.functions.list_map`](https://docs.daft.ai/en/stable/api/functions/list_map/)

## list_max

```python
list_max() -> Expression
```

Calculates the maximum of each list. If no non-null values in a list, the result is null.

Tip: See Also
    [`daft.functions.list_max`](https://docs.daft.ai/en/stable/api/functions/list_max/)

## list_mean

```python
list_mean() -> Expression
```

Calculates the mean of each list. If no non-null values in a list, the result is null.

Tip: See Also
    [`daft.functions.list_mean`](https://docs.daft.ai/en/stable/api/functions/list_mean/)

## list_min

```python
list_min() -> Expression
```

Calculates the minimum of each list. If no non-null values in a list, the result is null.

Tip: See Also
    [`daft.functions.list_min`](https://docs.daft.ai/en/stable/api/functions/list_min/)

## list_sort

```python
list_sort(desc: bool | Expression | None=None, nulls_first: bool | Expression | None=None) -> Expression
```

Sorts the inner lists of a list column.

Tip: See Also
    [`daft.functions.list_sort`](https://docs.daft.ai/en/stable/api/functions/list_sort/)

## list_sum

```python
list_sum() -> Expression
```

Sums each list. Empty lists and lists with all nulls yield null.

Tip: See Also
    [`daft.functions.list_sum`](https://docs.daft.ai/en/stable/api/functions/list_sum/)

## ln

```python
ln() -> Expression
```

The elementwise natural log of a numeric expression.

Tip: See Also
    [`daft.functions.ln`](https://docs.daft.ai/en/stable/api/functions/ln/)

## log

```python
log(base: int | builtins.float=math.e) -> Expression
```

The elementwise log with given base, of a numeric expression.

Tip: See Also
    [`daft.functions.log`](https://docs.daft.ai/en/stable/api/functions/log/)

## log10

```python
log10() -> Expression
```

The elementwise log base 10 of a numeric expression.

Tip: See Also
    [`daft.functions.log10`](https://docs.daft.ai/en/stable/api/functions/log10/)

## log1p

```python
log1p() -> Expression
```

The ln(self + 1) of a numeric expression.

Tip: See Also
    [`daft.functions.log1p`](https://docs.daft.ai/en/stable/api/functions/log1p/)

## log2

```python
log2() -> Expression
```

The elementwise log base 2 of a numeric expression.

Tip: See Also
    [`daft.functions.log2`](https://docs.daft.ai/en/stable/api/functions/log2/)

## lower

```python
lower() -> Expression
```

Convert UTF-8 string to all lowercase.

Tip: See Also
    [`daft.functions.lower`](https://docs.daft.ai/en/stable/api/functions/lower/)

## lpad

```python
lpad(length: int | Expression, pad: builtins.str | Expression) -> Expression
```

Left-pads each string by truncating or padding with the character.

Tip: See Also
    [`daft.functions.lpad`](https://docs.daft.ai/en/stable/api/functions/lpad/)

## lstrip

```python
lstrip() -> Expression
```

Strip whitespace from the left side of a UTF-8 string.

Tip: See Also
    [`daft.functions.lstrip`](https://docs.daft.ai/en/stable/api/functions/lstrip/)

## map_get

```python
map_get(key: Expression) -> Expression
```

Retrieves the value for a key in a map column.

Tip: See Also
    [`daft.functions.map_get`](https://docs.daft.ai/en/stable/api/functions/map_get/)

## map_keys

```python
map_keys() -> Expression
```

Returns a list of all keys in the map.

Tip: See Also
    [`daft.functions.map_keys`](https://docs.daft.ai/en/stable/api/functions/map_keys/)

## max

```python
max() -> Expression
```

Calculates the maximum value in the expression.

Tip: See Also
    [`daft.functions.max`](https://docs.daft.ai/en/stable/api/functions/max/)

## mean

```python
mean() -> Expression
```

Calculates the mean of the values in the expression.

Tip: See Also
    [`daft.functions.mean`](https://docs.daft.ai/en/stable/api/functions/mean/)

## median

```python
median() -> Expression
```

Calculates the median of the values in the expression.

Tip: See Also
    [`daft.functions.median`](https://docs.daft.ai/en/stable/api/functions/median/)

## microsecond

```python
microsecond() -> Expression
```

Retrieves the microsecond for a datetime column.

Tip: See Also
    [`daft.functions.microsecond`](https://docs.daft.ai/en/stable/api/functions/microsecond/)

## millisecond

```python
millisecond() -> Expression
```

Retrieves the millisecond for a datetime column.

Tip: See Also
    [`daft.functions.millisecond`](https://docs.daft.ai/en/stable/api/functions/millisecond/)

## min

```python
min() -> Expression
```

Calculates the minimum value in the expression.

Tip: See Also
    [`daft.functions.min`](https://docs.daft.ai/en/stable/api/functions/min/)

## minhash

```python
minhash(*, num_hashes: int, ngram_size: int, seed: int=1, hash_function: Literal['murmurhash3', 'xxhash', 'xxhash32', 'xxhash64', 'xxhash3_64', 'sha1']='murmurhash3') -> Expression
```

Runs the MinHash algorithm on the series.

Tip: See Also
    [`daft.functions.minhash`](https://docs.daft.ai/en/stable/api/functions/minhash/)

## minute

```python
minute() -> Expression
```

Retrieves the minute for a datetime column.

Tip: See Also
    [`daft.functions.minute`](https://docs.daft.ai/en/stable/api/functions/minute/)

## month

```python
month() -> Expression
```

Retrieves the month for a datetime column.

Tip: See Also
    [`daft.functions.month`](https://docs.daft.ai/en/stable/api/functions/month/)

## name

```python
name() -> builtins.str
```

_(no docstring)_

## nanosecond

```python
nanosecond() -> Expression
```

Retrieves the nanosecond for a datetime column.

Tip: See Also
    [`daft.functions.nanosecond`](https://docs.daft.ai/en/stable/api/functions/nanosecond/)

## negate

```python
negate() -> Expression
```

The negative of a numeric expression.

Tip: See Also
    [`daft.functions.negate`](https://docs.daft.ai/en/stable/api/functions/negate/)

## normalize

```python
normalize(*, remove_punct: bool=False, lowercase: bool=False, nfd_unicode: bool=False, white_space: bool=False) -> Expression
```

Normalizes a string for more useful deduplication.

Tip: See Also
    [`daft.functions.normalize`](https://docs.daft.ai/en/stable/api/functions/normalize/)

## not_nan

```python
not_nan() -> Expression
```

Checks if values are not NaN (a special float value indicating not-a-number).

Tip: See Also
    [`daft.functions.not_nan`](https://docs.daft.ai/en/stable/api/functions/not_nan/)

## not_null

```python
not_null() -> Expression
```

Checks if values in the Expression are not Null (a special value indicating missing data).

Tip: See Also
    [`daft.functions.not_null`](https://docs.daft.ai/en/stable/api/functions/not_null/)

## over

```python
over(window: Window) -> Expression
```

Apply the expression as a window function.

Tip: See Also
    [`daft.functions.over`](https://docs.daft.ai/en/stable/api/functions/over/)

## parse_url

```python
parse_url() -> Expression
```

Parse string URLs and extract URL components.

Tip: See Also
    [`daft.functions.parse_url`](https://docs.daft.ai/en/stable/api/functions/parse_url/)

## partition_days

```python
partition_days() -> Expression
```

Partitioning Transform that returns the number of days since epoch (1970-01-01).

Tip: See Also
    [`daft.functions.partition_days`](https://docs.daft.ai/en/stable/api/functions/partition_days/)

## partition_hours

```python
partition_hours() -> Expression
```

Partitioning Transform that returns the number of hours since epoch (1970-01-01).

Tip: See Also
    [`daft.functions.partition_hours`](https://docs.daft.ai/en/stable/api/functions/partition_hours/)

## partition_iceberg_bucket

```python
partition_iceberg_bucket(n: int) -> Expression
```

Partitioning Transform that returns the Hash Bucket following the Iceberg Specification of murmur3_32_x86.

Tip: See Also
    [`daft.functions.partition_iceberg_bucket`](https://docs.daft.ai/en/stable/api/functions/partition_iceberg_bucket/)

## partition_iceberg_truncate

```python
partition_iceberg_truncate(w: int) -> Expression
```

Partitioning Transform that truncates the input to a standard width `w` following the Iceberg Specification.

Tip: See Also
    [`daft.functions.partition_iceberg_truncate`](https://docs.daft.ai/en/stable/api/functions/partition_iceberg_truncate/)

## partition_months

```python
partition_months() -> Expression
```

Partitioning Transform that returns the number of months since epoch (1970-01-01).

Tip: See Also
    [`daft.functions.partition_months`](https://docs.daft.ai/en/stable/api/functions/partition_months/)

## partition_years

```python
partition_years() -> Expression
```

Partitioning Transform that returns the number of years since epoch (1970-01-01).

Tip: See Also
    [`daft.functions.partition_years`](https://docs.daft.ai/en/stable/api/functions/partition_years/)

## pearson_correlation

```python
pearson_correlation(other: Expression) -> Expression
```

Compute the Pearson correlation between two embeddings.

Tip: See Also
    [`daft.functions.pearson_correlation`](https://docs.daft.ai/en/stable/api/functions/pearson_correlation/)

## percentile

```python
percentile(percentage: builtins.float) -> Expression
```

Calculates the exact percentile for a column of numeric values.

Tip: See Also
    [`daft.functions.percentile`](https://docs.daft.ai/en/stable/api/functions/percentile/)

## pow

```python
pow(exp: Expression) -> Expression
```

The elementwise exponentiation of a numeric series.

Args:
    exp: The exponent to raise each element to.

## power

```python
power(exp: Expression) -> Expression
```

The elementwise exponentiation of a numeric series.

Args:
    exp: The exponent to raise each element to.

## product

```python
product() -> Expression
```

Calculates the product of the values in the expression.

Tip: See Also
    [`daft.functions.product`](https://docs.daft.ai/en/stable/api/functions/product/)

## quarter

```python
quarter() -> Expression
```

Retrieves the quarter for a datetime column.

Tip: See Also
    [`daft.functions.quarter`](https://docs.daft.ai/en/stable/api/functions/quarter/)

## radians

```python
radians() -> Expression
```

The elementwise radians of a numeric expression.

Tip: See Also
    [`daft.functions.radians`](https://docs.daft.ai/en/stable/api/functions/radians/)

## regexp

```python
regexp(pattern: builtins.str | Expression) -> Expression
```

Check whether each string matches the given regular expression pattern in a string column.

Tip: See Also
    [`daft.functions.regexp`](https://docs.daft.ai/en/stable/api/functions/regexp/)

## regexp_count

```python
regexp_count(pattern: builtins.str | Expression) -> Expression
```

Counts the number of times a regex pattern appears in a string.

Tip: See Also
    [`daft.functions.regexp_count`](https://docs.daft.ai/en/stable/api/functions/regexp_count/)

## regexp_extract

```python
regexp_extract(pattern: builtins.str | Expression, index: int=0) -> Expression
```

Extracts the specified match group from the first regex match in each string in a string column.

Tip: See Also
    [`daft.functions.regexp_extract`](https://docs.daft.ai/en/stable/api/functions/regexp_extract/)

## regexp_extract_all

```python
regexp_extract_all(pattern: builtins.str | Expression, index: int=0) -> Expression
```

Extracts the specified match group from all regex matches in each string in a string column.

Tip: See Also
    [`daft.functions.regexp_extract_all`](https://docs.daft.ai/en/stable/api/functions/regexp_extract_all/)

## regexp_replace

```python
regexp_replace(pattern: builtins.str | Expression, replacement: builtins.str | Expression) -> Expression
```

Replaces all occurrences of a regex pattern in a string column with a replacement string.

Tip: See Also
    [`daft.functions.regexp_replace`](https://docs.daft.ai/en/stable/api/functions/regexp_replace/)

## regexp_split

```python
regexp_split(pattern: builtins.str | Expression) -> Expression
```

Splits each string on the given regex pattern, into a list of strings.

Tip: See Also
    [`daft.functions.regexp_split`](https://docs.daft.ai/en/stable/api/functions/regexp_split/)

## repeat

```python
repeat(n: int | Expression) -> Expression
```

Repeats each string n times.

Tip: See Also
    [`daft.functions.repeat`](https://docs.daft.ai/en/stable/api/functions/repeat/)

## replace

```python
replace(search: builtins.str | Expression, replacement: builtins.str | Expression) -> Expression
```

Replaces all occurrences of a substring in a string with a replacement string.

Tip: See Also
    [`daft.functions.replace`](https://docs.daft.ai/en/stable/api/functions/replace/)

## replace_time_zone

```python
replace_time_zone(timezone: builtins.str | None=None) -> Expression
```

Replaces the timezone of a timestamp while preserving the local time.

Tip: See Also
    [`daft.functions.replace_time_zone`](https://docs.daft.ai/en/stable/api/functions/replace_time_zone/)

## resize

```python
resize(w: int, h: int) -> Expression
```

Resize image into the provided width and height.

Tip: See Also
    [`daft.functions.resize`](https://docs.daft.ai/en/stable/api/functions/resize/)

## reverse

```python
reverse() -> Expression
```

Reverse a UTF-8 string.

Tip: See Also
    [`daft.functions.reverse`](https://docs.daft.ai/en/stable/api/functions/reverse/)

## right

```python
right(nchars: int | Expression) -> Expression
```

Gets the n (from nchars) right-most characters of each string.

Tip: See Also
    [`daft.functions.right`](https://docs.daft.ai/en/stable/api/functions/right/)

## round

```python
round(decimals: Expression | int=0) -> Expression
```

The round of a numeric expression.

Tip: See Also
    [`daft.functions.round`](https://docs.daft.ai/en/stable/api/functions/round/)

## rpad

```python
rpad(length: int | Expression, pad: builtins.str | Expression) -> Expression
```

Right-pads each string by truncating or padding with the character.

Tip: See Also
    [`daft.functions.rpad`](https://docs.daft.ai/en/stable/api/functions/rpad/)

## rstrip

```python
rstrip() -> Expression
```

Strip whitespace from the right side of a UTF-8 string.

Tip: See Also
    [`daft.functions.rstrip`](https://docs.daft.ai/en/stable/api/functions/rstrip/)

## sec

```python
sec() -> Expression
```

The elementwise secant of a numeric expression.

Tip: See Also
    [`daft.functions.sec`](https://docs.daft.ai/en/stable/api/functions/sec/)

## second

```python
second() -> Expression
```

Retrieves the second for a datetime column.

Tip: See Also
    [`daft.functions.second`](https://docs.daft.ai/en/stable/api/functions/second/)

## serialize

```python
serialize(format: Literal['json']) -> Expression
```

Serializes the expression as a string using the specified format.

Tip: See Also
    [`daft.functions.serialize`](https://docs.daft.ai/en/stable/api/functions/serialize/)

## shift_left

```python
shift_left(other: Expression) -> Expression
```

Shifts the bits of an integer expression to the left (``expr << other``).

Tip: See Also
    [`daft.functions.shift_left`](https://docs.daft.ai/en/stable/api/functions/shift_left/)

## shift_right

```python
shift_right(other: Expression) -> Expression
```

Shifts the bits of an integer expression to the right (``expr >> other``).

Tip: See Also
    [`daft.functions.shift_right`](https://docs.daft.ai/en/stable/api/functions/shift_right/)

## sign

```python
sign() -> Expression
```

The sign of a numeric expression.

Tip: See Also
    [`daft.functions.sign`](https://docs.daft.ai/en/stable/api/functions/sign/)

## simhash

```python
simhash(*, ngram_size: int=3, hash_function: Literal['murmurhash3', 'xxhash', 'xxhash32', 'xxhash64', 'xxhash3_64', 'sha1']='xxhash3_64') -> Expression
```

Compute a SimHash fingerprint of this string expression.

Tip: See Also
    [`daft.functions.simhash`](https://docs.daft.ai/en/stable/api/functions/simhash/)

## sin

```python
sin() -> Expression
```

The elementwise sine of a numeric expression.

Tip: See Also
    [`daft.functions.sin`](https://docs.daft.ai/en/stable/api/functions/sin/)

## sinh

```python
sinh() -> Expression
```

The elementwise hyperbolic sine of a numeric expression.

Tip: See Also
    [`daft.functions.sinh`](https://docs.daft.ai/en/stable/api/functions/sinh/)

## skew

```python
skew() -> Expression
```

Calculates the skewness of the values from the expression.

Tip: See Also
    [`daft.functions.skew`](https://docs.daft.ai/en/stable/api/functions/skew/)

## slice

```python
slice(start: int | Expression, end: int | Expression | None=None) -> Expression
```

Get a subset of each list or binary value.

Tip: See Also
    [`daft.functions.slice`](https://docs.daft.ai/en/stable/api/functions/slice/)

## soundex

```python
soundex() -> Expression
```

Returns the Soundex code of the string.

Tip: See Also
    [`daft.functions.soundex`](https://docs.daft.ai/en/stable/api/functions/soundex/)

## split

```python
split(split_on: builtins.str | Expression) -> Expression
```

Splits each string on the given string, into a list of strings.

Tip: See Also
    [`daft.functions.split`](https://docs.daft.ai/en/stable/api/functions/split/)

## sqrt

```python
sqrt() -> Expression
```

The square root of a numeric expression.

Tip: See Also
    [`daft.functions.sqrt`](https://docs.daft.ai/en/stable/api/functions/sqrt/)

## startswith

```python
startswith(prefix: builtins.str | Expression) -> Expression
```

Checks whether each string starts with the given pattern in a string column.

Tip: See Also
    [`daft.functions.startswith`](https://docs.daft.ai/en/stable/api/functions/startswith/)

## stddev

```python
stddev(ddof: int=1) -> Expression
```

Calculates the standard deviation of the values in the expression.

Args:
    ddof: Delta degrees of freedom. The divisor used in calculations
        is N - ddof, where N is the number of non-null elements.
        Defaults to 1 (sample standard deviation).

Tip: See Also
    [`daft.functions.stddev`](https://docs.daft.ai/en/stable/api/functions/stddev/)

## strftime

```python
strftime(format: builtins.str | None=None) -> Expression
```

Converts a datetime/date column to a string column.

Tip: See Also
    [`daft.functions.strftime`](https://docs.daft.ai/en/stable/api/functions/strftime/)

## string_agg

```python
string_agg(delimiter: str | None=None) -> Expression
```

Aggregates the values in the expression into a single string by concatenating them.

Args:
    delimiter: Optional delimiter to insert between concatenated values. Only supported for string columns.

Tip: See Also
    [`daft.functions.string_agg`](https://docs.daft.ai/en/stable/api/functions/string_agg/)

## strip

```python
strip() -> Expression
```

Strip whitespace from both sides of a UTF-8 string.

Tip: See Also
    [`daft.functions.strip`](https://docs.daft.ai/en/stable/api/functions/strip/)

## substr

```python
substr(start: int | Expression, length: int | Expression | None=None) -> Expression
```

Extract a substring from a string, starting at a specified index and extending for a given length.

Tip: See Also
    [`daft.functions.substr`](https://docs.daft.ai/en/stable/api/functions/substr/)

## substring_index

```python
substring_index(delim: builtins.str | Expression, count: builtins.int | Expression) -> Expression
```

Returns the substring from string before count occurrences of the delimiter.

Tip: See Also
    [`daft.functions.substring_index`](https://docs.daft.ai/en/stable/api/functions/substring_index/)

## sum

```python
sum() -> Expression
```

Calculates the sum of the values in the expression.

Tip: See Also
    [`daft.functions.sum`](https://docs.daft.ai/en/stable/api/functions/sum/)

## tan

```python
tan() -> Expression
```

The elementwise tangent of a numeric expression.

Tip: See Also
    [`daft.functions.tan`](https://docs.daft.ai/en/stable/api/functions/tan/)

## tanh

```python
tanh() -> Expression
```

The elementwise hyperbolic tangent of a numeric expression.

Tip: See Also
    [`daft.functions.tanh`](https://docs.daft.ai/en/stable/api/functions/tanh/)

## time

```python
time() -> Expression
```

Retrieves the time for a datetime column.

## to_arrow_expr

```python
to_arrow_expr() -> pc.Expression
```

Returns this expression as a pyarrow.compute.Expression for integrations with other systems.

## to_camel_case

```python
to_camel_case() -> Expression
```

Convert a string to lower camel case.

Tip: See Also
    [`daft.functions.to_camel_case`](https://docs.daft.ai/en/stable/api/functions/to_camel_case/)

## to_date

```python
to_date(format: builtins.str) -> Expression
```

Converts a string to a date using the specified format.

Tip: See Also
    [`daft.functions.to_date`](https://docs.daft.ai/en/stable/api/functions/to_date/)

## to_datetime

```python
to_datetime(format: builtins.str, timezone: builtins.str | None=None) -> Expression
```

Converts a string to a datetime using the specified format and timezone.

Tip: See Also
    [`daft.functions.to_datetime`](https://docs.daft.ai/en/stable/api/functions/to_datetime/)

## to_kebab_case

```python
to_kebab_case() -> Expression
```

Convert a string to kebab case.

Tip: See Also
    [`daft.functions.to_kebab_case`](https://docs.daft.ai/en/stable/api/functions/to_kebab_case/)

## to_snake_case

```python
to_snake_case() -> Expression
```

Convert a string to snake case.

Tip: See Also
    [`daft.functions.to_snake_case`](https://docs.daft.ai/en/stable/api/functions/to_snake_case/)

## to_title_case

```python
to_title_case() -> Expression
```

Convert a string to title case.

Tip: See Also
    [`daft.functions.to_title_case`](https://docs.daft.ai/en/stable/api/functions/to_title_case/)

## to_unix_epoch

```python
to_unix_epoch(time_unit: builtins.str | TimeUnit | None=None) -> Expression
```

Converts a datetime column to a Unix timestamp with the specified time unit. (default: seconds).

Tip: See Also
    [`daft.functions.to_unix_epoch`](https://docs.daft.ai/en/stable/api/functions/to_unix_epoch/)

## to_upper_camel_case

```python
to_upper_camel_case() -> Expression
```

Convert a string to upper camel case.

Tip: See Also
    [`daft.functions.to_upper_camel_case`](https://docs.daft.ai/en/stable/api/functions/to_upper_camel_case/)

## to_upper_kebab_case

```python
to_upper_kebab_case() -> Expression
```

Convert a string to upper kebab case.

Tip: See Also
    [`daft.functions.to_upper_kebab_case`](https://docs.daft.ai/en/stable/api/functions/to_upper_kebab_case/)

## to_upper_snake_case

```python
to_upper_snake_case() -> Expression
```

Convert a string to upper snake case.

Tip: See Also
    [`daft.functions.to_upper_snake_case`](https://docs.daft.ai/en/stable/api/functions/to_upper_snake_case/)

## tokenize_decode

```python
tokenize_decode(tokens_path: builtins.str, *, io_config: IOConfig | None=None, pattern: builtins.str | None=None, special_tokens: builtins.str | None=None) -> Expression
```

Decodes each list of integer tokens into a string using a tokenizer.

Tip: See Also
    [`daft.functions.tokenize_decode`](https://docs.daft.ai/en/stable/api/functions/tokenize_decode/)

## tokenize_encode

```python
tokenize_encode(tokens_path: builtins.str, *, io_config: IOConfig | None=None, pattern: builtins.str | None=None, special_tokens: builtins.str | None=None, use_special_tokens: bool | None=None) -> Expression
```

Encodes each string as a list of integer tokens using a tokenizer.

Tip: See Also
    [`daft.functions.tokenize_encode`](https://docs.daft.ai/en/stable/api/functions/tokenize_encode/)

## total_days

```python
total_days() -> Expression
```

Calculates the total number of days for a duration column.

Tip: See Also
    [`daft.functions.total_days`](https://docs.daft.ai/en/stable/api/functions/total_days/)

## total_hours

```python
total_hours() -> Expression
```

Calculates the total number of hours for a duration column.

Tip: See Also
    [`daft.functions.total_hours`](https://docs.daft.ai/en/stable/api/functions/total_hours/)

## total_microseconds

```python
total_microseconds() -> Expression
```

Calculates the total number of microseconds for a duration column.

Tip: See Also
    [`daft.functions.total_microseconds`](https://docs.daft.ai/en/stable/api/functions/total_microseconds/)

## total_milliseconds

```python
total_milliseconds() -> Expression
```

Calculates the total number of milliseconds for a duration column.

Tip: See Also
    [`daft.functions.total_milliseconds`](https://docs.daft.ai/en/stable/api/functions/total_milliseconds/)

## total_minutes

```python
total_minutes() -> Expression
```

Calculates the total number of minutes for a duration column.

Tip: See Also
    [`daft.functions.total_minutes`](https://docs.daft.ai/en/stable/api/functions/total_minutes/)

## total_nanoseconds

```python
total_nanoseconds() -> Expression
```

Calculates the total number of nanoseconds for a duration column.

Tip: See Also
    [`daft.functions.total_nanoseconds`](https://docs.daft.ai/en/stable/api/functions/total_nanoseconds/)

## total_seconds

```python
total_seconds() -> Expression
```

Calculates the total number of seconds for a duration column.

Tip: See Also
    [`daft.functions.total_seconds`](https://docs.daft.ai/en/stable/api/functions/total_seconds/)

## translate

```python
translate(from_str: builtins.str | Expression, to_str: builtins.str | Expression) -> Expression
```

Translates characters in the string by replacing characters in 'from_str' with corresponding characters in 'to_str'.

Tip: See Also
    [`daft.functions.translate`](https://docs.daft.ai/en/stable/api/functions/translate/)

## try_cast

```python
try_cast(dtype: DataTypeLike) -> Expression
```

Attempts to cast an expression to the given datatype, returning null on failure.

Unlike `cast`, this method does not raise an error when the conversion fails.
Instead, it returns null for values that cannot be converted.

Tip: See Also
    [`daft.functions.try_cast`](https://docs.daft.ai/en/stable/api/functions/try_cast/)

## try_compress

```python
try_compress(codec: COMPRESSION_CODEC) -> Expression
```

Compress or null if unsuccessful.

Tip: See Also
    [`daft.functions.try_compress`](https://docs.daft.ai/en/stable/api/functions/try_compress/)

## try_decode

```python
try_decode(charset: ENCODING_CHARSET) -> Expression
```

Decode or null if unsuccessful.

Tip: See Also
    [`daft.functions.try_decode`](https://docs.daft.ai/en/stable/api/functions/try_decode/)

## try_decompress

```python
try_decompress(codec: COMPRESSION_CODEC) -> Expression
```

Decompress or null if unsuccessful.

Tip: See Also
    [`daft.functions.try_decompress`](https://docs.daft.ai/en/stable/api/functions/try_decompress/)

## try_deserialize

```python
try_deserialize(format: Literal['json'], dtype: DataTypeLike) -> Expression
```

Deserializes the expression (string) using the specified format and data type, inserting nulls on failures.

Tip: See Also
    [`daft.functions.try_deserialize`](https://docs.daft.ai/en/stable/api/functions/try_deserialize/)

## try_encode

```python
try_encode(charset: ENCODING_CHARSET) -> Expression
```

Encode or null if unsuccessful.

Tip: See Also
    [`daft.functions.try_encode`](https://docs.daft.ai/en/stable/api/functions/try_encode/)

## udf

```python
udf(name: builtins.str, inner: UninitializedUdf, bound_args: BoundUDFArgs, expressions: builtins.list[Expression], return_dtype: DataType, init_args: InitArgsType, resource_request: ResourceRequest | None, batch_size: int | None, concurrency: int | None, use_process: bool | None, ray_options: dict[builtins.str, builtins.str] | None=None) -> Expression
```

_(no docstring)_

## unix_date

```python
unix_date() -> Expression
```

Retrieves the number of days since 1970-01-01 00:00:00 UTC.

Tip: See Also
    [`daft.functions.unix_date`](https://docs.daft.ai/en/stable/api/functions/unix_date/)

## unnest

```python
unnest() -> Expression
```

Flatten the fields of a struct expression into columns in a DataFrame.

Tip: See Also
    [`daft.functions.unnest`](https://docs.daft.ai/en/stable/api/functions/unnest/)

## upload

```python
upload(location: builtins.str | Expression, max_connections: int=32, on_error: Literal['raise', 'null']='raise', io_config: IOConfig | None=None) -> Expression
```

Uploads a column of binary data to the provided location(s) (also supports S3, local etc).

Tip: See Also
    [`daft.functions.upload`](https://docs.daft.ai/en/stable/api/functions/upload/)

## upper

```python
upper() -> Expression
```

Convert UTF-8 string to all upper.

Tip: See Also
    [`daft.functions.upper`](https://docs.daft.ai/en/stable/api/functions/upper/)

## value_counts

```python
value_counts() -> Expression
```

Counts the occurrences of each distinct value in the list.

Tip: See Also
    [`daft.functions.value_counts`](https://docs.daft.ai/en/stable/api/functions/value_counts/)

## var

```python
var(ddof: int=1) -> Expression
```

Calculates the variance of the values in the expression.

Args:
    ddof: Delta degrees of freedom. The divisor used in calculations
        is N - ddof, where N is the number of non-null elements.
        Defaults to 1 (sample variance).

Tip: See Also
    [`daft.functions.var`](https://docs.daft.ai/en/stable/api/functions/var/)

## video_frames

```python
video_frames(*, start_time: float=0, end_time: float | None=None, width: int | None=None, height: int | None=None, is_key_frame: bool | None=None) -> Expression
```

Decodes video frames from a video file.

Tip: See Also
    [`daft.functions.video_frames`](https://docs.daft.ai/en/stable/api/functions/video_frames/)

## video_keyframes

```python
video_keyframes(*, start_time: float=0, end_time: float | None=None) -> Expression
```

Gets keyframes for a video file.

Tip: See Also
    [`daft.functions.video_keyframes`](https://docs.daft.ai/en/stable/api/functions/video_keyframes/)

## video_metadata

```python
video_metadata() -> Expression
```

Gets metadata for a video file.

Tip: See Also
    [`daft.functions.video_metadata`](https://docs.daft.ai/en/stable/api/functions/video_metadata/)

## week_of_year

```python
week_of_year() -> Expression
```

Retrieves the week of the year for a datetime column.

Tip: See Also
    [`daft.functions.week_of_year`](https://docs.daft.ai/en/stable/api/functions/week_of_year/)

## year

```python
year() -> Expression
```

Retrieves the year for a datetime column.

Tip: See Also
    [`daft.functions.year`](https://docs.daft.ai/en/stable/api/functions/year/)
