# functions str

## ascii_func

```python
ascii_func(expr: Expression) -> Expression
```

Returns the ASCII numeric value of the first character of the string.

Returns 0 for empty strings. This is compatible with Spark's ascii function.

Args:
    expr: The string expression

Returns:
    Expression: an Int32 expression with the ASCII value

## capitalize

```python
capitalize(expr: Expression) -> Expression
```

Capitalize a UTF-8 string.

Returns:
    Expression: a String expression which is `self` uppercased with the first character and lowercased the rest

## chr_func

```python
chr_func(expr: Expression) -> Expression
```

Converts an ASCII numeric value to a character.

Returns the character corresponding to the ASCII code.
This is compatible with Spark's chr function.

Args:
    expr: An integer expression representing the ASCII code

Returns:
    Expression: a String expression with the character

## concat_ws

```python
concat_ws(sep: str, *exprs: Expression) -> Expression
```

Concatenates strings with a separator, skipping null values.

Null values in any expression are skipped rather than propagating nulls.
The separator is only inserted between non-null values. Returns null only
if all inputs are null for that row.

Args:
    sep (str): The separator string to place between values.
    *exprs (Expression): Two or more string expressions to concatenate.

Returns:
    Expression (String Expression): An expression with the joined strings,
        or null if all inputs are null for that row.

## contains

```python
contains(expr: Expression, substr: str | Expression) -> Expression
```

Checks whether each string contains the given substring in a string column.

Args:
    expr: The expression to check.
    substr: The substring to search for as a literal string, or as a column to pick values from

Returns:
    Expression: a Boolean expression indicating whether each value contains the provided substring

## count_matches

```python
count_matches(expr: Expression, patterns: Any, *, whole_words: bool=False, case_sensitive: bool=True) -> Expression
```

Counts the number of times a pattern, or multiple patterns, appear in a string.

If whole_words is true, then matches are only counted if they are whole words. This
also applies to multi-word strings. For example, on the string "abc def", the strings
"def" and "abc def" would be matched, but "bc de", "abc d", and "abc " (with the space)
would not.

If case_sensitive is false, then case will be ignored. This only applies to ASCII
characters; unicode uppercase/lowercase will still be considered distinct.

Args:
    expr: The expression to check.
    patterns: A pattern or a list of patterns.
    whole_words: Whether to only match whole word(s). Defaults to false.
    case_sensitive: Whether the matching should be case sensitive. Defaults to true.

Note:
    If a pattern is a substring of another pattern, the longest pattern is matched first.
    For example, in the string "hello world", with patterns "hello", "world", and "hello world",
    one match is counted for "hello world".

## damerau_levenshtein_distance

```python
damerau_levenshtein_distance(left: Expression, right: Expression) -> Expression
```

Compute the Damerau-Levenshtein distance between two strings.

This extends the Levenshtein distance by also counting transpositions of two
adjacent characters as a single edit operation (in addition to insertions,
deletions, and substitutions).

Note:
    This computes the Optimal String Alignment (OSA) variant, which does not
    allow a substring to be edited more than once. Results may differ from the
    true Damerau-Levenshtein distance for inputs with overlapping transpositions
    (e.g., ``"CA"`` to ``"ABC"`` is 3 under OSA but 2 under true
    Damerau-Levenshtein). OSA does not satisfy the triangle inequality.

Args:
    left: The left string expression to compare.
    right: The right string expression to compare against.

Returns:
    The Damerau-Levenshtein (OSA) distance for each pair of strings. Returns null
    when either input is null.

## deserialize

```python
deserialize(expr: Expression, format: Literal['json'], dtype: DataTypeLike) -> Expression
```

Deserializes a string using the specified format and data type.

Args:
    expr: The expression to deserialize.
    format (Literal["json"]): The serialization format.
    dtype: The target data type to deserialize into.

Returns:
    Expression: A new expression with the deserialized value.

## endswith

```python
endswith(expr: Expression, suffix: str | Expression) -> Expression
```

Checks whether each string ends with the given suffix in a string column.

Args:
    expr: The expression to check.
    suffix: The suffix to search for as a literal string, or as a column to pick values from

Returns:
    Expression: a Boolean expression indicating whether each value ends with the provided suffix

## find

```python
find(expr: Expression, substr: str | Expression) -> Expression
```

Returns the index of the first occurrence of the substring in each string.

Returns:
    Expression: an Int64 expression with the index of the first occurrence of the substring in each string

Note:
    The returned index is 0-based. If the substring is not found, -1 is returned.

## format

```python
format(f_string: str, *args: Expression | str) -> Expression
```

Format a string using the given arguments.

Args:
    f_string: The format string.
    *args: The arguments to format the string with.

Returns:
    Expression: A string expression with the formatted result.

## hamming_distance_str

```python
hamming_distance_str(left: Expression, right: Expression) -> Expression
```

Compute the character-level Hamming distance between two strings.

The Hamming distance is the number of positions at which the corresponding
characters are different.

Args:
    left: The left string expression to compare.
    right: The right string expression to compare against.

Returns:
    The Hamming distance for each pair of strings. Returns null when either input
    is null or the two strings have different lengths.

## ilike

```python
ilike(expr: Expression, pattern: str | Expression) -> Expression
```

Checks whether each string matches the given SQL ILIKE pattern, case insensitive.

Returns:
    Expression: a Boolean expression indicating whether each value matches the provided pattern

Note:
    Use % as a multiple-character wildcard or _ as a single-character wildcard.

## jaro_similarity

```python
jaro_similarity(left: Expression, right: Expression) -> Expression
```

Compute the Jaro similarity between two strings.

The Jaro similarity is a measure of similarity between two strings, based on
matching characters and transpositions. Returns a value between 0.0 (no similarity)
and 1.0 (identical strings).

Args:
    left: The left string expression to compare.
    right: The right string expression to compare against.

Returns:
    The Jaro similarity (0.0 to 1.0) for each pair of strings. Returns null when
    either input is null.

## jaro_winkler_similarity

```python
jaro_winkler_similarity(left: Expression, right: Expression) -> Expression
```

Compute the Jaro-Winkler similarity between two strings.

This is the Jaro similarity with a prefix bonus for strings sharing a common
prefix (up to 4 characters). Returns a value between 0.0 (no similarity) and
1.0 (identical strings).

Args:
    left: The left string expression to compare.
    right: The right string expression to compare against.

Returns:
    The Jaro-Winkler similarity (0.0 to 1.0) for each pair of strings. Returns
    null when either input is null.

## jq

```python
jq(expr: Expression, filter: str) -> Expression
```

Applies a [jq](https://jqlang.github.io/jq/manual/) filter to a string, returning the results as a string.

Args:
    expr: The expression to apply the jq filter to.
    filter (str): The jq filter to apply.

Returns:
    Expression: Expression representing the result of the jq filter as a column of JSON-compatible strings.

Warning:
    This expression uses [jaq](https://github.com/01mf02/jaq) as its filter executor which can differ from the
    [jq](https://jqlang.org/) command-line tool. Please consult [jq vs. jaq](https://github.com/01mf02/jaq?tab=readme-ov-file#differences-between-jq-and-jaq)
    for a detailed look into possible differences.

## json_array_length

```python
json_array_length(expr: Expression) -> Expression
```

Returns the number of elements in the outermost JSON array.

Returns ``NULL`` when the input is ``NULL``, cannot be parsed as JSON,
or the parsed JSON is not an array. Equivalent to Spark's
``json_array_length``.

Args:
    expr: A string expression containing JSON.

Returns:
    Expression: An ``Int32`` expression with the array length.

## json_object_keys

```python
json_object_keys(expr: Expression) -> Expression
```

Returns the top-level keys of a JSON object as a list of strings.

Returns ``NULL`` when the input is ``NULL``, cannot be parsed as JSON,
or the parsed JSON is not an object. Returns an empty list for empty
objects.

Note:
    Keys are returned in **sorted alphabetical order**, not source
    insertion order. This differs from Spark's ``json_object_keys``,
    which preserves insertion order.

Args:
    expr: A string expression containing JSON.

Returns:
    Expression: A ``List[String]`` expression with the object's keys.

## json_tuple

```python
json_tuple(expr: Expression, *fields: str) -> Expression
```

Extracts the values for the given top-level keys from a JSON object string.

Spark's ``json_tuple`` returns one column per requested key (``c0``,
``c1``, ...). To fit Daft's single-output expression model, this returns
a ``Struct`` whose field names are the requested keys, each typed as
``String``. Use ``.get("key")`` to pull individual fields out.

Behavior:

* Non-string scalar values (numbers, booleans) are stringified without
  surrounding quotes (e.g. ``"1"``, ``"true"``).
* Nested objects/arrays are returned as their JSON-encoded string form.
* Missing keys yield ``NULL`` for that field only; the row itself is
  still valid as long as the input parses as a JSON object.
* Malformed JSON, non-object roots, and ``NULL`` inputs yield a
  row-level ``NULL`` (``is_null()`` returns ``True``); every child
  field is also ``NULL``.
* Field names must be unique; passing a duplicate raises an error.

Args:
    expr: A string expression containing JSON.
    *fields: One or more top-level keys to extract.

Returns:
    Expression: A ``Struct`` expression with one ``String`` field per key.

## left

```python
left(expr: Expression, nchars: int | Expression) -> Expression
```

Gets the n (from nchars) left-most characters of each string.

Returns:
    Expression: a String expression which is the `n` left-most characters of `self`

## length_bytes

```python
length_bytes(expr: Expression) -> Expression
```

Retrieves the length for a UTF-8 string column in bytes.

Returns:
    Expression: an UInt64 expression with the length of each string

## levenshtein_distance

```python
levenshtein_distance(left: Expression, right: Expression) -> Expression
```

Compute the Levenshtein edit distance between two strings.

The Levenshtein distance is the minimum number of single-character insertions,
deletions, or substitutions required to transform one string into the other.

Args:
    left: The left string expression to compare.
    right: The right string expression to compare against.

Returns:
    The Levenshtein distance for each pair of strings. Returns null when either
    input is null.

## like

```python
like(expr: Expression, pattern: str | Expression) -> Expression
```

Checks whether each string matches the given SQL LIKE pattern, case sensitive.

Returns:
    Expression: a Boolean expression indicating whether each value matches the provided pattern

Note:
    Use % as a multiple-character wildcard or _ as a single-character wildcard.

## lower

```python
lower(expr: Expression) -> Expression
```

Convert UTF-8 string to all lowercase.

Returns:
    Expression: a String expression which is `self` lowercased

## lpad

```python
lpad(expr: Expression, length: int | Expression, pad: str | Expression) -> Expression
```

Left-pads each string by truncating on the right or padding with the character.

Returns:
    Expression: a String expression which is `self` truncated or left-padded with the pad character

Note:
    If the string is longer than the specified length, it will be truncated on the right.
    The pad character must be a single character.

## lstrip

```python
lstrip(expr: Expression) -> Expression
```

Strip whitespace from the left side of a UTF-8 string.

Returns:
    Expression: a String expression which is `self` with leading whitespace stripped

## normalize

```python
normalize(expr: Expression, *, remove_punct: bool=False, lowercase: bool=False, nfd_unicode: bool=False, white_space: bool=False) -> Expression
```

Normalizes a string for more useful deduplication.

Args:
    expr: The expression to normalize.
    remove_punct: Whether to remove all punctuation (ASCII).
    lowercase: Whether to convert the string to lowercase.
    nfd_unicode: Whether to normalize and decompose Unicode characters according to NFD.
    white_space: Whether to normalize whitespace, replacing newlines etc with spaces and removing double spaces.

Returns:
    Expression: a String expression which is normalized.

Note:
    All processing options are off by default.

## regexp

```python
regexp(expr: Expression, pattern: str | Expression) -> Expression
```

Check whether each string matches the given regular expression pattern in a string column.

Args:
    expr: String expression to search in
    pattern: Regex pattern to search for as string or as a column to pick values from

Returns:
    Expression: a Boolean expression indicating whether each value matches the provided pattern

## regexp_count

```python
regexp_count(expr: Expression, pattern: str | Expression) -> Expression
```

Counts the number of times a regex pattern appears in a string.

Args:
    expr: The expression to check.
    pattern: The regex pattern to search for as a string or as a column to pick values from.

Returns:
    Expression: An UInt64 expression with the count of regex matches for each string.

## regexp_extract

```python
regexp_extract(expr: Expression, pattern: str | Expression, index: int=0) -> Expression
```

Extracts the specified match group from the first regex match in each string in a string column.

Args:
    expr: String expression to extract from
    pattern: The regex pattern to extract
    index: The index of the regex match group to extract

Returns:
    Expression: a String expression with the extracted regex match

Note:
    If index is 0, the entire match is returned.
    If the pattern does not match or the group does not exist, a null value is returned.

## regexp_extract_all

```python
regexp_extract_all(expr: Expression, pattern: str | Expression, index: int=0) -> Expression
```

Extracts the specified match group from all regex matches in each string in a string column.

Args:
    expr: String expression to extract from
    pattern: The regex pattern to extract
    index: The index of the regex match group to extract

Returns:
    Expression: a List[String] expression with the extracted regex matches

Note:
    This expression always returns a list of strings.
    If index is 0, the entire match is returned. If the pattern does not match or the group does not exist, an empty list is returned.

## regexp_replace

```python
regexp_replace(expr: Expression, pattern: str | Expression, replacement: str | Expression) -> Expression
```

Replaces all occurrences of a regex pattern in a string column with a replacement string.

Args:
    expr: The string expression to be replaced
    pattern: The pattern to replace
    replacement: The replacement string

Returns:
    Expression: a String expression with patterns replaced by the replacement string

## regexp_split

```python
regexp_split(expr: Expression, pattern: str | Expression) -> Expression
```

Splits each string on the given regex pattern, into a list of strings.

Args:
    expr: The expression to split.
    pattern: The pattern on which each string should be split, or a column to pick such patterns from.

Returns:
    Expression: A List[String] expression containing the string splits for each string in the column.

## repeat

```python
repeat(expr: Expression, n: int | Expression) -> Expression
```

Repeats each string n times.

Returns:
    Expression: a String expression which is `self` repeated `n` times

## replace

```python
replace(expr: Expression, search: str | Expression, replacement: str | Expression) -> Expression
```

Replaces all occurrences of a substring in a string with a replacement string.

Args:
    expr: The string expression to be replaced
    search: The substring to replace
    replacement: The replacement string

Returns:
    Expression: a String expression with patterns replaced by the replacement string

## reverse

```python
reverse(expr: Expression) -> Expression
```

Reverse a UTF-8 string.

Returns:
    Expression: a String expression which is `self` reversed

## right

```python
right(expr: Expression, nchars: int | Expression) -> Expression
```

Gets the n (from nchars) right-most characters of each string.

Returns:
    Expression: a String expression which is the `n` right-most characters of `self`

## rpad

```python
rpad(expr: Expression, length: int | Expression, pad: str | Expression) -> Expression
```

Right-pads each string by truncating or padding with the character.

Returns:
    Expression: a String expression which is `self` truncated or right-padded with the pad character

Note:
    If the string is longer than the specified length, it will be truncated.
    The pad character must be a single character.

## rstrip

```python
rstrip(expr: Expression) -> Expression
```

Strip whitespace from the right side of a UTF-8 string.

Returns:
    Expression: a String expression which is `self` with trailing whitespace stripped

## serialize

```python
serialize(expr: Expression, format: Literal['json']) -> Expression
```

Serializes a value to a string using the specified format.

Args:
    expr: The expression to serialize.
    format (Literal["json"]): The serialization format.

Returns:
    Expression: A new expression with the serialized string.

## soundex

```python
soundex(expr: Expression) -> Expression
```

Returns the Soundex code of the string.

Soundex is a phonetic algorithm that produces a 4-character code representing
the sound of the string. This is compatible with Spark's soundex function.

Args:
    expr: The string expression

Returns:
    Expression: a String expression with the Soundex code

## space

```python
space(expr: Expression) -> Expression
```

Returns a string consisting of n space characters.

This is compatible with Spark's space function.

Args:
    expr: An integer expression representing the number of spaces

Returns:
    Expression: a String expression with n spaces

## split

```python
split(expr: Expression, split_on: str | Expression) -> Expression
```

Splits each string on the given string, into a list of strings.

Args:
    expr: The expression to split.
    split_on: The string on which each string should be split, or a column to pick such patterns from.

Returns:
    Expression: A List[String] expression containing the string splits for each string in the column.

## startswith

```python
startswith(expr: Expression, prefix: str | Expression) -> Expression
```

Checks whether each string starts with the given prefix in a string column.

Args:
    expr: The expression to check.
    prefix: The prefix to search for as a literal string, or as a column to pick values from

Returns:
    Expression: a Boolean expression indicating whether each value starts with the provided prefix

## strip

```python
strip(expr: Expression) -> Expression
```

Strip whitespace from both sides of string.

Returns:
    Expression: a String expression which is `self` with leading and trailing whitespace stripped

## substr

```python
substr(expr: Expression, start: int | Expression, length: int | Expression | None=None) -> Expression
```

Extract a substring from a string, starting at a specified index and extending for a given length.

Returns:
    Expression: A String expression representing the extracted substring.

Note:
    If `length` is not provided, the substring will include all characters from `start` to the end of the string.

## substring_index

```python
substring_index(expr: Expression, delim: str | Expression, count: int | Expression) -> Expression
```

Returns the substring from string before count occurrences of the delimiter.

If count is positive, returns everything to the left of the final delimiter (counting from left).
If count is negative, returns everything to the right of the final delimiter (counting from right).
This is compatible with Spark's substring_index function.

Args:
    expr: The string expression
    delim: The delimiter string
    count: The number of occurrences of the delimiter

Returns:
    Expression: a String expression with the substring result

## to_camel_case

```python
to_camel_case(expr: Expression) -> Expression
```

Convert a string to lower camel case.

Returns:
    Expression: a String expression converted to lower camel case

## to_kebab_case

```python
to_kebab_case(expr: Expression) -> Expression
```

Convert a string to kebab case.

Returns:
    Expression: a String expression converted to kebab case

## to_snake_case

```python
to_snake_case(expr: Expression) -> Expression
```

Convert a string to snake case.

Returns:
    Expression: a String expression converted to snake case

## to_title_case

```python
to_title_case(expr: Expression) -> Expression
```

Convert a string to title case.

Returns:
    Expression: a String expression converted to title case

## to_upper_camel_case

```python
to_upper_camel_case(expr: Expression) -> Expression
```

Convert a string to upper camel case.

Returns:
    Expression: a String expression converted to upper camel case

## to_upper_kebab_case

```python
to_upper_kebab_case(expr: Expression) -> Expression
```

Convert a string to upper kebab case.

Returns:
    Expression: a String expression converted to upper kebab case

## to_upper_snake_case

```python
to_upper_snake_case(expr: Expression) -> Expression
```

Convert a string to upper snake case.

Returns:
    Expression: a String expression converted to upper snake case

## tokenize_decode

```python
tokenize_decode(expr: Expression, tokens_path: str, *, io_config: IOConfig | None=None, pattern: str | None=None, special_tokens: str | None=None) -> Expression
```

Decodes each list of integer tokens into a string using a tokenizer.

Uses [https://github.com/openai/tiktoken](https://github.com/openai/tiktoken) for tokenization.

Supported built-in tokenizers: `cl100k_base`, `o200k_base`, `p50k_base`, `p50k_edit`, `r50k_base`. Also supports
loading tokens from a file in tiktoken format.

Args:
    expr: The expression to decode.
    tokens_path: The name of a built-in tokenizer, or the path to a token file (supports downloading).
    io_config (optional): IOConfig to use when accessing remote storage.
    pattern (optional): Regex pattern to use to split strings in tokenization step. Necessary if loading from a file.
    special_tokens (optional): Name of the set of special tokens to use. Currently only "llama3" supported. Necessary if loading from a file.

Returns:
    Expression: An expression with decoded strings.

## tokenize_encode

```python
tokenize_encode(expr: Expression, tokens_path: str, *, io_config: IOConfig | None=None, pattern: str | None=None, special_tokens: str | None=None, use_special_tokens: bool | None=None) -> Expression
```

Encodes each string as a list of integer tokens using a tokenizer.

Uses https://github.com/openai/tiktoken for tokenization.

Supported built-in tokenizers: `cl100k_base`, `o200k_base`, `p50k_base`, `p50k_edit`, `r50k_base`. Also supports
loading tokens from a file in tiktoken format.

Args:
    expr: The expression to encode.
    tokens_path: The name of a built-in tokenizer, or the path to a token file (supports downloading).
    io_config (optional): IOConfig to use when accessing remote storage.
    pattern (optional): Regex pattern to use to split strings in tokenization step. Necessary if loading from a file.
    special_tokens (optional): Name of the set of special tokens to use. Currently only "llama3" supported. Necessary if loading from a file.
    use_special_tokens (optional): Whether or not to parse special tokens included in input. Disabled by default. Automatically enabled if `special_tokens` is provided.

Returns:
    Expression: An expression with the encodings of the strings as lists of unsigned 32-bit integers.

Note:
    If using this expression with Llama 3 tokens, note that Llama 3 does some extra preprocessing on
    strings in certain edge cases. This may result in slightly different encodings in these cases.

## translate

```python
translate(expr: Expression, from_str: str | Expression, to_str: str | Expression) -> Expression
```

Translates characters in the input string by replacing characters in 'from_str' with corresponding characters in 'to_str'.

Characters in 'from_str' without a corresponding character in 'to_str' are removed.
This is compatible with Spark's translate function.

Args:
    expr: The string expression to translate
    from_str: Characters to be replaced
    to_str: Replacement characters

Returns:
    Expression: a String expression with characters translated

## try_deserialize

```python
try_deserialize(expr: Expression, format: Literal['json'], dtype: DataTypeLike) -> Expression
```

Deserializes a string using the specified format and data type, inserting nulls on failures.

Args:
    expr: The expression to deserialize.
    format (Literal["json"]): The serialization format.
    dtype: The target data type to deserialize into.

Returns:
    Expression: A new expression with the deserialized value (or null).

## upper

```python
upper(expr: Expression) -> Expression
```

Convert UTF-8 string to all upper.

Returns:
    Expression: a String expression which is `self` uppercased
