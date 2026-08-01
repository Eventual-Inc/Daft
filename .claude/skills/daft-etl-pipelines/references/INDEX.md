# Daft API index

`name | namespace | signature | summary | file#anchor`

abs | daft.functions | (expr: Expression) -> Expression | Absolute of a numeric expression. | functions-numeric.md#abs
abs | Expression | () -> Expression | Absolute of a numeric expression. | expressions.md#abs
add_months | daft.functions | (expr: Expression, months: Expression) -> Expression | Adds a number of months to a date or timestamp. | functions-datetime.md#add_months
agg | DataFrame | (*to_agg: Expression | Iterable[Expression]) -> DataFrame | Perform aggregations on this DataFrame. | dataframe.md#agg
agg_concat | DataFrame | (*cols: ColumnInputType, delimiter: str | None=None) -> DataFrame | Performs a global concatenation agg on the DataFrame. | dataframe.md#agg_concat
agg_list | DataFrame | (*cols: ColumnInputType) -> DataFrame | Performs a global list agg on the DataFrame. | dataframe.md#agg_list
agg_set | DataFrame | (*cols: ColumnInputType) -> DataFrame | Performs a global set agg on the DataFrame (ignoring nulls). | dataframe.md#agg_set
alias | Expression | (name: builtins.str) -> Expression | Gives the expression a new name. | expressions.md#alias
any_value | daft.functions | (expr: Expression, ignore_nulls: bool=False) -> Expression | Returns any non-null value from the expression. | functions-agg.md#any_value
any_value | DataFrame | (*cols: ColumnInputType) -> DataFrame | Returns an arbitrary value on this DataFrame. | dataframe.md#any_value
any_value | Expression | (ignore_nulls: bool=False) -> Expression | Returns any value in the expression. | expressions.md#any_value
apply | Expression | (func: Callable[..., Any], return_dtype: DataTypeLike) -> Expression | Apply a function on each value in a given expression. | expressions.md#apply
approx_count_distinct | daft.functions | (expr: Expression) -> Expression | Calculates the approximate number of non-`NULL` distinct values in the expression. | functions-agg.md#approx_count_distinct
approx_count_distinct | Expression | () -> Expression | Calculates the approximate number of non-`NULL` distinct values in the expression. | expressions.md#approx_count_distinct
approx_percentiles | daft.functions | (expr: Expression, percentiles: float | list[float]) -> Expression | Calculates the approximate percentile(s) for a column of numeric values. | functions-agg.md#approx_percentiles
approx_percentiles | Expression | (percentiles: builtins.float | builtins.list[builtins.float]) -> Expression | Calculates the approximate percentile(s) for a column of numeric values. | expressions.md#approx_percentiles
arccos | daft.functions | (expr: Expression) -> Expression | The elementwise arc cosine of a numeric expression. | functions-numeric.md#arccos
arccos | Expression | () -> Expression | The elementwise arc cosine of a numeric expression. | expressions.md#arccos
arccosh | daft.functions | (expr: Expression) -> Expression | The elementwise inverse hyperbolic cosine of a numeric expression. | functions-numeric.md#arccosh
arccosh | Expression | () -> Expression | The elementwise inverse hyperbolic cosine of a numeric expression. | expressions.md#arccosh
arcsin | daft.functions | (expr: Expression) -> Expression | The elementwise arc sine of a numeric expression. | functions-numeric.md#arcsin
arcsin | Expression | () -> Expression | The elementwise arc sine of a numeric expression. | expressions.md#arcsin
arcsinh | daft.functions | (expr: Expression) -> Expression | The elementwise inverse hyperbolic sine of a numeric expression. | functions-numeric.md#arcsinh
arcsinh | Expression | () -> Expression | The elementwise inverse hyperbolic sine of a numeric expression. | expressions.md#arcsinh
arctan | daft.functions | (expr: Expression) -> Expression | The elementwise arc tangent of a numeric expression. | functions-numeric.md#arctan
arctan | Expression | () -> Expression | The elementwise arc tangent of a numeric expression. | expressions.md#arctan
arctan2 | daft.functions | (y: Expression, x: Expression) -> Expression | Calculates the four quadrant arctangent of coordinates (y, x), in radians. | functions-numeric.md#arctan2
arctan2 | Expression | (other: Expression) -> Expression | Calculates the four quadrant arctangent of coordinates (y, x), in radians. | expressions.md#arctan2
arctanh | daft.functions | (expr: Expression) -> Expression | The elementwise inverse hyperbolic tangent of a numeric expression. | functions-numeric.md#arctanh
arctanh | Expression | () -> Expression | The elementwise inverse hyperbolic tangent of a numeric expression. | expressions.md#arctanh
as_py | Expression | () -> Any | Returns this literal expression as a python value, raises a ValueError if this is not a literal expression. | expressions.md#as_py
ascii | Expression | () -> Expression | Returns the ASCII numeric value of the first character of the string. | expressions.md#ascii
ascii_func | daft.functions | (expr: Expression) -> Expression | Returns the ASCII numeric value of the first character of the string. | functions-str.md#ascii_func
attach | daft | (object: Catalog | Provider | Table | UDF | DataFrame, alias: str | None=None) -> None | Attaches a known attachable object like a Catalog or Table. | toplevel.md#attach
attach_catalog | daft | (catalog: object | Catalog, alias: str | None=None) -> Catalog | Attaches an external catalog to the current session. | toplevel.md#attach_catalog
attach_function | daft | (function: UDF, alias: str | None=None) -> None | Attaches a Python function as a UDF in the current session. | toplevel.md#attach_function
attach_provider | daft | (provider: Provider, alias: str | None=None) -> Provider | Attaches a provider instance to the current session. | toplevel.md#attach_provider
attach_subscriber | daft | (alias: str, subscriber: Subscriber) -> DaftContext | Attaches a subscriber to the current context. | toplevel.md#attach_subscriber
attach_table | daft | (table: object | Table, alias: str | None=None) -> Table | Attaches an external table to the current session. | toplevel.md#attach_table
attach_view | daft | (view: DataFrame, alias: str) -> Table | Attaches a DataFrame as a non-materialized temporary view to the current session. | toplevel.md#attach_view
audio_file | daft.functions | (url: Expression, verify: bool=False, io_config: IOConfig | None=None) -> Expression | Converts a string containing a file reference to a `daft.AudioFile` reference. | functions-media.md#audio_file
audio_metadata | daft.functions | (file_expr: Expression) -> Expression | Get metadata for a audio file. | functions-media.md#audio_metadata
AudioFile | daft | (url: str, io_config: IOConfig | None=None) -> None | An audio-specific file interface that provides audio operations. | toplevel.md#audiofile
avg | daft.functions | (expr: Expression) -> Expression | Calculates the mean of the values in the expression. | functions-agg.md#avg
avg | Expression | () -> Expression | Alias for `Expression.mean()`. | expressions.md#avg
AzureConfig | daft.io | (storage_account: str | None=None, access_key: str | None=None, sas_token: str | None=None, bearer_token: str | None=None, tenant_id: str | None=None, client_id: str | None=None, client_secret: str | None=None, use_fabric_endpoint: bool | None=None, anonymous: bool | None=None, endpoint_url: str | None=None, use_ssl: bool | None=None, max_connections: int | None=None) | I/O configuration for accessing Azure Blob Storage. | io.md#azureconfig
between | daft.functions | (expr: Expression, lower: Expression | int | float, upper: Expression | int | float) -> Expression | Checks if values in the Expression are between lower and upper, inclusive. | functions-numeric.md#between
between | Expression | (lower: int | builtins.float, upper: int | builtins.float) -> Expression | Checks if values in the Expression are between lower and upper, inclusive. | expressions.md#between
bin | daft.functions | (expr: Expression) -> Expression | Returns the string representation of the binary value of an integer. | functions-numeric.md#bin
bitwise_and | daft.functions | (left: Expression, right: Expression) -> Expression | Bitwise AND of two integer expressions. | functions-etc.md#bitwise_and
bitwise_and | Expression | (other: Expression) -> Expression | Bitwise AND of two integer expressions. | expressions.md#bitwise_and
bitwise_or | daft.functions | (left: Expression, right: Expression) -> Expression | Bitwise OR of two integer expressions. | functions-etc.md#bitwise_or
bitwise_or | Expression | (other: Expression) -> Expression | Bitwise OR of two integer expressions. | expressions.md#bitwise_or
bitwise_xor | daft.functions | (left: Expression, right: Expression) -> Expression | Bitwise XOR of two integer expressions. | functions-etc.md#bitwise_xor
bitwise_xor | Expression | (other: Expression) -> Expression | Bitwise XOR of two integer expressions. | expressions.md#bitwise_xor
bool_and | daft.functions | (expr: Expression) -> Expression | Calculates the boolean AND of all values in the expression. | functions-agg.md#bool_and
bool_and | Expression | () -> Expression | Calculates the boolean AND of all values in a list. | expressions.md#bool_and
bool_or | daft.functions | (expr: Expression) -> Expression | Calculates the boolean OR of all values in the expression. | functions-agg.md#bool_or
bool_or | Expression | () -> Expression | Calculates the boolean OR of all values in a list. | expressions.md#bool_or
capitalize | daft.functions | (expr: Expression) -> Expression | Capitalize a UTF-8 string. | functions-str.md#capitalize
capitalize | Expression | () -> Expression | Capitalize a UTF-8 string. | expressions.md#capitalize
cast | daft.functions | (expr: Expression, dtype: DataTypeLike) -> Expression | Casts an expression to the given datatype if possible. | functions-misc.md#cast
cast | Expression | (dtype: DataTypeLike) -> Expression | Casts an expression to the given datatype if possible. | expressions.md#cast
Catalog | daft | () | Interface for Python catalog implementations. | toplevel.md#catalog
cbrt | daft.functions | (expr: Expression) -> Expression | The cube root of a numeric expression. | functions-numeric.md#cbrt
cbrt | Expression | () -> Expression | The cube root of a numeric expression. | expressions.md#cbrt
ceil | daft.functions | (expr: Expression) -> Expression | The ceiling of a numeric expression. | functions-numeric.md#ceil
ceil | Expression | () -> Expression | The ceiling of a numeric expression. | expressions.md#ceil
CheckpointConfig | daft | (store: CheckpointStore, on: str, settings: KeyFilteringSettings | None=None) -> None | Per-source checkpoint configuration. | toplevel.md#checkpointconfig
CheckpointStore | daft | (path: str, io_config: IOConfig | None=None) -> None | A checkpoint store for tracking which source keys have been processed. | toplevel.md#checkpointstore
chr_func | daft.functions | (expr: Expression) -> Expression | Converts an ASCII numeric value to a character. | functions-str.md#chr_func
chunk | daft.functions | (list_expr: Expression, size: int) -> Expression | Splits each list into chunks of the given size. | functions-list.md#chunk
chunk | Expression | (size: int) -> Expression | Splits each list into chunks of the given size. | expressions.md#chunk
classify_image | daft.functions | (image: Expression, labels: Label | list[Label], *, provider: str | Provider | None=None, model: str | None=None, **options: Unpack[ClassifyImageOptions]) -> Expression | Returns an expression that classifies images using the specified model and provider. | functions-ai.md#classify_image
classify_text | daft.functions | (text: Expression, labels: Label | list[Label], *, provider: str | Provider | None=None, model: str | None=None, **options: Unpack[ClassifyTextOptions]) -> Expression | Returns an expression that classifies text using the specified model and provider. | functions-ai.md#classify_text
clip | daft.functions | (expr: Expression, min: Expression | None=None, max: Expression | None=None) -> Expression | Clips an expression to the given minimum and maximum values. | functions-numeric.md#clip
clip | Expression | (min: Expression | None=None, max: Expression | None=None) -> Expression | Clips an expression to the given minimum and maximum values. | expressions.md#clip
cls | daft | (class_: type | None=None, *, cpus: float | None=None, gpus: float=0, use_process: bool | None=None, max_concurrency: int | None=None, max_retries: int | None=None, on_error: Literal['raise', 'log', 'ignore'] | None=None, name_override: str | None=None, ray_options: dict[str, Any] | None=None) -> type | Callable[[type], type] | Decorator to convert a Python class into a Daft user-defined class. | toplevel.md#cls
coalesce | daft.functions | (*args: Expression) -> Expression | Returns the first non-null value in a list of expressions. | functions-misc.md#coalesce
coalesce | Expression | (*others: Expression) -> Expression | Returns the first non-null value among this expression and the provided expressions. | expressions.md#coalesce
col | daft | (name: str) -> Expression | Creates an Expression referring to the column with the provided name. | toplevel.md#col
collect | DataFrame | (num_preview_rows: int | None=8) -> DataFrame | Executes the entire DataFrame and materializes the results. | dataframe.md#collect
column_name | Expression | () -> builtins.str | None |  | expressions.md#column_name
column_names | DataFrame | () -> list[str] | Returns column names of DataFrame as a list of strings. | dataframe.md#column_names
columns | DataFrame | () -> list[Expression] | Returns column of DataFrame as a list of Expressions. | dataframe.md#columns
columns_avg | daft.functions | (*exprs: Expression | str) -> Expression | Average values across columns. | functions-etc.md#columns_avg
columns_max | daft.functions | (*exprs: Expression | str) -> Expression | Find the maximum value across columns. | functions-etc.md#columns_max
columns_mean | daft.functions | (*exprs: Expression | str) -> Expression | Average values across columns. | functions-etc.md#columns_mean
columns_min | daft.functions | (*exprs: Expression | str) -> Expression | Find the minimum value across columns. | functions-etc.md#columns_min
columns_sum | daft.functions | (*exprs: Expression | str) -> Expression | Sum values across columns. | functions-etc.md#columns_sum
compress | daft.functions | (expr: Expression, codec: COMPRESSION_CODEC) -> Expression | Compress binary or string values using the specified codec. | functions-etc.md#compress
compress | Expression | (codec: COMPRESSION_CODEC) -> Expression | Compress binary or string values using the specified codec. | expressions.md#compress
concat | daft | (dfs: Iterable['DataFrame']) -> DataFrame | Concatenates multiple DataFrames into a single DataFrame. | toplevel.md#concat
concat | daft.functions | (left: Expression | str | bytes, right: Expression | str | bytes) -> Expression | Concatenates two string or binary values. | functions-misc.md#concat
concat | DataFrame | (other: 'DataFrame') -> DataFrame | Concatenates two DataFrames together in a "vertical" concatenation. | dataframe.md#concat
concat | Expression | (other: Expression | builtins.str | bytes) -> Expression | Concatenate two string expressions. | expressions.md#concat
concat_ws | daft.functions | (sep: str, *exprs: Expression) -> Expression | Concatenates strings with a separator, skipping null values. | functions-str.md#concat_ws
contains | daft.functions | (expr: Expression, substr: str | Expression) -> Expression | Checks whether each string contains the given substring in a string column. | functions-str.md#contains
contains | Expression | (substr: builtins.str | Expression) -> Expression | Checks whether each string contains the given pattern in a string column. | expressions.md#contains
context | daft | (submodule) |  | toplevel.md#context
conv | daft.functions | (expr: Expression, from_base: int, to_base: int) -> Expression | Converts a number from base ``from_base`` to base ``to_base`` (bases 2-36). | functions-numeric.md#conv
convert_image | daft.functions | (image: Expression, mode: str | ImageMode) -> Expression | Convert an image expression to the specified mode. | functions-media.md#convert_image
convert_image | Expression | (mode: builtins.str | ImageMode) -> Expression | Convert an image expression to the specified mode. | expressions.md#convert_image
convert_time_zone | daft.functions | (expr: Expression, to_timezone: str, from_timezone: str | None=None) -> Expression | Converts a timestamp to another timezone while preserving the instant in time. | functions-datetime.md#convert_time_zone
convert_time_zone | Expression | (to_timezone: builtins.str, from_timezone: builtins.str | None=None) -> Expression | Converts a timestamp to another timezone while preserving the instant in time. | expressions.md#convert_time_zone
convert_timezone | daft.functions | (target_timezone: str, source_timestamp: Expression) -> Expression | Spark-style alias for :func:`convert_time_zone`. | functions-datetime.md#convert_timezone
cos | daft.functions | (expr: Expression) -> Expression | The elementwise cosine of a numeric expression. | functions-numeric.md#cos
cos | Expression | () -> Expression | The elementwise cosine of a numeric expression. | expressions.md#cos
CosConfig | daft.io | (region: str | None=None, endpoint: str | None=None, secret_id: str | None=None, secret_key: str | None=None, security_token: str | None=None, anonymous: bool | None=None, max_retries: int | None=None, retry_timeout_ms: int | None=None, connect_timeout_ms: int | None=None, read_timeout_ms: int | None=None, max_concurrent_requests: int | None=None, max_connections: int | None=None) | I/O configuration for accessing Tencent Cloud COS (Cloud Object Storage). | io.md#cosconfig
cosh | daft.functions | (expr: Expression) -> Expression | The elementwise hyperbolic cosine of a numeric expression. | functions-numeric.md#cosh
cosh | Expression | () -> Expression | The elementwise hyperbolic cosine of a numeric expression. | expressions.md#cosh
cosine_distance | daft.functions | (left: Expression, right: Expression) -> Expression | Compute the cosine distance between two embeddings. | functions-etc.md#cosine_distance
cosine_distance | Expression | (other: Expression) -> Expression | Compute the cosine distance between two embeddings. | expressions.md#cosine_distance
cosine_similarity | daft.functions | (left: Expression, right: Expression) -> Expression | Compute the cosine similarity between two embeddings. | functions-etc.md#cosine_similarity
cosine_similarity | Expression | (other: Expression) -> Expression | Compute the cosine similarity between two embeddings. | expressions.md#cosine_similarity
cot | daft.functions | (expr: Expression) -> Expression | The elementwise cotangent of a numeric expression. | functions-numeric.md#cot
cot | Expression | () -> Expression | The elementwise cotangent of a numeric expression. | expressions.md#cot
count | daft.functions | (expr: Expression | None=None, mode: Literal['all', 'valid', 'null'] | CountMode=CountMode.Valid) -> Expression | Counts the number of values in the expression. | functions-agg.md#count
count | DataFrame | (*cols: ColumnInputType | int) -> DataFrame | Performs a global count on the DataFrame. | dataframe.md#count
count | Expression | (mode: Literal['all', 'valid', 'null'] | CountMode=CountMode.Valid) -> Expression | Counts the number of values in the expression. | expressions.md#count
count_distinct | daft.functions | (expr: Expression) -> Expression | Counts the number of distinct values in the expression. | functions-agg.md#count_distinct
count_distinct | DataFrame | (*cols: ColumnInputType) -> DataFrame | Performs a global count of distinct values on the DataFrame. | dataframe.md#count_distinct
count_distinct | Expression | () -> Expression | Counts the number of distinct values in the expression. | expressions.md#count_distinct
count_matches | daft.functions | (expr: Expression, patterns: Any, *, whole_words: bool=False, case_sensitive: bool=True) -> Expression | Counts the number of times a pattern, or multiple patterns, appear in a string. | functions-str.md#count_matches
count_matches | Expression | (patterns: Any, *, whole_words: bool=False, case_sensitive: bool=True) -> Expression | Counts the number of times a pattern, or multiple patterns, appear in a string. | expressions.md#count_matches
count_rows | DataFrame | () -> int | Executes the Dataframe to count the number of rows. | dataframe.md#count_rows
create_namespace | daft | (identifier: Identifier | str) -> None | Creates a namespace in the current session's active catalog. | toplevel.md#create_namespace
create_namespace_if_not_exists | daft | (identifier: Identifier | str) -> None | Creates a namespace in the current session's active catalog if it does not already exist. | toplevel.md#create_namespace_if_not_exists
create_table | daft | (identifier: Identifier | str, source: Schema | DataFrame, **properties: Any) -> Table | Creates a table in the current session's active catalog and namespace. | toplevel.md#create_table
create_table_if_not_exists | daft | (identifier: Identifier | str, source: Schema | DataFrame, **properties: Any) -> Table | Creates a table in the current session's active catalog and namespace if it does not already exist. | toplevel.md#create_table_if_not_exists
create_temp_table | daft | (identifier: str, source: Schema | DataFrame) -> Table | Creates a temp table scoped to current session's lifetime. | toplevel.md#create_temp_table
create_temp_view | daft | (identifier: str, view: DataFrame) -> Table | Creates or replaces a non-materialized temporary view in the current session. | toplevel.md#create_temp_view
crop | daft.functions | (image: Expression, bbox: tuple[int, int, int, int] | Expression) -> Expression | Crops images with the provided bounding box. | functions-media.md#crop
crop | Expression | (bbox: tuple[int, int, int, int] | Expression) -> Expression | Crops images with the provided bounding box. | expressions.md#crop
csc | daft.functions | (expr: Expression) -> Expression | The elementwise cosecant of a numeric expression. | functions-numeric.md#csc
csc | Expression | () -> Expression | The elementwise cosecant of a numeric expression. | expressions.md#csc
current_catalog | daft | () -> Catalog | None | Returns the active session's current catalog or None. | toplevel.md#current_catalog
current_date | daft.functions | () -> Expression | Returns the current date (UTC). | functions-datetime.md#current_date
current_model | daft | () -> str | None | Returns the active session's current model or None. | toplevel.md#current_model
current_namespace | daft | () -> Identifier | None | Returns the active session's current namespace or None. | toplevel.md#current_namespace
current_provider | daft | () -> Provider | None | Returns the active session's current provider or None. | toplevel.md#current_provider
current_session | daft | () -> Session | Returns the active session's current session. | toplevel.md#current_session
current_timestamp | daft.functions | () -> Expression | Returns the current timestamp (UTC) with microsecond precision. | functions-datetime.md#current_timestamp
current_timezone | daft.functions | () -> Expression | Returns the current timezone as a string (always 'UTC' in Daft). | functions-datetime.md#current_timezone
damerau_levenshtein_distance | daft.functions | (left: Expression, right: Expression) -> Expression | Compute the Damerau-Levenshtein distance between two strings. | functions-str.md#damerau_levenshtein_distance
damerau_levenshtein_distance | Expression | (other: Expression) -> Expression | Compute the Damerau-Levenshtein distance between two strings. | expressions.md#damerau_levenshtein_distance
DataFrame | daft | (builder: LogicalPlanBuilder) -> None | A Daft DataFrame is a table of data. | toplevel.md#dataframe
datasets | daft | (submodule) |  | toplevel.md#datasets
DataSink | daft.io | () | Interface for writing data to a sink that is not built-in. | io.md#datasink
DataSource | daft.io | () | DataSource is a low-level interface for reading data into DataFrames. | io.md#datasource
DataSourceTask | daft.io | () | DataSourceTask represents a partition of data that can be processed independently. | io.md#datasourcetask
DataType | daft | () -> None | A Daft DataType defines the type of all the values in an Expression or DataFrame column. | toplevel.md#datatype
date | daft.functions | (expr: Expression) -> Expression | Retrieves the date for a datetime column. | functions-datetime.md#date
date | Expression | () -> Expression | Retrieves the date for a datetime column. | expressions.md#date
date_add | daft.functions | (expr: Expression, days: Expression) -> Expression | Adds a number of days to a date. | functions-datetime.md#date_add
date_diff | daft.functions | (end: Expression, start: Expression) -> Expression | Returns the number of days between two dates. | functions-datetime.md#date_diff
date_format | daft.functions | (expr: Expression, format: str | None=None) -> Expression | Alias for ``strftime``. | functions-datetime.md#date_format
date_from_unix_date | daft.functions | (expr: Expression) -> Expression | Converts days since Unix epoch (1970-01-01) to a date. | functions-datetime.md#date_from_unix_date
date_sub | daft.functions | (expr: Expression, days: Expression) -> Expression | Subtracts a number of days from a date. | functions-datetime.md#date_sub
date_trunc | daft.functions | (interval: str, expr: Expression, relative_to: Expression | None=None) -> Expression | Truncates the datetime column to the specified interval. | functions-datetime.md#date_trunc
date_trunc | Expression | (interval: builtins.str, relative_to: Expression | None=None) -> Expression | Truncates the datetime column to the specified interval. | expressions.md#date_trunc
dateadd | daft.functions | (expr: Expression, days: Expression) -> Expression | Alias for ``date_add``. | functions-datetime.md#dateadd
datediff | daft.functions | (end: Expression, start: Expression) -> Expression | Alias for ``date_diff``. | functions-datetime.md#datediff
datepart | daft.functions | (part: str, expr: Expression) -> Expression | Alias-style extractor over existing temporal functions. | functions-datetime.md#datepart
day | daft.functions | (expr: Expression) -> Expression | Retrieves the day for a datetime column. | functions-datetime.md#day
day | Expression | () -> Expression | Retrieves the day for a datetime column. | expressions.md#day
day_of_month | daft.functions | (expr: Expression) -> Expression | Retrieves the day of the month for a datetime column. | functions-datetime.md#day_of_month
day_of_month | Expression | () -> Expression | Retrieves the day of the month for a datetime column. | expressions.md#day_of_month
day_of_week | daft.functions | (expr: Expression) -> Expression | Retrieves the day of the week for a datetime column, starting at 0 for Monday and ending at 6 for Sunday. | functions-datetime.md#day_of_week
day_of_week | Expression | () -> Expression | Retrieves the day of the week for a datetime column, starting at 0 for Monday and ending at 6 for Sunday. | expressions.md#day_of_week
day_of_year | daft.functions | (expr: Expression) -> Expression | Retrieves the ordinal day for a datetime column. | functions-datetime.md#day_of_year
day_of_year | Expression | () -> Expression | Retrieves the ordinal day for a datetime column. | expressions.md#day_of_year
dayofmonth | daft.functions | (expr: Expression) -> Expression | Alias for ``day_of_month``. | functions-datetime.md#dayofmonth
dayofyear | daft.functions | (expr: Expression) -> Expression | Alias for ``day_of_year``. | functions-datetime.md#dayofyear
decode | daft.functions | (bytes: Expression, charset: ENCODING_CHARSET) -> Expression | Decodes binary values using the specified character set. | functions-etc.md#decode
decode | Expression | (charset: ENCODING_CHARSET) -> Expression | Decodes binary values using the specified character set. | expressions.md#decode
decode_image | daft.functions | (bytes: Expression, on_error: Literal['raise', 'null']='raise', mode: str | ImageMode | None=ImageMode.RGB) -> Expression | Decodes the binary data in this column into images. | functions-media.md#decode_image
decode_image | Expression | (on_error: Literal['raise', 'null']='raise', mode: builtins.str | ImageMode | None=ImageMode.RGB) -> Expression | Decodes the binary data in this column into images. | expressions.md#decode_image
decode_image_file | daft.functions | (file_expr: Expression, mode: str | None=None, on_error: Literal['raise', 'null']='raise') -> Expression | Decode image files from a File column into an Image column. | functions-media.md#decode_image_file
decode_image_file | Expression | () -> Expression | Decodes an image file into an Image column. | expressions.md#decode_image_file
decompress | daft.functions | (bytes: Expression, codec: COMPRESSION_CODEC) -> Expression | Decompress binary values using the specified codec. | functions-etc.md#decompress
decompress | Expression | (codec: COMPRESSION_CODEC) -> Expression | Decompress binary values using the specified codec. | expressions.md#decompress
degrees | daft.functions | (expr: Expression) -> Expression | The elementwise degrees of a numeric expression. | functions-numeric.md#degrees
degrees | Expression | () -> Expression | The elementwise degrees of a numeric expression. | expressions.md#degrees
delete_deltalake | daft | (table: Union[str, 'UnityCatalogTable', 'deltalake.DeltaTable'], predicate: str | None=None, io_config: IOConfig | None=None, custom_metadata: dict[str, str] | None=None) -> dict[str, Any] | Delete rows from a Delta Lake table. | toplevel.md#delete_deltalake
dense_rank | daft.functions | () -> Expression | Return the dense rank of the current row (used for window functions). | functions-window.md#dense_rank
describe | DataFrame | () -> DataFrame | Returns the Schema of the DataFrame, which provides information about each column, as a new DataFrame. | dataframe.md#describe
deserialize | daft.functions | (expr: Expression, format: Literal['json'], dtype: DataTypeLike) -> Expression | Deserializes a string using the specified format and data type. | functions-str.md#deserialize
deserialize | Expression | (format: Literal['json'], dtype: DataTypeLike) -> Expression | Deserializes the expression (string) using the specified format and data type. | expressions.md#deserialize
detach_catalog | daft | (alias: str) -> None | Detaches the catalog from the current session. | toplevel.md#detach_catalog
detach_function | daft | (alias: str) -> None | Detaches a Python function as a UDF in the current session. | toplevel.md#detach_function
detach_provider | daft | (alias: str) -> None | Detaches the provider from the current session. | toplevel.md#detach_provider
detach_subscriber | daft | (alias: str) -> None | Detaches a subscriber from the current context. | toplevel.md#detach_subscriber
detach_table | daft | (alias: str) -> None | Detaches the table from the current session. | toplevel.md#detach_table
distinct | DataFrame | (*on: ColumnInputType) -> DataFrame | Computes distinct rows, dropping duplicates. | dataframe.md#distinct
distributed_merge_deltalake | daft | (table: Union[str, 'UnityCatalogTable', 'deltalake.DeltaTable'], source: DataFrame, predicate: str, on: str | list[str] | None=None, io_config: IOConfig | None=None, source_alias: str='source', target_alias: str='target', custom_metadata: dict[str, str] | None=None, validate_unique_keys: bool=True, broadcast_join: bool | None=None, materialize_source: bool=True, materialize_join: bool=False, compression: str='snappy') -> DistributedDeltaMergeBuilder | Create a distributed Delta Lake MERGE builder that uses Daft's distributed join. | toplevel.md#distributed_merge_deltalake
dot_product | daft.functions | (left: Expression, right: Expression) -> Expression | Compute the dot product between two embeddings. | functions-etc.md#dot_product
dot_product | Expression | (other: Expression) -> Expression | Compute the dot product between two embeddings. | expressions.md#dot_product
download | daft.functions | (expr: Expression, max_connections: int=32, on_error: Literal['raise', 'null']='raise', io_config: IOConfig | None=None) -> Expression | Treats each string as a URL, and downloads the bytes contents as a bytes column. | functions-media.md#download
download | Expression | (max_connections: int=32, on_error: Literal['raise', 'null']='raise', io_config: IOConfig | None=None) -> Expression | Treats each string as a URL, and downloads the bytes contents as a bytes column. | expressions.md#download
drop_deltalake | DataFrame | (table: Union[str, pathlib.Path, 'deltalake.DeltaTable', 'UnityCatalogTable'], io_config: IOConfig | None=None) -> None | Delete a Delta Lake table completely from the filesystem. | dataframe.md#drop_deltalake
drop_duplicates | DataFrame | (*subset: ColumnInputType) -> DataFrame | Computes distinct rows, dropping duplicates. | dataframe.md#drop_duplicates
drop_namespace | daft | (identifier: Identifier | str) -> None | Drops the namespace in the current session's active catalog. | toplevel.md#drop_namespace
drop_nan | DataFrame | (*cols: ColumnInputType) -> DataFrame | Drops rows that contains NaNs. | dataframe.md#drop_nan
drop_null | DataFrame | (*cols: ColumnInputType) -> DataFrame | Drops rows that contains NaNs or NULLs. | dataframe.md#drop_null
drop_parquet | DataFrame | (table: Union[str, pathlib.Path], io_config: IOConfig | None=None) -> None | Delete a Parquet table path from the filesystem. | dataframe.md#drop_parquet
drop_table | daft | (identifier: Identifier | str) -> None | Drops the table in the current session's active catalog. | toplevel.md#drop_table
e | daft.functions | () -> Expression | Returns Euler's number (e = 2.71828...). | functions-numeric.md#e
element | daft | () -> Expression | Creates an expression referring to an elementwise list operation. | toplevel.md#element
embed_image | daft.functions | (image: Expression, *, provider: str | Provider | None=None, model: str | None=None, **options: Unpack[EmbedImageOptions]) -> Expression | Returns an expression that embeds images using the specified image model and provider. | functions-ai.md#embed_image
embed_text | daft.functions | (text: Expression, *, provider: str | Provider | None=None, model: str | None=None, dimensions: int | None=None, **options: Unpack[EmbedTextOptions]) -> Expression | Returns an expression that embeds text using the specified embedding model and provider. | functions-ai.md#embed_text
encode | daft.functions | (expr: Expression, charset: ENCODING_CHARSET) -> Expression | Encode binary or string values using the specified character set. | functions-etc.md#encode
encode | Expression | (charset: ENCODING_CHARSET) -> Expression | Encode binary or string values using the specified character set. | expressions.md#encode
encode_image | daft.functions | (image: Expression, image_format: str | ImageFormat) -> Expression | Encode an image column as the provided image file format, returning a binary column of encoded bytes. | functions-media.md#encode_image
encode_image | Expression | (image_format: builtins.str | ImageFormat) -> Expression | Encode an image column as the provided image file format, returning a binary column of encoded bytes. | expressions.md#encode_image
endswith | daft.functions | (expr: Expression, suffix: str | Expression) -> Expression | Checks whether each string ends with the given suffix in a string column. | functions-str.md#endswith
endswith | Expression | (suffix: builtins.str | Expression) -> Expression | Checks whether each string ends with the given pattern in a string column. | expressions.md#endswith
eq_null_safe | daft.functions | (left: Expression, right: Expression) -> Expression | Performs a null-safe equality comparison between two expressions. | functions-misc.md#eq_null_safe
eq_null_safe | Expression | (other: Expression | Any) -> Expression | Performs a null-safe equality comparison between two expressions. | expressions.md#eq_null_safe
euclidean_distance | daft.functions | (left: Expression, right: Expression) -> Expression | Compute the Euclidean distance between two embeddings. | functions-etc.md#euclidean_distance
euclidean_distance | Expression | (other: Expression) -> Expression | Compute the Euclidean distance between two embeddings. | expressions.md#euclidean_distance
except_all | DataFrame | (other: 'DataFrame') -> DataFrame | Returns the set difference of two DataFrames, considering duplicates. | dataframe.md#except_all
except_distinct | DataFrame | (other: 'DataFrame') -> DataFrame | Returns the set difference of two DataFrames. | dataframe.md#except_distinct
exclude | DataFrame | (*names: str) -> DataFrame | Drops columns from the current DataFrame by name. | dataframe.md#exclude
execution_config_ctx | daft | (**kwargs: Any) -> Generator[None, None, None] | Context manager that wraps set_execution_config to reset the config to its original setting afternwards. | toplevel.md#execution_config_ctx
exp | daft.functions | (expr: Expression) -> Expression | The e^expr of a numeric expression. | functions-numeric.md#exp
exp | Expression | () -> Expression | The e^self of a numeric expression. | expressions.md#exp
explain | DataFrame | (show_all: bool=False, format: str='ascii', simple: bool=False, file: io.IOBase | None=None) -> Any | Prints the (logical and physical) plans that will be executed to produce this DataFrame. | dataframe.md#explain
explode | daft.functions | (list_expr: Expression, ignore_empty_and_null: bool=False) -> Expression | Explode a list expression. | functions-list.md#explode
explode | DataFrame | (*columns: ColumnInputType, index_column: ColumnInputType | None=None, ignore_empty_and_null: bool=False) -> DataFrame | Explodes a List column, where every element in each row's List becomes its own row, and all other columns in the DataFrame are duplicated across rows. | dataframe.md#explode
explode | Expression | (ignore_empty_and_null: bool=False) -> Expression | Explode a list expression. | expressions.md#explode
expm1 | daft.functions | (expr: Expression) -> Expression | The e^expr - 1 of a numeric expression. | functions-numeric.md#expm1
expm1 | Expression | () -> Expression | The e^self - 1 of a numeric expression. | expressions.md#expm1
Expression | daft | () -> None |  | toplevel.md#expression
extract_day_uuid7 | daft.functions | (expr: Expression) -> Expression | Partitioning Transform that extracts the number of days since epoch (1970-01-01) from a UUIDv7. | functions-etc.md#extract_day_uuid7
extract_hour_uuid7 | daft.functions | (expr: Expression) -> Expression | Partitioning Transform that extracts the number of hours since epoch (1970-01-01) from a UUIDv7. | functions-etc.md#extract_hour_uuid7
extract_minute_uuid7 | daft.functions | (expr: Expression) -> Expression | Partitioning Transform that extracts the number of minutes since epoch (1970-01-01) from a UUIDv7. | functions-etc.md#extract_minute_uuid7
extract_month_uuid7 | daft.functions | (expr: Expression) -> Expression | Partitioning Transform that extracts the number of calendar months since 1970-01 from a UUIDv7. | functions-etc.md#extract_month_uuid7
factorial | daft.functions | (expr: Expression) -> Expression | Returns the factorial of a non-negative integer. | functions-numeric.md#factorial
File | daft | (url: str, io_config: IOConfig | None=None, media_type: MediaType=MediaType.unknown(), position: int | None=None, size: int | None=None, offset: int | None=None, length: int | None=None) -> None | A file-like object for working with file contents in Daft. | toplevel.md#file
file | daft.functions | (url: Expression, io_config: IOConfig | None=None) -> Expression | Converts a string containing a file reference to a `daft.File` reference. | functions-media.md#file
file_exists | daft.functions | (file: Expression) -> Expression | Returns whether the file exists. | functions-media.md#file_exists
file_exists | Expression | () -> Expression | Checks whether a file exists. | expressions.md#file_exists
file_path | daft.functions | (file: Expression) -> Expression | Returns the path (URL) of the file as a string. | functions-media.md#file_path
file_path | Expression | () -> Expression | Gets the path (URL) of a file as a string. | expressions.md#file_path
file_size | daft.functions | (file: Expression) -> Expression | Returns the size of the file in bytes. | functions-media.md#file_size
file_size | Expression | () -> Expression | Gets the size of a file in bytes. | expressions.md#file_size
fill_nan | daft.functions | (expr: Expression, fill_value: Expression) -> Expression | Fills NaN values in the Expression with the provided fill_value. | functions-numeric.md#fill_nan
fill_nan | Expression | (fill_value: Expression) -> Expression | Fills NaN values in the Expression with the provided fill_value. | expressions.md#fill_nan
fill_null | daft.functions | (expr: Expression, fill_value: Expression) -> Expression | Fills null values in the Expression with the provided fill_value. | functions-misc.md#fill_null
fill_null | Expression | (fill_value: Expression | Any) -> Expression | Fills null values in the Expression with the provided fill_value. | expressions.md#fill_null
filter | DataFrame | (predicate: Expression | str) -> DataFrame | Filters rows via a predicate expression, similar to SQL ``WHERE``. | dataframe.md#filter
find | daft.functions | (expr: Expression, substr: str | Expression) -> Expression | Returns the index of the first occurrence of the substring in each string. | functions-str.md#find
find | Expression | (substr: builtins.str | Expression) -> Expression | Returns the index of the first occurrence of the substring in each string. | expressions.md#find
first_value | daft.functions | (expr: Expression, ignore_nulls: bool=False) -> Expression | Returns the first value in the window frame. | functions-window.md#first_value
first_value | Expression | (ignore_nulls: bool=False) -> Expression | Returns the first value in the window frame. | expressions.md#first_value
floor | daft.functions | (expr: Expression) -> Expression | The floor of a numeric expression. | functions-numeric.md#floor
floor | Expression | () -> Expression | The floor of a numeric expression. | expressions.md#floor
format | daft.functions | (f_string: str, *args: Expression | str) -> Expression | Format a string using the given arguments. | functions-str.md#format
from_arrow | daft | (data: Union['pa.Table', list['pa.Table'], Iterable['pa.Table'], ArrowStreamExportable]) -> DataFrame | Creates a DataFrame from Arrow data. | toplevel.md#from_arrow
from_dask_dataframe | daft | (ddf: 'dask.DataFrame') -> DataFrame | Creates a Daft DataFrame from a Dask DataFrame. | toplevel.md#from_dask_dataframe
from_files | daft | (path: str | list[str], io_config: IOConfig | None=None) -> DataFrame | Creates a DataFrame of `daft.File` references from a glob path. | toplevel.md#from_files
from_glob_path | daft | (path: str | list[str], io_config: IOConfig | None=None) -> DataFrame | Creates a DataFrame of file paths and other metadata from a glob path. | toplevel.md#from_glob_path
from_pandas | daft | (data: Union['pd.DataFrame', list['pd.DataFrame']]) -> DataFrame | Creates a Daft DataFrame from a pandas DataFrame. | toplevel.md#from_pandas
from_pydict | daft | (data: dict[str, InputListType]) -> DataFrame | Creates a DataFrame from a Python dictionary. | toplevel.md#from_pydict
from_pylist | daft | (data: list[dict[str, Any]]) -> DataFrame | Creates a DataFrame from a list of dictionaries. | toplevel.md#from_pylist
from_ray_dataset | daft | (ds: 'RayDataset') -> DataFrame | Creates a DataFrame from a Ray Dataset. | toplevel.md#from_ray_dataset
from_unixtime | daft.functions | (expr: Expression, format: str | None=None) -> Expression | Converts a Unix timestamp (seconds) to a formatted string. | functions-datetime.md#from_unixtime
from_utc_timestamp | daft.functions | (expr: Expression, timezone: str) -> Expression | Interprets a UTC timestamp and returns the wall-clock time in the given timezone. | functions-datetime.md#from_utc_timestamp
func | daft | (object) |  | toplevel.md#func
functions | daft | (submodule) |  | toplevel.md#functions
GCSConfig | daft.io | (project_id: str | None=None, credentials: str | None=None, token: str | None=None, anonymous: bool | None=None, max_connections: int | None=None, retry_initial_backoff_ms: int | None=None, connect_timeout_ms: int | None=None, read_timeout_ms: int | None=None, num_tries: int | None=None) | I/O configuration for accessing Google Cloud Storage. | io.md#gcsconfig
get | daft.functions | (expr: Expression, key: int | str | Expression, default: Any=None) -> Expression | Get an index from a list expression or a field from a struct expression. | functions-misc.md#get
get | Expression | (index: int | builtins.str | Expression, default: Any=None) -> Expression | Get an index from a list expression or a field from a struct expression. | expressions.md#get
get_aggregate_function | daft | (name: str, *args: Expression) -> Expression | Returns the aggregate function from the current session or raises an exception if it does not exist. | toplevel.md#get_aggregate_function
get_catalog | daft | (identifier: str) -> Catalog | Returns the catalog from the current session or raises an exception if it does not exist. | toplevel.md#get_catalog
get_context | daft | () -> DaftContext | Returns the global singleton daft context. | toplevel.md#get_context
get_function | daft | (name: str, *args: Expression) -> Expression | Returns the function from the current session or raises an exception if it does not exist. | toplevel.md#get_function
get_loaded_extension_paths | daft | () -> list[str] |  | toplevel.md#get_loaded_extension_paths
get_or_create_runner | daft | () -> Runner[PartitionT] | Get or create the current runner instance. | toplevel.md#get_or_create_runner
get_or_infer_runner_type | daft | () -> str | Get or infer the runner type. | toplevel.md#get_or_infer_runner_type
get_provider | daft | (identifier: str) -> Provider | Returns the provider from the current session or raises an exception if it does not exist. | toplevel.md#get_provider
get_table | daft | (identifier: Identifier | str) -> Table | Returns the table from the current session or raises an exception if it does not exist. | toplevel.md#get_table
GooseFSConfig | daft.io | (root: str | None=None, master_addr: str | None=None, block_size: int | None=None, chunk_size: int | None=None, write_type: str | None=None, auth_type: str | None=None, auth_username: str | None=None, auth_password: str | None=None, anonymous: bool | None=None, max_retries: int | None=None, retry_timeout_ms: int | None=None, connect_timeout_ms: int | None=None, read_timeout_ms: int | None=None, max_concurrent_requests: int | None=None, max_connections: int | None=None) | I/O configuration for accessing GooseFS (distributed caching file system) via native gRPC. | io.md#goosefsconfig
GravitinoConfig | daft.io | (endpoint: str | None, metalake_name: str | None, auth_type: str | None, username: str | None, password: str | None, token: str | None) | I/O configuration for Gravitino filesets. | io.md#gravitinoconfig
great_circle_distance | daft.functions | (lat1: Expression, lon1: Expression, lat2: Expression, lon2: Expression) -> Expression | Compute the great circle distance between two points on the Earth. | functions-spatial.md#great_circle_distance
groupby | DataFrame | (*group_by: ManyColumnsInputType) -> GroupedDataFrame | Performs a GroupBy on the DataFrame for aggregation. | dataframe.md#groupby
guess_mime_type | daft.functions | (bytes_expr: Expression) -> Expression | Guess the MIME type of binary data by inspecting magic bytes. | functions-media.md#guess_mime_type
hamming_distance | daft.functions | (left: Expression, right: Expression) -> Expression | Compute the Hamming distance (number of differing bits) between two hash fingerprints. | functions-etc.md#hamming_distance
hamming_distance | Expression | (other: Expression) -> Expression | Compute the bitwise Hamming distance between two hash fingerprints. | expressions.md#hamming_distance
hamming_distance_str | daft.functions | (left: Expression, right: Expression) -> Expression | Compute the character-level Hamming distance between two strings. | functions-str.md#hamming_distance_str
hamming_distance_str | Expression | (other: Expression) -> Expression | Compute the character-level Hamming distance between two strings. | expressions.md#hamming_distance_str
has_catalog | daft | (identifier: str) -> bool | Returns true if a catalog with the given identifier exists in the current session. | toplevel.md#has_catalog
has_namespace | daft | (identifier: Identifier | str) -> bool | Returns true if a namespace with the given identifier exists in the current session. | toplevel.md#has_namespace
has_provider | daft | (identifier: str) -> bool | Returns true if a provider with the given identifier exists in the current session. | toplevel.md#has_provider
has_table | daft | (identifier: Identifier | str) -> bool | Returns true if a table with the given identifier exists in the current session. | toplevel.md#has_table
hash | daft.functions | (*exprs: Expression, seed: Any | None=None, hash_function: Literal['xxhash', 'xxhash32', 'xxhash64', 'xxhash3_64', 'murmurhash3', 'sha1'] | None='xxhash') -> Expression | Hashes the values in the Expression. | functions-misc.md#hash
hash | Expression | (seed: Any | None=None, hash_function: Literal['xxhash', 'xxhash32', 'xxhash64', 'xxhash3_64', 'murmurhash3', 'sha1'] | None='xxhash') -> Expression | Hashes the values in the Expression. | expressions.md#hash
hdf5_attrs | daft.functions | (file_expr: Expression, h5path: str='/') -> Expression | Read HDF5 attributes for a group or dataset. | functions-media.md#hdf5_attrs
hdf5_attrs | Expression | (h5path: str='/') -> Expression | Read HDF5 attributes for a group or dataset. | expressions.md#hdf5_attrs
hdf5_file | daft.functions | (url: Expression, verify: bool=False, io_config: IOConfig | None=None) -> Expression | Converts a string containing a file reference to a `daft.Hdf5File` reference. | functions-media.md#hdf5_file
hdf5_keys | daft.functions | (file_expr: Expression, group: str='/') -> Expression | List member names directly under an HDF5 group. | functions-media.md#hdf5_keys
hdf5_keys | Expression | (group: str='/') -> Expression | List member names directly under an HDF5 group. | expressions.md#hdf5_keys
hdf5_metadata | daft.functions | (file_expr: Expression, group: str='/') -> Expression | Collect metadata for groups and datasets under an HDF5 group. | functions-media.md#hdf5_metadata
hdf5_metadata | Expression | (group: str='/') -> Expression | Collect metadata for groups and datasets under an HDF5 group. | expressions.md#hdf5_metadata
Hdf5File | daft | (url: str, io_config: IOConfig | None=None) -> None | Represents an HDF5 file backed by Daft file IO. | toplevel.md#hdf5file
history_deltalake | daft | (table: Union[str, 'UnityCatalogTable', 'deltalake.DeltaTable'], limit: int | None=None, io_config: IOConfig | None=None, parse_operation_metrics: bool=True) -> list[dict[str, Any]] | Return commit history for a Delta Lake table. | toplevel.md#history_deltalake
hour | daft.functions | (expr: Expression) -> Expression | Retrieves the hour for a datetime column. | functions-datetime.md#hour
hour | Expression | () -> Expression | Retrieves the hour for a datetime column. | expressions.md#hour
HTTPConfig | daft.io | (bearer_token: str | None=None, retry_initial_backoff_ms: int | None=None, connect_timeout_ms: int | None=None, read_timeout_ms: int | None=None, num_tries: int | None=None) | I/O configuration for accessing HTTP systems. | io.md#httpconfig
HuggingFaceConfig | daft.io | (token: str | None=None, anonymous: bool | None=None, use_xet: bool | None=None, use_content_defined_chunking: bool | None=None, row_group_size: int | None=None, target_filesize: int | None=None, max_operations_per_commit: int | None=None) | I/O configuration for accessing Hugging Face datasets. | io.md#huggingfaceconfig
hypot | daft.functions | (a: Expression, b: Expression) -> Expression | Returns sqrt(a^2 + b^2), the Euclidean norm. | functions-numeric.md#hypot
IdempotentCommit | daft | () | Per-call idempotent commit configuration for sink writes. | toplevel.md#idempotentcommit
Identifier | daft | (*parts: str) | A reference (path) to a catalog object. | toplevel.md#identifier
ilike | daft.functions | (expr: Expression, pattern: str | Expression) -> Expression | Checks whether each string matches the given SQL ILIKE pattern, case insensitive. | functions-str.md#ilike
ilike | Expression | (pattern: builtins.str | Expression) -> Expression | Checks whether each string matches the given SQL ILIKE pattern, case insensitive. | expressions.md#ilike
image_attribute | daft.functions | (image: Expression, name: Literal['width', 'height', 'channel', 'mode'] | ImageProperty) -> Expression | Get a property of the image, such as 'width', 'height', 'channel', or 'mode'. | functions-media.md#image_attribute
image_attribute | Expression | (name: Literal['width', 'height', 'channel', 'mode'] | ImageProperty) -> Expression | Get a property of the image, such as 'width', 'height', 'channel', or 'mode'. | expressions.md#image_attribute
image_channel | daft.functions | (image: Expression) -> Expression | Gets the number of channels in an image. | functions-media.md#image_channel
image_channel | Expression | () -> Expression | Gets the number of channels in an image. | expressions.md#image_channel
image_file | daft.functions | (url: Expression, verify: bool=False, io_config: IOConfig | None=None) -> Expression | Converts a string containing a file reference to a `daft.ImageFile` reference. | functions-media.md#image_file
image_file_metadata | daft.functions | (file_expr: Expression) -> Expression | Extract image metadata (width, height, format, mode) from a File column. | functions-media.md#image_file_metadata
image_file_metadata | Expression | () -> Expression | Gets metadata for an image file (width, height, format, mode). | expressions.md#image_file_metadata
image_hash | daft.functions | (image: Expression, *, method: Literal['phash', 'phash_simple', 'dhash', 'dhash_vertical', 'ahash', 'whash', 'crop_resistant', 'colorhash']='phash', hash_size: int=8, binbits: int=3, segments: int=3) -> Expression | Compute a perceptual hash of an image column for near-duplicate detection. | functions-media.md#image_hash
image_hash | Expression | (*, method: Literal['phash', 'phash_simple', 'dhash', 'dhash_vertical', 'ahash', 'whash', 'crop_resistant', 'colorhash']='phash', hash_size: int=8, binbits: int=3) -> Expression | Computes a perceptual hash of an image. | expressions.md#image_hash
image_height | daft.functions | (image: Expression) -> Expression | Gets the height of an image in pixels. | functions-media.md#image_height
image_height | Expression | () -> Expression | Gets the height of an image in pixels. | expressions.md#image_height
image_mode | daft.functions | (image: Expression) -> Expression | Gets the mode of an image. | functions-media.md#image_mode
image_mode | Expression | () -> Expression | Gets the mode of an image as a string. | expressions.md#image_mode
image_to_tensor | daft.functions | (image: Expression) -> Expression | Convert an image expression to a tensor, inferring dtype and shape. | functions-media.md#image_to_tensor
image_to_tensor | Expression | () -> Expression | Convert an image expression to a tensor, inferring dtype and shape. | expressions.md#image_to_tensor
image_width | daft.functions | (image: Expression) -> Expression | Gets the width of an image in pixels. | functions-media.md#image_width
image_width | Expression | () -> Expression | Gets the width of an image in pixels. | expressions.md#image_width
ImageFile | daft | (url: str, io_config: IOConfig | None=None) -> None | An image-specific file interface that provides image operations. | toplevel.md#imagefile
ImageFormat | daft | () | Supported image formats for Daft's image I/O. | toplevel.md#imageformat
ImageMode | daft | () | Supported image modes for Daft's image type. | toplevel.md#imagemode
ImageProperty | daft | () | Supported image properties for Daft's image type. | toplevel.md#imageproperty
intersect | DataFrame | (other: 'DataFrame') -> DataFrame | Returns the intersection of two DataFrames. | dataframe.md#intersect
intersect_all | DataFrame | (other: 'DataFrame') -> DataFrame | Returns the intersection of two DataFrames, including duplicates. | dataframe.md#intersect_all
interval | daft | (years: int | None=None, months: int | None=None, days: int | None=None, hours: int | None=None, minutes: int | None=None, seconds: int | None=None, millis: int | None=None, nanos: int | None=None) -> Expression | Creates an Expression representing an interval. | toplevel.md#interval
into_batches | DataFrame | (batch_size: int) -> DataFrame | Splits or coalesces DataFrame to partitions of size ``batch_size``. | dataframe.md#into_batches
into_partitions | DataFrame | (num: int) -> DataFrame | Splits or coalesces DataFrame to ``num`` partitions. | dataframe.md#into_partitions
io | daft | (submodule) |  | toplevel.md#io
IOConfig | daft | (s3: S3Config | None=None, azure: AzureConfig | None=None, gcs: GCSConfig | None=None, http: HTTPConfig | None=None, unity: UnityConfig | None=None, hf: HuggingFaceConfig | None=None, disable_suffix_range: bool | None=None, tos: TosConfig | None=None, gravitino: GravitinoConfig | None=None, cos: CosConfig | None=None, goosefs: GooseFSConfig | None=None, opendal_backends: dict[str, dict[str, str]] | None=None, protocol_aliases: dict[str, str] | None=None) | Configuration for the native I/O layer, e.g. credentials for accessing cloud storage systems. | toplevel.md#ioconfig
is_column | Expression | () -> bool |  | expressions.md#is_column
is_in | daft.functions | (expr: Expression, other: Iterable[Any] | Expression) -> Expression | Checks if values in the Expression are in the provided iterable. | functions-misc.md#is_in
is_in | Expression | (other: Iterable[Any] | Expression) -> Expression | Checks if values in the Expression are in the provided iterable. | expressions.md#is_in
is_inf | daft.functions | (expr: Expression) -> Expression | Checks if values in the Expression are Infinity. | functions-numeric.md#is_inf
is_inf | Expression | () -> Expression | Checks if values in the Expression are Infinity. | expressions.md#is_inf
is_literal | Expression | () -> bool |  | expressions.md#is_literal
is_nan | daft.functions | (expr: Expression) -> Expression | Checks if values are NaN (a special float value indicating not-a-number). | functions-numeric.md#is_nan
is_nan | Expression | () -> Expression | Checks if values are NaN (a special float value indicating not-a-number). | expressions.md#is_nan
is_null | daft.functions | (expr: Expression) -> Expression | Checks if values in the Expression are Null (a special value indicating missing data). | functions-misc.md#is_null
is_null | Expression | () -> Expression | Checks if values in the Expression are Null (a special value indicating missing data). | expressions.md#is_null
iter_partitions | DataFrame | (results_buffer_size: int | None | Literal['num_cpus']='num_cpus') -> Iterator[Union[MicroPartition, 'ray.ObjectRef']] | Begin executing this dataframe and return an iterator over the partitions. | dataframe.md#iter_partitions
iter_rows | DataFrame | (results_buffer_size: int | None | Literal['num_cpus']='num_cpus', column_format: Literal['python', 'arrow']='python') -> Iterator[dict[str, Any]] | Return an iterator of rows for this dataframe. | dataframe.md#iter_rows
jaccard_similarity | daft.functions | (left: Expression, right: Expression) -> Expression | Compute the Jaccard similarity between two embeddings. | functions-etc.md#jaccard_similarity
jaccard_similarity | Expression | (other: Expression) -> Expression | Compute the Jaccard similarity between two embeddings. | expressions.md#jaccard_similarity
jaro_similarity | daft.functions | (left: Expression, right: Expression) -> Expression | Compute the Jaro similarity between two strings. | functions-str.md#jaro_similarity
jaro_similarity | Expression | (other: Expression) -> Expression | Compute the Jaro similarity between two strings. | expressions.md#jaro_similarity
jaro_winkler_similarity | daft.functions | (left: Expression, right: Expression) -> Expression | Compute the Jaro-Winkler similarity between two strings. | functions-str.md#jaro_winkler_similarity
jaro_winkler_similarity | Expression | (other: Expression) -> Expression | Compute the Jaro-Winkler similarity between two strings. | expressions.md#jaro_winkler_similarity
join | DataFrame | (other: 'DataFrame', on: list[ColumnInputType] | ColumnInputType | None=None, left_on: list[ColumnInputType] | ColumnInputType | None=None, right_on: list[ColumnInputType] | ColumnInputType | None=None, how: Literal['inner', 'left', 'right', 'outer', 'anti', 'semi', 'cross']='inner', strategy: Literal['hash', 'sort_merge', 'broadcast'] | None=None, prefix: str | None=None, suffix: str | None=None) -> DataFrame | Column-wise join of the current DataFrame with an ``other`` DataFrame, similar to a SQL ``JOIN``. | dataframe.md#join
join_asof | DataFrame | (other: 'DataFrame', *, on: ColumnInputType | None=None, left_on: ColumnInputType | None=None, right_on: ColumnInputType | None=None, by: list[ColumnInputType] | ColumnInputType | None=None, left_by: list[ColumnInputType] | ColumnInputType | None=None, right_by: list[ColumnInputType] | ColumnInputType | None=None, strategy: Literal['backward', 'forward', 'nearest']='backward', prefix: str | None=None, suffix: str | None=None, _assume_sorted_and_aligned: bool=False) -> DataFrame | Point-in-time (asof) join: each left row matches the nearest right row according to the chosen strategy. | dataframe.md#join_asof
jq | daft.functions | (expr: Expression, filter: str) -> Expression | Applies a [jq](https://jqlang.github.io/jq/manual/) filter to a string, returning the results as a string. | functions-str.md#jq
jq | Expression | (filter: builtins.str) -> Expression | Applies a [jq](https://jqlang.github.io/jq/manual/) filter to the expression (string), returning the results as a string. | expressions.md#jq
json_array_length | daft.functions | (expr: Expression) -> Expression | Returns the number of elements in the outermost JSON array. | functions-str.md#json_array_length
json_object_keys | daft.functions | (expr: Expression) -> Expression | Returns the top-level keys of a JSON object as a list of strings. | functions-str.md#json_object_keys
json_tuple | daft.functions | (expr: Expression, *fields: str) -> Expression | Extracts the values for the given top-level keys from a JSON object string. | functions-str.md#json_tuple
KeyFilteringSettings | daft | (num_workers: int | None=None, cpus_per_worker: float | None=None, keys_load_batch_size: int | None=None, max_concurrency_per_worker: int | None=None, filter_batch_size: int | None=None) -> None |  | toplevel.md#keyfilteringsettings
lag | daft.functions | (expr: Expression, offset: int=1, default: Expression | None=None) -> Expression | Get the value from a previous row within a window partition. | functions-window.md#lag
lag | Expression | (offset: int=1, default: Any | None=None) -> Expression | Get the value from a previous row within a window partition. | expressions.md#lag
last_day | daft.functions | (expr: Expression) -> Expression | Returns the last day of the month for the given date or timestamp. | functions-datetime.md#last_day
last_value | daft.functions | (expr: Expression, ignore_nulls: bool=False) -> Expression | Returns the last value in the window frame. | functions-window.md#last_value
last_value | Expression | (ignore_nulls: bool=False) -> Expression | Returns the last value in the window frame. | expressions.md#last_value
lead | daft.functions | (expr: Expression, offset: int=1, default: Expression | None=None) -> Expression | Get the value from a future row within a window partition. | functions-window.md#lead
lead | Expression | (offset: int=1, default: Any | None=None) -> Expression | Get the value from a future row within a window partition. | expressions.md#lead
left | daft.functions | (expr: Expression, nchars: int | Expression) -> Expression | Gets the n (from nchars) left-most characters of each string. | functions-str.md#left
left | Expression | (nchars: int | Expression) -> Expression | Gets the n (from nchars) left-most characters of each string. | expressions.md#left
length | daft.functions | (expr: Expression) -> Expression | Retrieves the length of the given expression. | functions-misc.md#length
length | Expression | () -> Expression | Retrieves the length of the given expression. | expressions.md#length
length_bytes | daft.functions | (expr: Expression) -> Expression | Retrieves the length for a UTF-8 string column in bytes. | functions-str.md#length_bytes
length_bytes | Expression | () -> Expression | Retrieves the length for a UTF-8 string column in bytes. | expressions.md#length_bytes
levenshtein_distance | daft.functions | (left: Expression, right: Expression) -> Expression | Compute the Levenshtein edit distance between two strings. | functions-str.md#levenshtein_distance
levenshtein_distance | Expression | (other: Expression) -> Expression | Compute the Levenshtein edit distance between two strings. | expressions.md#levenshtein_distance
like | daft.functions | (expr: Expression, pattern: str | Expression) -> Expression | Checks whether each string matches the given SQL LIKE pattern, case sensitive. | functions-str.md#like
like | Expression | (pattern: builtins.str | Expression) -> Expression | Checks whether each string matches the given SQL LIKE pattern, case sensitive. | expressions.md#like
limit | DataFrame | (num: int) -> DataFrame | Limits the rows in the DataFrame to the first ``N`` rows, similar to a SQL ``LIMIT``. | dataframe.md#limit
list_agg | daft.functions | (expr: Expression) -> Expression | Aggregates the values in the expression into a list. | functions-agg.md#list_agg
list_agg | Expression | () -> Expression | Aggregates the values in the expression into a list. | expressions.md#list_agg
list_agg_distinct | daft.functions | (expr: Expression) -> Expression | Aggregates the values in the expression into a list of distinct values (ignoring nulls). | functions-agg.md#list_agg_distinct
list_agg_distinct | Expression | () -> Expression | Aggregates the values in the expression into a list of distinct values (ignoring nulls). | expressions.md#list_agg_distinct
list_append | daft.functions | (list_expr: Expression, other: Expression) -> Expression | Appends a value to each list in the column. | functions-list.md#list_append
list_append | Expression | (other: Expression) -> Expression | Appends a value to each list in the column. | expressions.md#list_append
list_bool_and | daft.functions | (list_expr: Expression) -> Expression | Calculates the boolean AND of all values in a list. | functions-list.md#list_bool_and
list_bool_and | Expression | () -> Expression | Calculates the boolean AND of all values in a list. | expressions.md#list_bool_and
list_bool_or | daft.functions | (list_expr: Expression) -> Expression | Calculates the boolean OR of all values in a list. | functions-list.md#list_bool_or
list_bool_or | Expression | () -> Expression | Calculates the boolean OR of all values in a list. | expressions.md#list_bool_or
list_catalogs | daft | (pattern: str | None=None) -> list[str] | Returns a list of available catalogs in the current session. | toplevel.md#list_catalogs
list_contains | daft.functions | (list_expr: Expression, item: Expression) -> Expression | Checks if each list contains the specified item. | functions-list.md#list_contains
list_contains | Expression | (item: Expression) -> Expression | Checks if each list contains the specified item. | expressions.md#list_contains
list_count | daft.functions | (list_expr: Expression, mode: Literal['all', 'valid', 'null'] | CountMode=CountMode.Valid) -> Expression | Counts the number of elements in each list. | functions-list.md#list_count
list_count | Expression | (mode: Literal['all', 'valid', 'null'] | CountMode=CountMode.Valid) -> Expression | Counts the number of elements in each list. | expressions.md#list_count
list_distinct | daft.functions | (list_expr: Expression) -> Expression | Returns a list of unique elements in each list, preserving order of first occurrence and ignoring nulls. | functions-list.md#list_distinct
list_distinct | Expression | () -> Expression | Returns a list of unique elements in each list, preserving order of first occurrence and ignoring nulls. | expressions.md#list_distinct
list_filter | daft.functions | (list_expr: Expression, predicate: Expression) -> Expression | Filters elements in a list using a boolean predicate expression. | functions-list.md#list_filter
list_filter | Expression | (predicate: Expression) -> Expression | Filters elements in the list using a boolean predicate over `daft.element()`. | expressions.md#list_filter
list_flatten | daft.functions | (list_expr: Expression) -> Expression | Flattens one level of nesting in each list. | functions-list.md#list_flatten
list_flatten | Expression | () -> Expression | Flattens one level of nesting in each list. | expressions.md#list_flatten
list_join | daft.functions | (list_expr: Expression, delimiter: str | Expression) -> Expression | Joins every element of a list using the specified string delimiter. | functions-list.md#list_join
list_join | Expression | (delimiter: builtins.str | Expression) -> Expression | Joins every element of a list using the specified string delimiter. | expressions.md#list_join
list_map | daft.functions | (list_expr: Expression, mapper: Expression) -> Expression | Evaluates an expression on all elements in the list. | functions-list.md#list_map
list_map | Expression | (mapper: Expression) -> Expression | Evaluates an expression on all elements in the list. | expressions.md#list_map
list_max | daft.functions | (list_expr: Expression) -> Expression | Calculates the maximum of each list. | functions-list.md#list_max
list_max | Expression | () -> Expression | Calculates the maximum of each list. | expressions.md#list_max
list_mean | daft.functions | (list_expr: Expression) -> Expression | Calculates the mean of each list. | functions-list.md#list_mean
list_mean | Expression | () -> Expression | Calculates the mean of each list. | expressions.md#list_mean
list_min | daft.functions | (list_expr: Expression) -> Expression | Calculates the minimum of each list. | functions-list.md#list_min
list_min | Expression | () -> Expression | Calculates the minimum of each list. | expressions.md#list_min
list_sort | daft.functions | (list_expr: Expression, desc: bool | Expression | None=None, nulls_first: bool | Expression | None=None) -> Expression | Sorts the inner lists of a list column. | functions-list.md#list_sort
list_sort | Expression | (desc: bool | Expression | None=None, nulls_first: bool | Expression | None=None) -> Expression | Sorts the inner lists of a list column. | expressions.md#list_sort
list_sum | daft.functions | (list_expr: Expression) -> Expression | Sums each list. | functions-list.md#list_sum
list_sum | Expression | () -> Expression | Sums each list. | expressions.md#list_sum
list_tables | daft | (pattern: str | None=None) -> list[Identifier] | Returns a list of available tables in the current session. | toplevel.md#list_tables
lit | daft | (value: object) -> Expression | Creates an Expression representing a column with every value set to the provided value. | toplevel.md#lit
llm_generate | daft.functions | (text: Expression, model: str='facebook/opt-125m', provider: Literal['vllm', 'openai']='vllm', concurrency: int=1, batch_size: int | None=None, num_cpus: int | None=None, num_gpus: int | None=None, **generation_config: dict[str, Any]) -> Expression | A UDF for running LLM inference over an input column of strings. | functions-etc.md#llm_generate
ln | daft.functions | (expr: Expression) -> Expression | The elementwise natural log of a numeric expression. | functions-numeric.md#ln
ln | Expression | () -> Expression | The elementwise natural log of a numeric expression. | expressions.md#ln
load_extension | daft | (extension: str | types.ModuleType | Path) -> None | Load a native extension by module symbol or an explicit file path. | toplevel.md#load_extension
log | daft.functions | (expr: Expression, base: int | float=math.e) -> Expression | The elementwise log with given base, of a numeric expression. | functions-numeric.md#log
log | Expression | (base: int | builtins.float=math.e) -> Expression | The elementwise log with given base, of a numeric expression. | expressions.md#log
log10 | daft.functions | (expr: Expression) -> Expression | The elementwise log base 10 of a numeric expression. | functions-numeric.md#log10
log10 | Expression | () -> Expression | The elementwise log base 10 of a numeric expression. | expressions.md#log10
log1p | daft.functions | (expr: Expression) -> Expression | The ln(expr + 1) of a numeric expression. | functions-numeric.md#log1p
log1p | Expression | () -> Expression | The ln(self + 1) of a numeric expression. | expressions.md#log1p
log2 | daft.functions | (expr: Expression) -> Expression | The elementwise log base 2 of a numeric expression. | functions-numeric.md#log2
log2 | Expression | () -> Expression | The elementwise log base 2 of a numeric expression. | expressions.md#log2
lower | daft.functions | (expr: Expression) -> Expression | Convert UTF-8 string to all lowercase. | functions-str.md#lower
lower | Expression | () -> Expression | Convert UTF-8 string to all lowercase. | expressions.md#lower
lpad | daft.functions | (expr: Expression, length: int | Expression, pad: str | Expression) -> Expression | Left-pads each string by truncating on the right or padding with the character. | functions-str.md#lpad
lpad | Expression | (length: int | Expression, pad: builtins.str | Expression) -> Expression | Left-pads each string by truncating or padding with the character. | expressions.md#lpad
lstrip | daft.functions | (expr: Expression) -> Expression | Strip whitespace from the left side of a UTF-8 string. | functions-str.md#lstrip
lstrip | Expression | () -> Expression | Strip whitespace from the left side of a UTF-8 string. | expressions.md#lstrip
make_date | daft.functions | (year: Expression, month: Expression, day: Expression) -> Expression | Creates a date from year, month, and day integer components. | functions-datetime.md#make_date
make_timestamp | daft.functions | (year: Expression, month: Expression, day: Expression, hour: Expression, minute: Expression, second: Expression, timezone: str | None=None) -> Expression | Creates a timestamp from individual date/time components. | functions-datetime.md#make_timestamp
make_timestamp_ltz | daft.functions | (year: Expression, month: Expression, day: Expression, hour: Expression, minute: Expression, second: Expression, timezone: str | None=None) -> Expression | Creates a UTC timestamp from individual date/time components. | functions-datetime.md#make_timestamp_ltz
map_get | daft.functions | (expr: Expression, key: Expression) -> Expression | Retrieves the value for a key in a map column. | functions-misc.md#map_get
map_get | Expression | (key: Expression) -> Expression | Retrieves the value for a key in a map column. | expressions.md#map_get
map_keys | daft.functions | (expr: Expression) -> Expression | Returns a list of all keys in the map. | functions-misc.md#map_keys
map_keys | Expression | () -> Expression | Returns a list of all keys in the map. | expressions.md#map_keys
max | daft.functions | (expr: Expression) -> Expression | Calculates the maximum of the values in the expression. | functions-agg.md#max
max | DataFrame | (*cols: ColumnInputType) -> DataFrame | Performs a global max on the DataFrame. | dataframe.md#max
max | Expression | () -> Expression | Calculates the maximum value in the expression. | expressions.md#max
md5 | daft.functions | (expr: Expression) -> Expression | Computes the MD5 digest for the input expression. | functions-misc.md#md5
mean | daft.functions | (expr: Expression) -> Expression | Calculates the mean of the values in the expression. | functions-agg.md#mean
mean | DataFrame | (*cols: ColumnInputType) -> DataFrame | Performs a global mean on the DataFrame. | dataframe.md#mean
mean | Expression | () -> Expression | Calculates the mean of the values in the expression. | expressions.md#mean
median | daft.functions | (expr: Expression) -> Expression | Calculates the median of the values in the expression. | functions-agg.md#median
median | Expression | () -> Expression | Calculates the median of the values in the expression. | expressions.md#median
MediaType | daft | () -> None |  | toplevel.md#mediatype
melt | DataFrame | (ids: ManyColumnsInputType, values: ManyColumnsInputType=[], variable_name: str='variable', value_name: str='value') -> DataFrame | Alias for unpivot. | dataframe.md#melt
merge_deltalake | daft | (table: Union[str, 'UnityCatalogTable', 'deltalake.DeltaTable'], source: Union[DataFrame, 'pa.Table'], predicate: str, io_config: IOConfig | None=None, source_alias: str='source', target_alias: str='target', custom_metadata: dict[str, str] | None=None, safe_cast: bool=True, merge_schema: bool=False, writer_properties: 'deltalake.WriterProperties | None'=None, streamed_exec: bool=True, max_spill_size: int | None=None, max_temp_directory_size: int | None=None, post_commithook_properties: 'deltalake.PostCommitHookProperties | None'=None, compression: str | None=None) -> DeltaMergeBuilder | Create a Delta Lake MERGE operation builder for composable merge clauses. | toplevel.md#merge_deltalake
merge_deltalake | DataFrame | (table: Union[str, 'UnityCatalogTable', 'deltalake.DeltaTable'], predicate: str, io_config: IOConfig | None=None, source_alias: str='source', target_alias: str='target', custom_metadata: dict[str, str] | None=None, safe_cast: bool=True, merge_schema: bool=False, writer_properties: 'deltalake.WriterProperties | None'=None, streamed_exec: bool=True, max_spill_size: int | None=None, max_temp_directory_size: int | None=None, post_commithook_properties: 'deltalake.PostCommitHookProperties | None'=None, compression: str | None=None) -> DeltaMergeBuilder | Create a Delta Lake MERGE operation builder using this DataFrame. | dataframe.md#merge_deltalake
method | daft | (object) |  | toplevel.md#method
metrics | daft | (submodule) |  | toplevel.md#metrics
metrics | DataFrame | () -> RecordBatch | None |  | dataframe.md#metrics
microsecond | daft.functions | (expr: Expression) -> Expression | Retrieves the microsecond for a datetime column. | functions-datetime.md#microsecond
microsecond | Expression | () -> Expression | Retrieves the microsecond for a datetime column. | expressions.md#microsecond
millisecond | daft.functions | (expr: Expression) -> Expression | Retrieves the millisecond for a datetime column. | functions-datetime.md#millisecond
millisecond | Expression | () -> Expression | Retrieves the millisecond for a datetime column. | expressions.md#millisecond
min | daft.functions | (expr: Expression) -> Expression | Calculates the minimum of the values in the expression. | functions-agg.md#min
min | DataFrame | (*cols: ColumnInputType) -> DataFrame | Performs a global min on the DataFrame. | dataframe.md#min
min | Expression | () -> Expression | Calculates the minimum value in the expression. | expressions.md#min
minhash | daft.functions | (text: Expression, *, num_hashes: int, ngram_size: int, seed: int=1, hash_function: Literal['murmurhash3', 'xxhash', 'xxhash32', 'xxhash64', 'xxhash3_64', 'sha1']='murmurhash3') -> Expression | Runs the MinHash algorithm on the series. | functions-misc.md#minhash
minhash | Expression | (*, num_hashes: int, ngram_size: int, seed: int=1, hash_function: Literal['murmurhash3', 'xxhash', 'xxhash32', 'xxhash64', 'xxhash3_64', 'sha1']='murmurhash3') -> Expression | Runs the MinHash algorithm on the series. | expressions.md#minhash
minute | daft.functions | (expr: Expression) -> Expression | Retrieves the minute for a datetime column. | functions-datetime.md#minute
minute | Expression | () -> Expression | Retrieves the minute for a datetime column. | expressions.md#minute
monotonically_increasing_id | daft.functions | () -> Expression | Generates a column of monotonically increasing unique ids. | functions-misc.md#monotonically_increasing_id
month | daft.functions | (expr: Expression) -> Expression | Retrieves the month for a datetime column. | functions-datetime.md#month
month | Expression | () -> Expression | Retrieves the month for a datetime column. | expressions.md#month
months_between | daft.functions | (end: Expression, start: Expression) -> Expression | Returns the number of months between two dates or timestamps. | functions-datetime.md#months_between
name | Expression | () -> builtins.str |  | expressions.md#name
named_struct | daft.functions | (*args: str | Expression) -> Expression | Constructs a struct from alternating field name and value pairs. | functions-etc.md#named_struct
nanosecond | daft.functions | (expr: Expression) -> Expression | Retrieves the nanosecond for a datetime column. | functions-datetime.md#nanosecond
nanosecond | Expression | () -> Expression | Retrieves the nanosecond for a datetime column. | expressions.md#nanosecond
negate | daft.functions | (expr: Expression) -> Expression | The negative of a numeric expression. | functions-numeric.md#negate
negate | Expression | () -> Expression | The negative of a numeric expression. | expressions.md#negate
next_day | daft.functions | (expr: Expression, day_of_week: str) -> Expression | Returns the next occurrence of the specified day of the week after the given date. | functions-datetime.md#next_day
normalize | daft.functions | (expr: Expression, *, remove_punct: bool=False, lowercase: bool=False, nfd_unicode: bool=False, white_space: bool=False) -> Expression | Normalizes a string for more useful deduplication. | functions-str.md#normalize
normalize | Expression | (*, remove_punct: bool=False, lowercase: bool=False, nfd_unicode: bool=False, white_space: bool=False) -> Expression | Normalizes a string for more useful deduplication. | expressions.md#normalize
not_nan | daft.functions | (expr: Expression) -> Expression | Checks if values are not NaN (a special float value indicating not-a-number). | functions-numeric.md#not_nan
not_nan | Expression | () -> Expression | Checks if values are not NaN (a special float value indicating not-a-number). | expressions.md#not_nan
not_null | daft.functions | (expr: Expression) -> Expression | Checks if values in the Expression are not Null (a special value indicating missing data). | functions-misc.md#not_null
not_null | Expression | () -> Expression | Checks if values in the Expression are not Null (a special value indicating missing data). | expressions.md#not_null
num_partitions | DataFrame | () -> int | None | Returns the number of partitions that will be used to execute this DataFrame. | dataframe.md#num_partitions
offset | DataFrame | (num: int) -> DataFrame | Returns a new DataFrame by skipping the first ``N`` rows, similar to a SQL ``Offset``. | dataframe.md#offset
open_file | daft | (url: str, mode: Literal['r', 'rt', 'rb']='r', buffering: int=-1, encoding: str | None=None, io_config: IOConfig | None=None) -> io.IOBase | Open a file from a URL, potentially from a remote filesystem using Daft's IO backend. | toplevel.md#open_file
over | daft.functions | (expr: Expression, window: Window) -> Expression | Apply the expression as a window function. | functions-window.md#over
over | Expression | (window: Window) -> Expression | Apply the expression as a window function. | expressions.md#over
parse_url | daft.functions | (expr: Expression) -> Expression | Parse string URLs and extract URL components. | functions-media.md#parse_url
parse_url | Expression | () -> Expression | Parse string URLs and extract URL components. | expressions.md#parse_url
partition_days | daft.functions | (expr: Expression) -> Expression | Partitioning Transform that returns the number of days since epoch (1970-01-01). | functions-etc.md#partition_days
partition_days | Expression | () -> Expression | Partitioning Transform that returns the number of days since epoch (1970-01-01). | expressions.md#partition_days
partition_hours | daft.functions | (expr: Expression) -> Expression | Partitioning Transform that returns the number of hours since epoch (1970-01-01). | functions-etc.md#partition_hours
partition_hours | Expression | () -> Expression | Partitioning Transform that returns the number of hours since epoch (1970-01-01). | expressions.md#partition_hours
partition_iceberg_bucket | daft.functions | (expr: Expression, n: int) -> Expression | Partitioning Transform that returns the Hash Bucket following the Iceberg Specification of murmur3_32_x86. | functions-etc.md#partition_iceberg_bucket
partition_iceberg_bucket | Expression | (n: int) -> Expression | Partitioning Transform that returns the Hash Bucket following the Iceberg Specification of murmur3_32_x86. | expressions.md#partition_iceberg_bucket
partition_iceberg_truncate | daft.functions | (expr: Expression, w: int) -> Expression | Partitioning Transform that truncates the input to a standard width `w` following the Iceberg Specification. | functions-etc.md#partition_iceberg_truncate
partition_iceberg_truncate | Expression | (w: int) -> Expression | Partitioning Transform that truncates the input to a standard width `w` following the Iceberg Specification. | expressions.md#partition_iceberg_truncate
partition_months | daft.functions | (expr: Expression) -> Expression | Partitioning Transform that returns the number of months since epoch (1970-01-01). | functions-etc.md#partition_months
partition_months | Expression | () -> Expression | Partitioning Transform that returns the number of months since epoch (1970-01-01). | expressions.md#partition_months
partition_years | daft.functions | (expr: Expression) -> Expression | Partitioning Transform that returns the number of years since epoch (1970-01-01). | functions-etc.md#partition_years
partition_years | Expression | () -> Expression | Partitioning Transform that returns the number of years since epoch (1970-01-01). | expressions.md#partition_years
pearson_correlation | daft.functions | (left: Expression, right: Expression) -> Expression | Compute the Pearson correlation between two embeddings. | functions-etc.md#pearson_correlation
pearson_correlation | Expression | (other: Expression) -> Expression | Compute the Pearson correlation between two embeddings. | expressions.md#pearson_correlation
percentile | daft.functions | (expr: Expression, percentage: float) -> Expression | Calculates the exact percentile for a column of numeric values. | functions-agg.md#percentile
percentile | Expression | (percentage: builtins.float) -> Expression | Calculates the exact percentile for a column of numeric values. | expressions.md#percentile
pi | daft.functions | () -> Expression | Returns the mathematical constant pi (3.14159...). | functions-numeric.md#pi
pipe | DataFrame | (function: Callable[Concatenate['DataFrame', P], T], *args: P.args, **kwargs: P.kwargs) -> T | Apply the function to this DataFrame. | dataframe.md#pipe
pivot | DataFrame | (group_by: ManyColumnsInputType, pivot_col: ColumnInputType, value_col: ColumnInputType, agg_fn: str, names: list[str] | None=None) -> DataFrame | Pivots a column of the DataFrame and performs an aggregation on the values. | dataframe.md#pivot
planning_config_ctx | daft | (**kwargs: Any) -> Generator[None, None, None] | Context manager that wraps set_planning_config to reset the config to its original setting afternwards. | toplevel.md#planning_config_ctx
pmod | daft.functions | (a: Expression, b: Expression) -> Expression | Returns the positive modulo of ``a`` by ``b``. | functions-numeric.md#pmod
pow | daft.functions | (base: Expression, expr: Expression) -> Expression | The base^expr of a numeric expression. | functions-numeric.md#pow
pow | Expression | (exp: Expression) -> Expression | The elementwise exponentiation of a numeric series. | expressions.md#pow
power | daft.functions | (base: Expression, expr: Expression) -> Expression | The base^expr of a numeric expression. | functions-numeric.md#power
power | Expression | (exp: Expression) -> Expression | The elementwise exponentiation of a numeric series. | expressions.md#power
product | daft.functions | (expr: Expression) -> Expression | Calculates the product of the values in the expression. | functions-agg.md#product
product | DataFrame | (*cols: ColumnInputType) -> DataFrame | Performs a global product on the DataFrame. | dataframe.md#product
product | Expression | () -> Expression | Calculates the product of the values in the expression. | expressions.md#product
prompt | daft.functions | (messages: list[Expression] | Expression, return_format: BaseModel | None=None, *, system_message: str | None=None, provider: str | Provider | None=None, model: str | None=None, **options: Any) -> Expression | Returns an expression that prompts a large language model using the specified model and provider. | functions-ai.md#prompt
quarter | daft.functions | (expr: Expression) -> Expression | Retrieves the quarter for a datetime column. | functions-datetime.md#quarter
quarter | Expression | () -> Expression | Retrieves the quarter for a datetime column. | expressions.md#quarter
radians | daft.functions | (expr: Expression) -> Expression | The elementwise radians of a numeric expression. | functions-numeric.md#radians
radians | Expression | () -> Expression | The elementwise radians of a numeric expression. | expressions.md#radians
random_int | daft.functions | (low: int, high: int, seed: int | None=None) -> Expression | Generates a column of random integer values. | functions-misc.md#random_int
range | daft | (start: int, end: int | None=None, step: int=1, partitions: int=1) -> DataFrame | Creates a DataFrame with a range of values. | toplevel.md#range
rank | daft.functions | () -> Expression | Return the rank of the current row (used for window functions). | functions-window.md#rank
read_csv | daft | (path: str | list[str], infer_schema: bool=True, schema: dict[str, DataType] | None=None, has_headers: bool=True, delimiter: str | None=None, double_quote: bool=True, quote: str | None=None, escape_char: str | None=None, comment: str | None=None, allow_variable_columns: bool=False, io_config: IOConfig | None=None, file_path_column: str | None=None, hive_partitioning: bool=False, ignore_corrupt_files: bool=False, _buffer_size: int | None=None, _chunk_size: int | None=None, checkpoint: 'CheckpointConfig | None'=None) -> DataFrame | Creates a DataFrame from CSV file(s). | toplevel.md#read_csv
read_deltalake | daft | (table: Union[str, 'UnityCatalogTable'], version: Union[int, str, 'datetime'] | None=None, io_config: IOConfig | None=None, ignore_deletion_vectors: bool=False, _multithreaded_io: bool | None=None) -> DataFrame | Create a DataFrame from a Delta Lake table. | toplevel.md#read_deltalake
read_hudi | daft | (table_uri: str, io_config: IOConfig | None=None, checkpoint: 'CheckpointConfig | None'=None) -> DataFrame | Create a DataFrame from a Hudi table. | toplevel.md#read_hudi
read_huggingface | daft | (repo: str, io_config: IOConfig | None=None) -> DataFrame | Create a DataFrame from a Hugging Face dataset. | toplevel.md#read_huggingface
read_iceberg | daft | (table: Union[str, os.PathLike[str], 'PyIcebergTable'], snapshot_id: int | None=None, branch: str | None=None, tag: str | None=None, io_config: IOConfig | None=None, checkpoint: 'CheckpointConfig | None'=None, ignore_corrupt_files: bool=False) -> DataFrame | Create a DataFrame from an Iceberg table. | toplevel.md#read_iceberg
read_json | daft | (path: str | list[str], infer_schema: bool=True, schema: dict[str, DataType] | None=None, io_config: IOConfig | None=None, file_path_column: str | None=None, hive_partitioning: bool=False, skip_empty_files: bool=False, _buffer_size: int | None=None, _chunk_size: int | None=None, checkpoint: 'CheckpointConfig | None'=None) -> DataFrame | Creates a DataFrame from line-delimited JSON file(s). | toplevel.md#read_json
read_kafka | daft | (bootstrap_servers: str | Sequence[str], topics: str | Sequence[str], *, start: object=_KIND_EARLIEST, end: object=_KIND_LATEST, group_id: str='daft-bounded-kafka-reader', partitions: Sequence[int] | None=None, chunk_size: int=1024, kafka_client_config: Mapping[str, object] | None=None, timeout_ms: int=10000) -> DataFrame | Creates a DataFrame by reading messages from Kafka topic(s). | toplevel.md#read_kafka
read_lance | daft | (uri: str | os.PathLike[str], io_config: Any=None, version: Any=None, asof: Any=None, block_size: Any=None, commit_lock: Any=None, index_cache_size: Any=None, default_scan_options: Any=None, metadata_cache_size_bytes: Any=None, fragment_group_size: Any=None, include_fragment_id: Any=None, checkpoint: Any=None) -> Any | Create a DataFrame from a LanceDB table. | toplevel.md#read_lance
read_mcap | daft | (path: str, io_config: IOConfig | None=None, start_time: int | None=None, end_time: int | None=None, topics: list[str] | None=None, batch_size: int=1000, topic_start_time_resolver: TopicStartTimeResolver | None=None) -> DataFrame | Read mcap file. | toplevel.md#read_mcap
read_paimon | daft | (table: 'PaimonTable', io_config: IOConfig | None=None) -> DataFrame | Create a DataFrame from an Apache Paimon table. | toplevel.md#read_paimon
read_parquet | daft | (path: str | list[str], row_groups: list[list[int]] | None=None, infer_schema: bool=True, schema: dict[str, DataType] | None=None, io_config: IOConfig | None=None, file_path_column: str | None=None, hive_partitioning: bool=False, coerce_int96_timestamp_unit: str | TimeUnit | None=None, ignore_corrupt_files: bool=False, geometry: bool=True, _multithreaded_io: bool | None=None, _chunk_size: int | None=None, checkpoint: 'CheckpointConfig | None'=None) -> DataFrame | Creates a DataFrame from Parquet file(s). | toplevel.md#read_parquet
read_sql | daft | (sql: str, conn: Callable[[], 'Connection'] | str, partition_col: str | None=None, num_partitions: int | None=None, partition_bound_strategy: str='min-max', disable_pushdowns_to_sql: bool=False, infer_schema: bool=True, infer_schema_length: int=10, schema: dict[str, DataType] | None=None) -> DataFrame | Create a DataFrame from the results of a SQL query. | toplevel.md#read_sql
read_table | daft | (identifier: Identifier | str, **options: Any) -> DataFrame | Returns the table as a DataFrame or raises an exception if it does not exist. | toplevel.md#read_table
read_text | daft | (path: str | list[str], *, encoding: str='utf-8', skip_blank_lines: bool=True, whole_text: bool=False, file_path_column: str | None=None, hive_partitioning: bool=False, io_config: IOConfig | None=None, _buffer_size: int | None=None, _chunk_size: int | None=None) -> DataFrame | Creates a DataFrame from line-oriented text file(s). | toplevel.md#read_text
read_video_frames | daft | (path: str | list[str], image_height: int, image_width: int, is_key_frame: bool | None=None, *, sample_interval_seconds: float | None=None, io_config: IOConfig | None=None) -> DataFrame | Creates a DataFrame by reading the frames of one or more video files. | toplevel.md#read_video_frames
read_warc | daft | (path: str | list[str], io_config: IOConfig | None=None, file_path_column: str | None=None, _multithreaded_io: bool | None=None, checkpoint: 'CheckpointConfig | None'=None) -> DataFrame | Creates a DataFrame from WARC or gzipped WARC file(s). | toplevel.md#read_warc
refresh_logger | daft | () -> None | Refreshes Daft's internal rust logging to the current python log level. | toplevel.md#refresh_logger
regexp | daft.functions | (expr: Expression, pattern: str | Expression) -> Expression | Check whether each string matches the given regular expression pattern in a string column. | functions-str.md#regexp
regexp | Expression | (pattern: builtins.str | Expression) -> Expression | Check whether each string matches the given regular expression pattern in a string column. | expressions.md#regexp
regexp_count | daft.functions | (expr: Expression, pattern: str | Expression) -> Expression | Counts the number of times a regex pattern appears in a string. | functions-str.md#regexp_count
regexp_count | Expression | (pattern: builtins.str | Expression) -> Expression | Counts the number of times a regex pattern appears in a string. | expressions.md#regexp_count
regexp_extract | daft.functions | (expr: Expression, pattern: str | Expression, index: int=0) -> Expression | Extracts the specified match group from the first regex match in each string in a string column. | functions-str.md#regexp_extract
regexp_extract | Expression | (pattern: builtins.str | Expression, index: int=0) -> Expression | Extracts the specified match group from the first regex match in each string in a string column. | expressions.md#regexp_extract
regexp_extract_all | daft.functions | (expr: Expression, pattern: str | Expression, index: int=0) -> Expression | Extracts the specified match group from all regex matches in each string in a string column. | functions-str.md#regexp_extract_all
regexp_extract_all | Expression | (pattern: builtins.str | Expression, index: int=0) -> Expression | Extracts the specified match group from all regex matches in each string in a string column. | expressions.md#regexp_extract_all
regexp_replace | daft.functions | (expr: Expression, pattern: str | Expression, replacement: str | Expression) -> Expression | Replaces all occurrences of a regex pattern in a string column with a replacement string. | functions-str.md#regexp_replace
regexp_replace | Expression | (pattern: builtins.str | Expression, replacement: builtins.str | Expression) -> Expression | Replaces all occurrences of a regex pattern in a string column with a replacement string. | expressions.md#regexp_replace
regexp_split | daft.functions | (expr: Expression, pattern: str | Expression) -> Expression | Splits each string on the given regex pattern, into a list of strings. | functions-str.md#regexp_split
regexp_split | Expression | (pattern: builtins.str | Expression) -> Expression | Splits each string on the given regex pattern, into a list of strings. | expressions.md#regexp_split
register_viz_hook | daft | (klass: type[HookClass], hook: Callable[[Any], str]) -> None | Registers a visualization hook that returns the appropriate HTML for visualizing a specific class in HTML. | toplevel.md#register_viz_hook
repartition | DataFrame | (num: int | None, *partition_by: ColumnInputType) -> DataFrame | Repartitions DataFrame to ``num`` partitions. | dataframe.md#repartition
repeat | daft.functions | (expr: Expression, n: int | Expression) -> Expression | Repeats each string n times. | functions-str.md#repeat
repeat | Expression | (n: int | Expression) -> Expression | Repeats each string n times. | expressions.md#repeat
replace | daft.functions | (expr: Expression, search: str | Expression, replacement: str | Expression) -> Expression | Replaces all occurrences of a substring in a string with a replacement string. | functions-str.md#replace
replace | Expression | (search: builtins.str | Expression, replacement: builtins.str | Expression) -> Expression | Replaces all occurrences of a substring in a string with a replacement string. | expressions.md#replace
replace_time_zone | daft.functions | (expr: Expression, timezone: str | None=None) -> Expression | Replaces the timezone of a timestamp while preserving the local time. | functions-datetime.md#replace_time_zone
replace_time_zone | Expression | (timezone: builtins.str | None=None) -> Expression | Replaces the timezone of a timestamp while preserving the local time. | expressions.md#replace_time_zone
resample | daft.functions | (file_expr: Expression, sample_rate: int) -> Expression | Resample a audio file. | functions-media.md#resample
resize | daft.functions | (image: Expression, w: int, h: int) -> Expression | Resize image into the provided width and height. | functions-media.md#resize
resize | Expression | (w: int, h: int) -> Expression | Resize image into the provided width and height. | expressions.md#resize
resolve_deltalake | DataFrame | () -> tuple[str, 'deltalake.DeltaTable'] | Resolve the Delta Lake table path and table object for this DataFrame. | dataframe.md#resolve_deltalake
resolve_parquet | DataFrame | () -> str | Resolve the parquet table path associated with this DataFrame. | dataframe.md#resolve_parquet
ResourceRequest | daft | (num_cpus: float | None=None, num_gpus: float | None=None, memory_bytes: int | None=None) | Resource request for a query fragment task. | toplevel.md#resourcerequest
reverse | daft.functions | (expr: Expression) -> Expression | Reverse a UTF-8 string. | functions-str.md#reverse
reverse | Expression | () -> Expression | Reverse a UTF-8 string. | expressions.md#reverse
right | daft.functions | (expr: Expression, nchars: int | Expression) -> Expression | Gets the n (from nchars) right-most characters of each string. | functions-str.md#right
right | Expression | (nchars: int | Expression) -> Expression | Gets the n (from nchars) right-most characters of each string. | expressions.md#right
round | daft.functions | (expr: Expression, decimals: Expression | int=0) -> Expression | The round of a numeric expression. | functions-numeric.md#round
round | Expression | (decimals: Expression | int=0) -> Expression | The round of a numeric expression. | expressions.md#round
row_number | daft.functions | () -> Expression | Return the row number of the current row (used for window functions). | functions-window.md#row_number
rpad | daft.functions | (expr: Expression, length: int | Expression, pad: str | Expression) -> Expression | Right-pads each string by truncating or padding with the character. | functions-str.md#rpad
rpad | Expression | (length: int | Expression, pad: builtins.str | Expression) -> Expression | Right-pads each string by truncating or padding with the character. | expressions.md#rpad
rstrip | daft.functions | (expr: Expression) -> Expression | Strip whitespace from the right side of a UTF-8 string. | functions-str.md#rstrip
rstrip | Expression | () -> Expression | Strip whitespace from the right side of a UTF-8 string. | expressions.md#rstrip
run_process | daft.functions | (args: Expression | list[Expression | Any], *, shell: bool=False, on_error: Literal['raise', 'ignore', 'log']='log', return_dtype: DataTypeLike=DataType.string()) -> Expression | Returns an expression that runs an external process (optionally via a shell) and exposes its stdout as a column. | functions-etc.md#run_process
runners | daft | (submodule) |  | toplevel.md#runners
S3Config | daft.io | (region_name: str | None=None, endpoint_url: str | None=None, key_id: str | None=None, session_token: str | None=None, access_key: str | None=None, credentials_provider: Callable[[], S3Credentials] | None=None, buffer_time: int | None=None, max_connections: int | None=None, retry_initial_backoff_ms: int | None=None, connect_timeout_ms: int | None=None, read_timeout_ms: int | None=None, num_tries: int | None=None, retry_mode: str | None=None, anonymous: bool | None=None, use_ssl: bool | None=None, verify_ssl: bool | None=None, check_hostname_ssl: bool | None=None, requester_pays: bool | None=None, force_virtual_addressing: bool | None=None, profile_name: str | None=None, multipart_size: int | None=None, multipart_max_concurrency: int | None=None, custom_retry_msgs: list[str] | None=None) | I/O configuration for accessing an S3-compatible system. | io.md#s3config
S3Credentials | daft.io | (key_id: str, access_key: str, session_token: str | None=None, expiry: datetime.datetime | None=None) |  | io.md#s3credentials
sample | DataFrame | (fraction: float | None=None, size: int | None=None, with_replacement: bool=False, seed: int | None=None) -> DataFrame | Samples rows from the DataFrame. | dataframe.md#sample
Schema | daft | () -> None |  | toplevel.md#schema
schema | DataFrame | () -> Schema | Returns the Schema of the DataFrame, which provides information about each column, as a Python object. | dataframe.md#schema
sec | daft.functions | (expr: Expression) -> Expression | The elementwise secant of a numeric expression. | functions-numeric.md#sec
sec | Expression | () -> Expression | The elementwise secant of a numeric expression. | expressions.md#sec
second | daft.functions | (expr: Expression) -> Expression | Retrieves the second for a datetime column. | functions-datetime.md#second
second | Expression | () -> Expression | Retrieves the second for a datetime column. | expressions.md#second
select | DataFrame | (*columns: ColumnInputType, **projections: Expression) -> DataFrame | Creates a new DataFrame from the provided expressions, similar to a SQL ``SELECT``. | dataframe.md#select
seq | daft.functions | (n: Expression) -> Expression | Generates a list of sequential integers [0, 1, 2, ..., n-1] for each row. | functions-list.md#seq
serialize | daft.functions | (expr: Expression, format: Literal['json']) -> Expression | Serializes a value to a string using the specified format. | functions-str.md#serialize
serialize | Expression | (format: Literal['json']) -> Expression | Serializes the expression as a string using the specified format. | expressions.md#serialize
Series | daft | () -> None | A Daft Series is an array of data of a single type, and is usually a column in a DataFrame. | toplevel.md#series
Session | daft | () -> None | Session holds a connection's state and orchestrates execution of DataFrame and SQL queries against catalogs. | toplevel.md#session
session | daft | () -> Session | Creates a default daft session to be used with a context manager. | toplevel.md#session
set_catalog | daft | (identifier: str | None) -> None | Set the given catalog as current_catalog for the current session or raises an if it does not exist. | toplevel.md#set_catalog
set_execution_config | daft | (config: PyDaftExecutionConfig | None=None, enable_scan_task_split_and_merge: bool | None=None, scan_tasks_min_size_bytes: int | None=None, scan_tasks_max_size_bytes: int | None=None, max_sources_per_scan_task: int | None=None, broadcast_join_size_bytes_threshold: int | None=None, parquet_split_row_groups_max_files: int | None=None, hash_join_partition_size_leniency: float | None=None, sample_size_for_sort: int | None=None, num_preview_rows: int | None=None, parquet_target_filesize: int | None=None, parquet_target_row_group_size: int | None=None, parquet_inflation_factor: float | None=None, csv_target_filesize: int | None=None, csv_inflation_factor: float | None=None, json_target_filesize: int | None=None, json_inflation_factor: float | None=None, text_inflation_factor: float | None=None, shuffle_aggregation_default_partitions: int | None=None, partial_aggregation_threshold: int | None=None, high_cardinality_aggregation_threshold: float | None=None, read_sql_partition_size_bytes: int | None=None, default_morsel_size: int | None=None, shuffle_algorithm: str | None=None, pre_shuffle_merge_threshold: int | None=None, pre_shuffle_merge_partition_threshold: int | None=None, scantask_max_parallel: int | None=None, native_parquet_writer: bool | None=None, min_cpu_per_task: float | None=None, actor_udf_ready_timeout: int | None=None, maintain_order: bool | None=None, enable_dynamic_batching: bool | None=None, dynamic_batching_strategy: str | None=None, flight_shuffle_dirs: list[str] | None=None, flight_shuffle_compression: str | None=None, enable_multi_glob_path_tasks: bool | None=None) -> DaftContext | Globally sets various configuration parameters which control various aspects of Daft execution. | toplevel.md#set_execution_config
set_model | daft | (identifier: str | None) -> None | Set the given model as current_model for the active session. | toplevel.md#set_model
set_namespace | daft | (identifier: Identifier | str | None) -> None | Set the given namespace as current_namespace for the active session. | toplevel.md#set_namespace
set_planning_config | daft | (config: PyDaftPlanningConfig | None=None, default_io_config: IOConfig | None=None, enable_strict_filter_pushdown: bool | None=None) -> DaftContext | Globally sets various configuration parameters which control Daft plan construction behavior. | toplevel.md#set_planning_config
set_provider | daft | (identifier: str | Provider | None, **options: Any) -> None | Set the given provider as current_provider for the active session. | toplevel.md#set_provider
set_runner_native | daft | (num_threads: int | None=None) -> Runner[PartitionT] | Configure Daft to execute dataframes using native multi-threaded processing. | toplevel.md#set_runner_native
set_runner_ray | daft | (address: str | None=None, noop_if_initialized: bool=False, force_client_mode: bool=False, *, downscale_enabled: bool | None=None, downscale_idle_seconds: int | None=None, min_survivor_workers: int | None=None, pending_release_exclude_seconds: int | None=None, worker_startup_timeout: int | None=None) -> Runner[PartitionT] | Configure Daft to execute dataframes using the Ray distributed computing framework. | toplevel.md#set_runner_ray
set_session | daft | (session: Session) -> None | Sets the global context's current session. | toplevel.md#set_session
shift_left | daft.functions | (expr: Expression, num_bits: Expression) -> Expression | Shifts the bits of an integer expression to the left (``expr << num_bits``). | functions-etc.md#shift_left
shift_left | Expression | (other: Expression) -> Expression | Shifts the bits of an integer expression to the left (``expr << other``). | expressions.md#shift_left
shift_right | daft.functions | (expr: Expression, num_bits: Expression) -> Expression | Shifts the bits of an integer expression to the right (``expr >> num_bits``). | functions-etc.md#shift_right
shift_right | Expression | (other: Expression) -> Expression | Shifts the bits of an integer expression to the right (``expr >> other``). | expressions.md#shift_right
show | DataFrame | (n: int=8, format: PreviewFormat | None=None, verbose: bool | None=None, max_width: int | None=None, align: PreviewAlign | None=None, columns: list[PreviewColumn] | None=None) -> None | Executes enough of the DataFrame in order to display the first ``n`` rows. | dataframe.md#show
shuffle | DataFrame | (seed: int | None=None) -> DataFrame | Randomly reorders rows of the DataFrame. | dataframe.md#shuffle
sign | daft.functions | (expr: Expression) -> Expression | The sign of a numeric expression. | functions-numeric.md#sign
sign | Expression | () -> Expression | The sign of a numeric expression. | expressions.md#sign
simhash | daft.functions | (text: Expression, *, ngram_size: int=3, hash_function: Literal['murmurhash3', 'xxhash', 'xxhash32', 'xxhash64', 'xxhash3_64', 'sha1']='xxhash3_64') -> Expression | Compute a SimHash fingerprint of the input text. | functions-misc.md#simhash
simhash | Expression | (*, ngram_size: int=3, hash_function: Literal['murmurhash3', 'xxhash', 'xxhash32', 'xxhash64', 'xxhash3_64', 'sha1']='xxhash3_64') -> Expression | Compute a SimHash fingerprint of this string expression. | expressions.md#simhash
sin | daft.functions | (expr: Expression) -> Expression | The elementwise sine of a numeric expression. | functions-numeric.md#sin
sin | Expression | () -> Expression | The elementwise sine of a numeric expression. | expressions.md#sin
sinh | daft.functions | (expr: Expression) -> Expression | The elementwise hyperbolic sine of a numeric expression. | functions-numeric.md#sinh
sinh | Expression | () -> Expression | The elementwise hyperbolic sine of a numeric expression. | expressions.md#sinh
skew | daft.functions | (expr: Expression) -> Expression | Calculates the skewness of the values from the expression. | functions-agg.md#skew
skew | DataFrame | (*cols: ColumnInputType) -> DataFrame | Performs a global skew on the DataFrame. | dataframe.md#skew
skew | Expression | () -> Expression | Calculates the skewness of the values from the expression. | expressions.md#skew
skip_existing | DataFrame | (existing_path: str | pathlib.Path | list[str | pathlib.Path], key_column: str | list[str], file_format: str | FileFormat, io_config: IOConfig | None=None, num_workers: int=4, cpus_per_worker: float=0.5, keys_load_batch_size: int=100000, max_concurrency_per_worker: int=1, filter_batch_size: int=10000, **reader_args: Any) -> DataFrame | Filter out rows whose key(s) already exist in existing data (i.e., already processed rows). | dataframe.md#skip_existing
skipped_corrupt_files | DataFrame | () -> list[tuple[str, str, bool]] | Files skipped during the last execution due to ignore_corrupt_files=True. | dataframe.md#skipped_corrupt_files
slice | daft.functions | (expr: Expression, start: int | Expression, end: int | Expression | None=None) -> Expression | Get a subset of each list or binary value. | functions-misc.md#slice
slice | Expression | (start: int | Expression, end: int | Expression | None=None) -> Expression | Get a subset of each list or binary value. | expressions.md#slice
sort | DataFrame | (by: ColumnInputType | list[ColumnInputType], desc: bool | list[bool]=False, nulls_first: bool | list[bool] | None=None) -> DataFrame | Sorts DataFrame globally. | dataframe.md#sort
soundex | daft.functions | (expr: Expression) -> Expression | Returns the Soundex code of the string. | functions-str.md#soundex
soundex | Expression | () -> Expression | Returns the Soundex code of the string. | expressions.md#soundex
space | daft.functions | (expr: Expression) -> Expression | Returns a string consisting of n space characters. | functions-str.md#space
split | daft.functions | (expr: Expression, split_on: str | Expression) -> Expression | Splits each string on the given string, into a list of strings. | functions-str.md#split
split | Expression | (split_on: builtins.str | Expression) -> Expression | Splits each string on the given string, into a list of strings. | expressions.md#split
sql | daft | (sql: str, register_globals: bool=True, **bindings: DataFrame) -> DataFrame | Run a SQL query, returning the results as a DataFrame. | toplevel.md#sql
sql_expr | daft | (sql: str) -> Expression | Parses a SQL string into a Daft Expression. | toplevel.md#sql_expr
sqrt | daft.functions | (expr: Expression) -> Expression | The square root of a numeric expression. | functions-numeric.md#sqrt
sqrt | Expression | () -> Expression | The square root of a numeric expression. | expressions.md#sqrt
st_area | daft.functions | (geom: Expression, use_spheroid: bool=False) -> Expression | Return the 2D area of a geometry. | functions-spatial.md#st_area
st_astext | daft.functions | (geom: Expression) -> Expression | Return the Well-Known Text (WKT) representation of a geometry. | functions-spatial.md#st_astext
st_bbox | daft.functions | (geom: Expression) -> Expression | Returns the geometry's bounding box as a struct ``{min_x, min_y, max_x, max_y}`` (Float64). | functions-spatial.md#st_bbox
st_buffer | daft.functions | (geom: Expression, distance: float) -> Expression | Return a geometry that is the given distance from the input geometry (planar Cartesian). | functions-spatial.md#st_buffer
st_centroid | daft.functions | (geom: Expression) -> Expression | Return the geometric centroid (center of mass) of a geometry as a Point. | functions-spatial.md#st_centroid
st_contains | daft.functions | (geom_a: Expression, geom_b: Expression) -> Expression | Return whether geometry A completely contains geometry B. | functions-spatial.md#st_contains
st_convexhull | daft.functions | (geom: Expression) -> Expression | Return the convex hull of a geometry as a Polygon. | functions-spatial.md#st_convexhull
st_covered_by | daft.functions | (geom_a: Expression, geom_b: Expression) -> Expression | Returns true if geometry A is covered by geometry B (no point of A is outside B; boundary included). | functions-spatial.md#st_covered_by
st_covers | daft.functions | (geom_a: Expression, geom_b: Expression) -> Expression | Returns true if geometry A covers geometry B (no point of B is outside A; boundary included). | functions-spatial.md#st_covers
st_crosses | daft.functions | (geom_a: Expression, geom_b: Expression) -> Expression | Return true where A and B cross. | functions-spatial.md#st_crosses
st_difference | daft.functions | (geom_a: Expression, geom_b: Expression) -> Expression | Return the part of geometry A that does not intersect geometry B. | functions-spatial.md#st_difference
st_disjoint | daft.functions | (geom_a: Expression, geom_b: Expression) -> Expression | Return true where A and B share no points. | functions-spatial.md#st_disjoint
st_distance | daft.functions | (geom_a: Expression, geom_b: Expression, use_spheroid: bool=False) -> Expression | Return the minimum distance between two geometries. | functions-spatial.md#st_distance
st_dwithin | daft.functions | (geom_a: Expression, geom_b: Expression, distance: float) -> Expression | Returns true if the planar distance between two geometries is <= ``distance`` (coordinate units). | functions-spatial.md#st_dwithin
st_envelope | daft.functions | (geom: Expression) -> Expression | Return the minimum bounding rectangle of a geometry as a Polygon. | functions-spatial.md#st_envelope
st_equals | daft.functions | (geom_a: Expression, geom_b: Expression) -> Expression | Return true where A and B are topologically equal. | functions-spatial.md#st_equals
st_geohash | daft.functions | (geom: Expression, precision: int=5) -> Expression | Return the geohash of a geometry's centroid. | functions-spatial.md#st_geohash
st_geojsonfromgeom | daft.functions | (geom: Expression) -> Expression | Return the GeoJSON representation of a geometry. | functions-spatial.md#st_geojsonfromgeom
st_geometrytype | daft.functions | (geom: Expression) -> Expression | Return the geometry type name as a string. | functions-spatial.md#st_geometrytype
st_geomfromgeojson | daft.functions | (geojson: Expression) -> Expression | Parse a GeoJSON geometry or feature string into a Geometry. | functions-spatial.md#st_geomfromgeojson
st_geomfromtext | daft.functions | (wkt: Expression) -> Expression | Parse a Well-Known Text (WKT) string into a Geometry. | functions-spatial.md#st_geomfromtext
st_intersection | daft.functions | (geom_a: Expression, geom_b: Expression) -> Expression | Return the geometric intersection of two polygon geometries. | functions-spatial.md#st_intersection
st_intersects | daft.functions | (geom_a: Expression, geom_b: Expression) -> Expression | Return whether geometry A and geometry B spatially intersect. | functions-spatial.md#st_intersects
st_isvalid | daft.functions | (geom: Expression) -> Expression | Return whether a geometry is topologically valid according to OGC rules. | functions-spatial.md#st_isvalid
st_length | daft.functions | (geom: Expression, use_spheroid: bool=False) -> Expression | Return the length of line geometries. | functions-spatial.md#st_length
st_makeline | daft.functions | (geom_a: Expression, geom_b: Expression) -> Expression | Construct a LineString geometry from two Point geometries. | functions-spatial.md#st_makeline
st_makevalid | daft.functions | (geom: Expression) -> Expression | Repair an invalid geometry, returning a valid one. | functions-spatial.md#st_makevalid
st_overlaps | daft.functions | (geom_a: Expression, geom_b: Expression) -> Expression | Return true where A and B overlap (same dimension, partial intersection). | functions-spatial.md#st_overlaps
st_perimeter | daft.functions | (geom: Expression, use_spheroid: bool=False) -> Expression | Return the perimeter of areal geometries (Polygon, MultiPolygon). | functions-spatial.md#st_perimeter
st_point | daft.functions | (x: Expression, y: Expression) -> Expression | Construct a Point geometry from x and y coordinate columns. | functions-spatial.md#st_point
st_pointonsurface | daft.functions | (geom: Expression) -> Expression | Return a Point guaranteed to lie on the surface of a geometry. | functions-spatial.md#st_pointonsurface
st_simplify | daft.functions | (geom: Expression, tolerance: float) -> Expression | Simplify a geometry using the Ramer–Douglas–Peucker algorithm. | functions-spatial.md#st_simplify
st_symdifference | daft.functions | (geom_a: Expression, geom_b: Expression) -> Expression | Return the symmetric difference (XOR) of two polygon geometries. | functions-spatial.md#st_symdifference
st_touches | daft.functions | (geom_a: Expression, geom_b: Expression) -> Expression | Return true where A and B share a boundary but their interiors do not intersect. | functions-spatial.md#st_touches
st_union | daft.functions | (geom_a: Expression, geom_b: Expression) -> Expression | Return the geometric union of two polygon geometries. | functions-spatial.md#st_union
st_within | daft.functions | (geom_a: Expression, geom_b: Expression) -> Expression | Return whether geometry A is completely within geometry B. | functions-spatial.md#st_within
st_x | daft.functions | (geom: Expression) -> Expression | Return the X (longitude) coordinate of a Point geometry. | functions-spatial.md#st_x
st_y | daft.functions | (geom: Expression) -> Expression | Return the Y (latitude) coordinate of a Point geometry. | functions-spatial.md#st_y
startswith | daft.functions | (expr: Expression, prefix: str | Expression) -> Expression | Checks whether each string starts with the given prefix in a string column. | functions-str.md#startswith
startswith | Expression | (prefix: builtins.str | Expression) -> Expression | Checks whether each string starts with the given pattern in a string column. | expressions.md#startswith
stddev | daft.functions | (expr: Expression, ddof: int=1) -> Expression | Calculates the standard deviation of the values in the expression. | functions-agg.md#stddev
stddev | DataFrame | (*cols: ColumnInputType, ddof: int=1) -> DataFrame | Performs a global standard deviation on the DataFrame. | dataframe.md#stddev
stddev | Expression | (ddof: int=1) -> Expression | Calculates the standard deviation of the values in the expression. | expressions.md#stddev
strftime | daft.functions | (expr: Expression, format: str | None=None) -> Expression | Converts a datetime/date column to a string column. | functions-datetime.md#strftime
strftime | Expression | (format: builtins.str | None=None) -> Expression | Converts a datetime/date column to a string column. | expressions.md#strftime
string_agg | daft.functions | (expr: Expression, delimiter: str | None=None) -> Expression | Aggregates the values in the expression into a single string by concatenating them. | functions-agg.md#string_agg
string_agg | Expression | (delimiter: str | None=None) -> Expression | Aggregates the values in the expression into a single string by concatenating them. | expressions.md#string_agg
strip | daft.functions | (expr: Expression) -> Expression | Strip whitespace from both sides of string. | functions-str.md#strip
strip | Expression | () -> Expression | Strip whitespace from both sides of a UTF-8 string. | expressions.md#strip
substr | daft.functions | (expr: Expression, start: int | Expression, length: int | Expression | None=None) -> Expression | Extract a substring from a string, starting at a specified index and extending for a given length. | functions-str.md#substr
substr | Expression | (start: int | Expression, length: int | Expression | None=None) -> Expression | Extract a substring from a string, starting at a specified index and extending for a given length. | expressions.md#substr
substring_index | daft.functions | (expr: Expression, delim: str | Expression, count: int | Expression) -> Expression | Returns the substring from string before count occurrences of the delimiter. | functions-str.md#substring_index
substring_index | Expression | (delim: builtins.str | Expression, count: builtins.int | Expression) -> Expression | Returns the substring from string before count occurrences of the delimiter. | expressions.md#substring_index
sum | daft.functions | (expr: Expression) -> Expression | Calculates the sum of the values in the expression. | functions-agg.md#sum
sum | DataFrame | (*cols: ManyColumnsInputType) -> DataFrame | Performs a global sum on the DataFrame. | dataframe.md#sum
sum | Expression | () -> Expression | Calculates the sum of the values in the expression. | expressions.md#sum
summarize | DataFrame | () -> DataFrame | Returns column statistics for the DataFrame. | dataframe.md#summarize
Table | daft | () | Interface for python table implementations. | toplevel.md#table
tan | daft.functions | (expr: Expression) -> Expression | The elementwise tangent of a numeric expression. | functions-numeric.md#tan
tan | Expression | () -> Expression | The elementwise tangent of a numeric expression. | expressions.md#tan
tanh | daft.functions | (expr: Expression) -> Expression | The elementwise hyperbolic tangent of a numeric expression. | functions-numeric.md#tanh
tanh | Expression | () -> Expression | The elementwise hyperbolic tangent of a numeric expression. | expressions.md#tanh
time | daft.functions | (expr: Expression) -> Expression | Retrieves the time for a datetime column. | functions-datetime.md#time
time | Expression | () -> Expression | Retrieves the time for a datetime column. | expressions.md#time
timestamp_micros | daft.functions | (expr: Expression) -> Expression | Creates a timestamp from microseconds since Unix epoch. | functions-datetime.md#timestamp_micros
timestamp_millis | daft.functions | (expr: Expression) -> Expression | Creates a timestamp from milliseconds since Unix epoch. | functions-datetime.md#timestamp_millis
timestamp_seconds | daft.functions | (expr: Expression) -> Expression | Creates a timestamp from seconds since Unix epoch. | functions-datetime.md#timestamp_seconds
TimeUnit | daft | () -> None |  | toplevel.md#timeunit
to_arrow | DataFrame | () -> pyarrow.Table | Converts the current DataFrame to a [pyarrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html). | dataframe.md#to_arrow
to_arrow_expr | Expression | () -> pc.Expression | Returns this expression as a pyarrow.compute.Expression for integrations with other systems. | expressions.md#to_arrow_expr
to_arrow_iter | DataFrame | (results_buffer_size: int | None | Literal['num_cpus']='num_cpus') -> Iterator['pyarrow.RecordBatch'] | Return an iterator of pyarrow recordbatches for this dataframe. | dataframe.md#to_arrow_iter
to_camel_case | daft.functions | (expr: Expression) -> Expression | Convert a string to lower camel case. | functions-str.md#to_camel_case
to_camel_case | Expression | () -> Expression | Convert a string to lower camel case. | expressions.md#to_camel_case
to_dask_dataframe | DataFrame | (meta: Union['pandas.DataFrame', 'pandas.Series[Any]', dict[str, Any], Iterable[Any], tuple[Any], None]=None) -> dask.DataFrame | Converts the current Daft DataFrame to a Dask DataFrame. | dataframe.md#to_dask_dataframe
to_date | daft.functions | (expr: Expression, format: str) -> Expression | Converts a string to a date using the specified format. | functions-datetime.md#to_date
to_date | Expression | (format: builtins.str) -> Expression | Converts a string to a date using the specified format. | expressions.md#to_date
to_datetime | daft.functions | (expr: Expression, format: str, timezone: str | None=None) -> Expression | Converts a string to a datetime using the specified format and timezone. | functions-datetime.md#to_datetime
to_datetime | Expression | (format: builtins.str, timezone: builtins.str | None=None) -> Expression | Converts a string to a datetime using the specified format and timezone. | expressions.md#to_datetime
to_kebab_case | daft.functions | (expr: Expression) -> Expression | Convert a string to kebab case. | functions-str.md#to_kebab_case
to_kebab_case | Expression | () -> Expression | Convert a string to kebab case. | expressions.md#to_kebab_case
to_list | daft.functions | (*items: Expression) -> Expression | Constructs a list from the item expressions. | functions-list.md#to_list
to_pandas | DataFrame | (coerce_temporal_nanoseconds: bool=False) -> pandas.DataFrame | Converts the current DataFrame to a [pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html). | dataframe.md#to_pandas
to_pydict | DataFrame | (maps_as_pydicts: Literal['lossy', 'strict'] | None=None) -> dict[str, list[Any]] | Converts the current DataFrame to a python dictionary. | dataframe.md#to_pydict
to_pylist | DataFrame | (maps_as_pydicts: Literal['lossy', 'strict'] | None=None) -> list[Any] | Converts the current Dataframe into a python list. | dataframe.md#to_pylist
to_ray_dataset | DataFrame | () -> ray.data.dataset.DataSet | Converts the current DataFrame to a [Ray Dataset](https://docs.ray.io/en/latest/data/api/dataset.html#ray.data.Dataset) which is useful for running distributed ML model training in Ray. | dataframe.md#to_ray_dataset
to_snake_case | daft.functions | (expr: Expression) -> Expression | Convert a string to snake case. | functions-str.md#to_snake_case
to_snake_case | Expression | () -> Expression | Convert a string to snake case. | expressions.md#to_snake_case
to_struct | daft.functions | (*fields: Expression, **named_fields: Expression) -> Expression | Constructs a struct from the input expressions. | functions-etc.md#to_struct
to_title_case | daft.functions | (expr: Expression) -> Expression | Convert a string to title case. | functions-str.md#to_title_case
to_title_case | Expression | () -> Expression | Convert a string to title case. | expressions.md#to_title_case
to_torch_dataloader | DataFrame | (batch_size: int=1, *, pin_memory: bool=False, pin_memory_device: str='', prefetch_count: int=0) -> DaftTorchDataLoader | Return a DataLoader-like iterator that streams batched partitions for PyTorch training. | dataframe.md#to_torch_dataloader
to_torch_iter_dataset | DataFrame | (shard_strategy: Literal['file'] | None=None, world_size: int | None=None, rank: int | None=None) -> torch.utils.data.IterableDataset | Convert the current DataFrame into a `Torch IterableDataset <https://pytorch.org/docs/stable/data.html#torch.utils.data.IterableDataset>`__ for use with PyTorch. | dataframe.md#to_torch_iter_dataset
to_torch_map_dataset | DataFrame | (shard_strategy: Literal['file'] | None=None, world_size: int | None=None, rank: int | None=None) -> torch.utils.data.Dataset | Convert the current DataFrame into a map-style [Torch Dataset](https://pytorch.org/docs/stable/data.html#map-style-datasets) for use with PyTorch. | dataframe.md#to_torch_map_dataset
to_unix_epoch | daft.functions | (expr: Expression, time_unit: str | TimeUnit | None=None) -> Expression | Converts a datetime column to a Unix timestamp with the specified time unit. | functions-datetime.md#to_unix_epoch
to_unix_epoch | Expression | (time_unit: builtins.str | TimeUnit | None=None) -> Expression | Converts a datetime column to a Unix timestamp with the specified time unit. | expressions.md#to_unix_epoch
to_upper_camel_case | daft.functions | (expr: Expression) -> Expression | Convert a string to upper camel case. | functions-str.md#to_upper_camel_case
to_upper_camel_case | Expression | () -> Expression | Convert a string to upper camel case. | expressions.md#to_upper_camel_case
to_upper_kebab_case | daft.functions | (expr: Expression) -> Expression | Convert a string to upper kebab case. | functions-str.md#to_upper_kebab_case
to_upper_kebab_case | Expression | () -> Expression | Convert a string to upper kebab case. | expressions.md#to_upper_kebab_case
to_upper_snake_case | daft.functions | (expr: Expression) -> Expression | Convert a string to upper snake case. | functions-str.md#to_upper_snake_case
to_upper_snake_case | Expression | () -> Expression | Convert a string to upper snake case. | expressions.md#to_upper_snake_case
to_utc_timestamp | daft.functions | (expr: Expression, timezone: str) -> Expression | Interprets a wall-clock timestamp in the given timezone and returns the UTC instant. | functions-datetime.md#to_utc_timestamp
tokenize_decode | daft.functions | (expr: Expression, tokens_path: str, *, io_config: IOConfig | None=None, pattern: str | None=None, special_tokens: str | None=None) -> Expression | Decodes each list of integer tokens into a string using a tokenizer. | functions-str.md#tokenize_decode
tokenize_decode | Expression | (tokens_path: builtins.str, *, io_config: IOConfig | None=None, pattern: builtins.str | None=None, special_tokens: builtins.str | None=None) -> Expression | Decodes each list of integer tokens into a string using a tokenizer. | expressions.md#tokenize_decode
tokenize_encode | daft.functions | (expr: Expression, tokens_path: str, *, io_config: IOConfig | None=None, pattern: str | None=None, special_tokens: str | None=None, use_special_tokens: bool | None=None) -> Expression | Encodes each string as a list of integer tokens using a tokenizer. | functions-str.md#tokenize_encode
tokenize_encode | Expression | (tokens_path: builtins.str, *, io_config: IOConfig | None=None, pattern: builtins.str | None=None, special_tokens: builtins.str | None=None, use_special_tokens: bool | None=None) -> Expression | Encodes each string as a list of integer tokens using a tokenizer. | expressions.md#tokenize_encode
TosConfig | daft.io | (region: str | None=None, endpoint: str | None=None, access_key: str | None=None, secret_key: str | None=None, security_token: str | None=None, anonymous: bool | None=None, max_retries: int | None=None, retry_timeout_ms: int | None=None, connect_timeout_ms: int | None=None, read_timeout_ms: int | None=None, max_concurrent_requests: int | None=None, max_connections_per_io_thread: int | None=None) | I/O configuration for accessing Volcengine TOS (Torch Object Storage). | io.md#tosconfig
total_days | daft.functions | (expr: Expression) -> Expression | Calculates the total number of days for a duration column. | functions-datetime.md#total_days
total_days | Expression | () -> Expression | Calculates the total number of days for a duration column. | expressions.md#total_days
total_hours | daft.functions | (expr: Expression) -> Expression | Calculates the total number of hours for a duration column. | functions-datetime.md#total_hours
total_hours | Expression | () -> Expression | Calculates the total number of hours for a duration column. | expressions.md#total_hours
total_microseconds | daft.functions | (expr: Expression) -> Expression | Calculates the total number of microseconds for a duration column. | functions-datetime.md#total_microseconds
total_microseconds | Expression | () -> Expression | Calculates the total number of microseconds for a duration column. | expressions.md#total_microseconds
total_milliseconds | daft.functions | (expr: Expression) -> Expression | Calculates the total number of milliseconds for a duration column. | functions-datetime.md#total_milliseconds
total_milliseconds | Expression | () -> Expression | Calculates the total number of milliseconds for a duration column. | expressions.md#total_milliseconds
total_minutes | daft.functions | (expr: Expression) -> Expression | Calculates the total number of minutes for a duration column. | functions-datetime.md#total_minutes
total_minutes | Expression | () -> Expression | Calculates the total number of minutes for a duration column. | expressions.md#total_minutes
total_nanoseconds | daft.functions | (expr: Expression) -> Expression | Calculates the total number of nanoseconds for a duration column. | functions-datetime.md#total_nanoseconds
total_nanoseconds | Expression | () -> Expression | Calculates the total number of nanoseconds for a duration column. | expressions.md#total_nanoseconds
total_seconds | daft.functions | (expr: Expression) -> Expression | Calculates the total number of seconds for a duration column. | functions-datetime.md#total_seconds
total_seconds | Expression | () -> Expression | Calculates the total number of seconds for a duration column. | expressions.md#total_seconds
transform | DataFrame | (func: Callable[..., 'DataFrame'], *args: Any, **kwargs: Any) -> DataFrame | Apply a function that takes and returns a DataFrame. | dataframe.md#transform
translate | daft.functions | (expr: Expression, from_str: str | Expression, to_str: str | Expression) -> Expression | Translates characters in the input string by replacing characters in 'from_str' with corresponding characters in 'to_str'. | functions-str.md#translate
translate | Expression | (from_str: builtins.str | Expression, to_str: builtins.str | Expression) -> Expression | Translates characters in the string by replacing characters in 'from_str' with corresponding characters in 'to_str'. | expressions.md#translate
trunc | daft.functions | (expr: Expression, interval: str, relative_to: Expression | None=None) -> Expression | Alias for ``date_trunc`` with Spark-style argument order. | functions-datetime.md#trunc
try_cast | daft.functions | (expr: Expression, dtype: DataTypeLike) -> Expression | Attempts to cast an expression to the given datatype, returning null on failure. | functions-misc.md#try_cast
try_cast | Expression | (dtype: DataTypeLike) -> Expression | Attempts to cast an expression to the given datatype, returning null on failure. | expressions.md#try_cast
try_compress | daft.functions | (expr: Expression, codec: COMPRESSION_CODEC) -> Expression | Compress or null if unsuccessful. | functions-etc.md#try_compress
try_compress | Expression | (codec: COMPRESSION_CODEC) -> Expression | Compress or null if unsuccessful. | expressions.md#try_compress
try_decode | daft.functions | (bytes: Expression, charset: ENCODING_CHARSET) -> Expression | Decode or null if unsuccessful. | functions-etc.md#try_decode
try_decode | Expression | (charset: ENCODING_CHARSET) -> Expression | Decode or null if unsuccessful. | expressions.md#try_decode
try_decompress | daft.functions | (expr: Expression, codec: COMPRESSION_CODEC) -> Expression | Decompress or null if unsuccessful. | functions-etc.md#try_decompress
try_decompress | Expression | (codec: COMPRESSION_CODEC) -> Expression | Decompress or null if unsuccessful. | expressions.md#try_decompress
try_deserialize | daft.functions | (expr: Expression, format: Literal['json'], dtype: DataTypeLike) -> Expression | Deserializes a string using the specified format and data type, inserting nulls on failures. | functions-str.md#try_deserialize
try_deserialize | Expression | (format: Literal['json'], dtype: DataTypeLike) -> Expression | Deserializes the expression (string) using the specified format and data type, inserting nulls on failures. | expressions.md#try_deserialize
try_encode | daft.functions | (expr: Expression, charset: ENCODING_CHARSET) -> Expression | Encode or null if unsuccessful. | functions-etc.md#try_encode
try_encode | Expression | (charset: ENCODING_CHARSET) -> Expression | Encode or null if unsuccessful. | expressions.md#try_encode
udaf | daft | (: type | None=None, *, return_dtype: DataTypeLike, state: DataTypeLike | dict[str, DataTypeLike]) -> type | Callable[[type], type] | Decorator to create a user-defined aggregate function (UDAF) from a class. | toplevel.md#udaf
udf | daft | (*, return_dtype: DataTypeLike, num_cpus: float | None=None, num_gpus: float | None=None, memory_bytes: int | None=None, ray_options: dict[str, Any] | None=None, batch_size: int | None=None, concurrency: int | None=None, use_process: bool | None=None) -> Callable[[UserDefinedPyFuncLike], UDF] | (DEPRECATED) `@udf` Decorator to convert a Python function/class into a `UDF`. | toplevel.md#udf
udf | Expression | (name: builtins.str, inner: UninitializedUdf, bound_args: BoundUDFArgs, expressions: builtins.list[Expression], return_dtype: DataType, init_args: InitArgsType, resource_request: ResourceRequest | None, batch_size: int | None, concurrency: int | None, use_process: bool | None, ray_options: dict[builtins.str, builtins.str] | None=None) -> Expression |  | expressions.md#udf
union | DataFrame | (other: 'DataFrame') -> DataFrame | Returns the distinct union of two DataFrames. | dataframe.md#union
union_all | DataFrame | (other: 'DataFrame') -> DataFrame | Returns the union of two DataFrames, including duplicates. | dataframe.md#union_all
union_all_by_name | DataFrame | (other: 'DataFrame') -> DataFrame | Returns the union of two DataFrames, including duplicates, with columns matched by name. | dataframe.md#union_all_by_name
union_by_name | DataFrame | (other: 'DataFrame') -> DataFrame | Returns the distinct union by name. | dataframe.md#union_by_name
UnionMode | daft | () | Union mode for Arrow union types. | toplevel.md#unionmode
unique | DataFrame | (*by: ColumnInputType) -> DataFrame | Computes distinct rows, dropping duplicates. | dataframe.md#unique
UnityConfig | daft.io | (endpoint: str | None, token: str | None) | I/O configuration for Unity Catalog volumes. | io.md#unityconfig
unix_date | daft.functions | (expr: Expression) -> Expression | Retrieves the number of days since 1970-01-01 00:00:00 UTC. | functions-datetime.md#unix_date
unix_date | Expression | () -> Expression | Retrieves the number of days since 1970-01-01 00:00:00 UTC. | expressions.md#unix_date
unnest | daft.functions | (expr: Expression) -> Expression | Flatten the fields of a struct expression into columns in a DataFrame. | functions-etc.md#unnest
unnest | Expression | () -> Expression | Flatten the fields of a struct expression into columns in a DataFrame. | expressions.md#unnest
unpivot | DataFrame | (ids: ManyColumnsInputType, values: ManyColumnsInputType=[], variable_name: str='variable', value_name: str='value') -> DataFrame | Unpivots a DataFrame from wide to long format. | dataframe.md#unpivot
update_deltalake | daft | (table: Union[str, 'UnityCatalogTable', 'deltalake.DeltaTable'], updates: 'Mapping[str, str]', predicate: str | None=None, io_config: IOConfig | None=None, custom_metadata: dict[str, str] | None=None, safe_cast: bool=True) -> dict[str, Any] | Update rows in a Delta Lake table. | toplevel.md#update_deltalake
upload | daft.functions | (expr: Expression, location: str | Expression, max_connections: int=32, on_error: Literal['raise', 'null']='raise', io_config: IOConfig | None=None) -> Expression | Uploads a column of binary data to the provided location(s) (also supports S3, local etc). | functions-media.md#upload
upload | Expression | (location: builtins.str | Expression, max_connections: int=32, on_error: Literal['raise', 'null']='raise', io_config: IOConfig | None=None) -> Expression | Uploads a column of binary data to the provided location(s) (also supports S3, local etc). | expressions.md#upload
upper | daft.functions | (expr: Expression) -> Expression | Convert UTF-8 string to all upper. | functions-str.md#upper
upper | Expression | () -> Expression | Convert UTF-8 string to all upper. | expressions.md#upper
uuid | daft.functions | (version: Literal['v4', 'v7']='v4') -> Expression | Generates a column of UUID strings. | functions-misc.md#uuid
value_counts | daft.functions | (list_expr: Expression) -> Expression | Counts the occurrences of each distinct value in the list. | functions-list.md#value_counts
value_counts | Expression | () -> Expression | Counts the occurrences of each distinct value in the list. | expressions.md#value_counts
var | daft.functions | (expr: Expression, ddof: int=1) -> Expression | Calculates the variance of the values in the expression. | functions-agg.md#var
var | DataFrame | (*cols: ColumnInputType, ddof: int=1) -> DataFrame | Performs a global variance on the DataFrame. | dataframe.md#var
var | Expression | (ddof: int=1) -> Expression | Calculates the variance of the values in the expression. | expressions.md#var
video_file | daft.functions | (url: Expression, verify: bool=False, io_config: IOConfig | None=None) -> Expression | Converts a string containing a file reference to a `daft.VideoFile` reference. | functions-media.md#video_file
video_frames | daft.functions | (file_expr: Expression, *, start_time: float | Expression=0, end_time: float | None | Expression=None, width: int | None=None, height: int | None=None, is_key_frame: bool | None=None, sample_interval_seconds: float | None=None) -> Expression | Decode all video frames within a time range, with per-frame metadata. | functions-media.md#video_frames
video_frames | Expression | (*, start_time: float=0, end_time: float | None=None, width: int | None=None, height: int | None=None, is_key_frame: bool | None=None) -> Expression | Decodes video frames from a video file. | expressions.md#video_frames
video_keyframes | daft.functions | (file_expr: Expression, *, start_time: float=0, end_time: float | None=None) -> Expression | Get keyframes for a video file. | functions-media.md#video_keyframes
video_keyframes | Expression | (*, start_time: float=0, end_time: float | None=None) -> Expression | Gets keyframes for a video file. | expressions.md#video_keyframes
video_metadata | daft.functions | (file_expr: Expression) -> Expression | Get metadata for a video file. | functions-media.md#video_metadata
video_metadata | Expression | () -> Expression | Gets metadata for a video file. | expressions.md#video_metadata
VideoFile | daft | (url: str, io_config: IOConfig | None=None) -> None | A video-specific file interface that provides video operations. | toplevel.md#videofile
week_of_year | daft.functions | (expr: Expression) -> Expression | Retrieves the week of the year for a datetime column. | functions-datetime.md#week_of_year
week_of_year | Expression | () -> Expression | Retrieves the week of the year for a datetime column. | expressions.md#week_of_year
weekofyear | daft.functions | (expr: Expression) -> Expression | Alias for ``week_of_year``. | functions-datetime.md#weekofyear
when | daft.functions | (condition: Expression | bool, then: Expression | Any) -> WhenExpr | Start a conditional expression, similar to SQL CASE WHEN. | functions-misc.md#when
where | DataFrame | (predicate: Expression | str) -> DataFrame | Filters rows via a predicate expression, similar to SQL ``WHERE``. | dataframe.md#where
Window | daft | () -> None | Describes how to partition data and in what order to apply the window function. | toplevel.md#window
with_column | DataFrame | (column_name: str, expr: Expression) -> DataFrame | Adds a column to the current DataFrame with an Expression, equivalent to a ``select`` with all current columns and the new one. | dataframe.md#with_column
with_column_renamed | DataFrame | (existing: str, new: str) -> DataFrame | Renames a column in the current DataFrame. | dataframe.md#with_column_renamed
with_columns | DataFrame | (columns: dict[str, Expression]) -> DataFrame | Adds columns to the current DataFrame with Expressions, equivalent to a ``select`` with all current columns and the new ones. | dataframe.md#with_columns
with_columns_renamed | DataFrame | (cols_map: dict[str, str]) -> DataFrame | Renames multiple columns in the current DataFrame. | dataframe.md#with_columns_renamed
with_spatial_bbox | DataFrame | (geom_col: str) -> DataFrame | Add ``rtree_min_x``, ``rtree_min_y``, ``rtree_max_x``, ``rtree_max_y`` Float64 columns holding the bounding box of ``geom_col``. | dataframe.md#with_spatial_bbox
with_subscriber | daft | (alias: str, subscriber: Subscriber) -> Generator[None, None, None] | Context manager that attaches a subscriber to the current context, and detaches it afterwards. | toplevel.md#with_subscriber
write_bigtable | DataFrame | (project_id: str, instance_id: str, table_id: str, row_key_column: str, column_family_mappings: dict[str, str], client_kwargs: dict[str, Any] | None=None, write_kwargs: dict[str, Any] | None=None, serialize_incompatible_types: bool=True) -> DataFrame | Write a DataFrame into a Google Cloud Bigtable table. | dataframe.md#write_bigtable
write_clickhouse | DataFrame | (table: str, *, host: str, port: int | None=None, user: str | None=None, password: str | None=None, database: str | None=None, client_kwargs: dict[str, Any] | None=None, write_kwargs: dict[str, Any] | None=None) -> DataFrame | Writes the DataFrame to a ClickHouse table. | dataframe.md#write_clickhouse
write_csv | DataFrame | (root_dir: str | pathlib.Path, write_mode: Literal['append', 'overwrite', 'overwrite-partitions']='append', partition_cols: list[ColumnInputType] | None=None, io_config: IOConfig | None=None, delimiter: str | None=None, quote: str | None=None, escape: str | None=None, header: bool | None=True, date_format: str | None=None, timestamp_format: str | None=None) -> DataFrame | Writes the DataFrame as CSV files, returning a new DataFrame with paths to the files that were written. | dataframe.md#write_csv
write_deltalake | DataFrame | (table: Union[str, pathlib.Path, 'deltalake.DeltaTable', 'UnityCatalogTable'], partition_cols: list[str] | None=None, mode: Literal['append', 'overwrite', 'error', 'ignore']='append', schema_mode: Literal['merge', 'overwrite'] | None=None, name: str | None=None, description: str | None=None, configuration: Mapping[str, str | None] | None=None, custom_metadata: dict[str, str] | None=None, dynamo_table_name: str | None=None, allow_unsafe_rename: bool=False, io_config: IOConfig | None=None, checkpoint: 'IdempotentCommit | None'=None, compression: str='snappy') -> DataFrame | Writes the DataFrame to a [Delta Lake](https://docs.delta.io/latest/index.html) table, returning a new DataFrame with the operations that occurred. | dataframe.md#write_deltalake
write_huggingface | DataFrame | (repo: str, split: str='train', data_dir: str='data', revision: str='main', overwrite: bool=False, commit_message: str='Upload dataset using Daft', commit_description: str | None=None, io_config: IOConfig | None=None) -> DataFrame | Write a DataFrame into a Hugging Face dataset. | dataframe.md#write_huggingface
write_iceberg | DataFrame | (table: 'pyiceberg.table.Table', mode: str='append', io_config: IOConfig | None=None, snapshot_properties: dict[str, str] | None=None, checkpoint: 'IdempotentCommit | None'=None) -> DataFrame | Writes the DataFrame to an [Iceberg](https://iceberg.apache.org/docs/nightly/) table, returning a new DataFrame with the operations that occurred. | dataframe.md#write_iceberg
write_json | DataFrame | (root_dir: str | pathlib.Path, write_mode: Literal['append', 'overwrite', 'overwrite-partitions']='append', partition_cols: list[ColumnInputType] | None=None, io_config: IOConfig | None=None, ignore_null_fields: bool | None=False, date_format: str | None=None, timestamp_format: str | None=None) -> DataFrame | Writes the DataFrame as JSON files, returning a new DataFrame with paths to the files that were written. | dataframe.md#write_json
write_lance | DataFrame | (uri: str | pathlib.Path, mode: Literal['create', 'append', 'overwrite', 'merge']='create', io_config: IOConfig | None=None, schema: Union[Schema, 'pyarrow.Schema'] | None=None, left_on: str | None=None, right_on: str | None=None, **kwargs: Any) -> DataFrame | Writes the DataFrame to a Lance table. | dataframe.md#write_lance
write_paimon | DataFrame | (table: 'pypaimon.table.Table', mode: str='append') -> DataFrame | Writes the DataFrame to an Apache Paimon table, returning a summary DataFrame. | dataframe.md#write_paimon
write_parquet | DataFrame | (root_dir: str | pathlib.Path, compression: str='snappy', write_mode: Literal['append', 'overwrite', 'overwrite-partitions']='append', write_success_file: bool=False, partition_cols: list[ColumnInputType] | None=None, io_config: IOConfig | None=None, column_compression: dict[str, str] | None=None, crs: str | None=None, geometry_columns: list[str] | None=None, single_file: bool=False) -> DataFrame | Writes the DataFrame as parquet files, returning a new DataFrame with paths to the files that were written. | dataframe.md#write_parquet
write_sink | DataFrame | (sink: 'DataSink[WriteResultType]') -> DataFrame | Writes the DataFrame to the given DataSink. | dataframe.md#write_sink
write_sql | DataFrame | (table_name: str, conn: str | Callable[[], 'Connection'], write_mode: Literal['append', 'overwrite', 'fail']='append', column_types: dict[str, Any] | None=None, non_primitive_handling: Literal['bytes', 'str', 'error'] | None=None) -> DataFrame | Write the DataFrame to a SQL database and return write metrics. | dataframe.md#write_sql
write_table | daft | (identifier: Identifier | str, df: DataFrame, mode: Literal['append', 'overwrite']='append', **options: Any) -> None | Writes the DataFrame to the table specified with the identifier. | toplevel.md#write_table
write_turbopuffer | DataFrame | (namespace: str | Expression, api_key: str | None=None, region: str | None=None, distance_metric: Literal['cosine_distance', 'euclidean_squared'] | None=None, schema: dict[str, Any] | None=None, id_column: str | None=None, vector_column: str | None=None, client_kwargs: dict[str, Any] | None=None, write_kwargs: dict[str, Any] | None=None) -> DataFrame | Writes the DataFrame to a Turbopuffer namespace. | dataframe.md#write_turbopuffer
year | daft.functions | (expr: Expression) -> Expression | Retrieves the year for a datetime column. | functions-datetime.md#year
year | Expression | () -> Expression | Retrieves the year for a datetime column. | expressions.md#year
