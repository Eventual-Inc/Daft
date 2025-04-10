# AWS Glue

## Type System

| Glue Type   | Daft Type                                                                                                     |
|-------------|---------------------------------------------------------------------------------------------------------------|
| `boolean`   | [`daft.DataType.bool()`](../api_docs/datatype.html)                                                           |
| `byte`      | [`daft.DataType.int8()`](../api_docs/datatype.html#daft.DataType.int8)                                        |
| `short`     | [`daft.DataType.int16()`](../api_docs/datatype.html#daft.DataType.int16)                                      |
| `integer`   | [`daft.DataType.int32()`](../api_docs/datatype.html#daft.DataType.int32)                                      |
| `long`      | [`daft.DataType.int64()`](../api_docs/datatype.html#daft.DataType.int64)                                      |
| `float`     | [`daft.DataType.float32()`](../api_docs/datatype.html#daft.DataType.float32)                                  |
| `double`    | [`daft.DataType.float64()`](../api_docs/datatype.html#daft.DataType.float64)                                  |
| `decimal`   | [`daft.DataType.decimal128(precision=38, scale=18)`](../api_docs/datatype.html#daft.DataType.decimal128)      |
| `string`    | [`daft.DataType.string()`](../api_docs/datatype.html#daft.DataType.string)                                    |
| `timestamp` | [`daft.DataType.timestamp(timeunit="us", timezone="UTC")`](../api_docs/datatype.html#daft.DataType.timestamp) |
| `date`      | [`daft.DataType.date()`](../api_docs/datatype.html#daft.DataType.date)                                        |

References:

* https://docs.aws.amazon.com/glue/latest/dg/glue-types.html
* https://github.com/awslabs/aws-glue-libs/blob/master/awsglue/gluetypes.py
* https://cwiki.apache.org/confluence/display/hive/languagemanual+types
