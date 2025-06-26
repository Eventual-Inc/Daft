use std::collections::HashMap;

use daft_core::prelude::{DataType, Field, Schema, TimeUnit};
use sqlparser::{
    ast::{ArrayElemTypeDef, ExactNumberInfo, StructField, TimezoneInfo},
    dialect::GenericDialect,
    parser::{Parser, ParserOptions},
    tokenizer::Tokenizer,
};

use crate::{
    error::{PlannerError, SQLPlannerResult},
    invalid_operation_err, unsupported_sql_err,
};

/// Parses a SQL string as a daft DataType
pub fn try_parse_dtype<S: AsRef<str>>(s: S) -> SQLPlannerResult<DataType> {
    let tokens = Tokenizer::new(&GenericDialect {}, s.as_ref()).tokenize()?;
    let mut parser = Parser::new(&GenericDialect {})
        .with_options(ParserOptions {
            trailing_commas: true,
            ..Default::default()
        })
        .with_tokens(tokens);
    let dtype = parser.parse_data_type()?;
    sql_dtype_to_dtype(&dtype)
}

/// Parses a SQL map of name-type pairs into a new Schema
pub(crate) fn try_parse_schema(schema: HashMap<String, String>) -> SQLPlannerResult<Schema> {
    let mut fields = vec![];
    for (name, dtype_str) in schema {
        let dtype = try_parse_dtype(dtype_str)?;
        fields.push(Field::new(name, dtype));
    }
    Ok(Schema::new(fields))
}

/// Converts a sqlparser DataType to a daft DataType
pub(crate) fn sql_dtype_to_dtype(dtype: &sqlparser::ast::DataType) -> SQLPlannerResult<DataType> {
    use sqlparser::ast::DataType as SQLDataType;
    macro_rules! use_instead {
        ($dtype:expr, $($expected:expr),*) => {
            unsupported_sql_err!(
                "`{dtype}` is not supported, instead try using {expected}",
                dtype = $dtype,
                expected = format!($($expected),*)
            )
        };
    }

    Ok(match dtype {
        // ---------------------------------
        // array/list
        // ---------------------------------
        SQLDataType::Array(ArrayElemTypeDef::AngleBracket(_)) => use_instead!(dtype, "array[..]"),
        SQLDataType::Array(ArrayElemTypeDef::SquareBracket(inner_type, None)) => {
            DataType::List(Box::new(sql_dtype_to_dtype(inner_type)?))
        }
        SQLDataType::Array(ArrayElemTypeDef::SquareBracket(inner_type, Some(size))) => {
            DataType::FixedSizeList(
                Box::new(sql_dtype_to_dtype(inner_type)?),
                *size as usize,
            )
        }

        // ---------------------------------
        // binary
        // ---------------------------------
        SQLDataType::Bytea
        | SQLDataType::Blob(_)
        | SQLDataType::Varbinary(_) => use_instead!(dtype, "`binary` or `bytes`"),
        SQLDataType::Binary(None) | SQLDataType::Bytes(None) => DataType::Binary,
        SQLDataType::Binary(Some(n_bytes)) | SQLDataType::Bytes(Some(n_bytes)) => DataType::FixedSizeBinary(*n_bytes as usize),

        // ---------------------------------
        // boolean
        // ---------------------------------
        SQLDataType::Boolean | SQLDataType::Bool => DataType::Boolean,
        // ---------------------------------
        // signed integer
        // ---------------------------------
        SQLDataType::Int2(_) => use_instead!(dtype, "`int16` or `smallint`"),
        SQLDataType::Int4(_) | SQLDataType::MediumInt(_)  => use_instead!(dtype, "`int32`, `integer`, or `int`"),
        SQLDataType::Int8(_) => use_instead!(dtype, "`int64` or `bigint` for 64-bit integer, or `tinyint` for 8-bit integer"),
        SQLDataType::TinyInt(_) => DataType::Int8,
        SQLDataType::SmallInt(_) | SQLDataType::Int16 => DataType::Int16,
        SQLDataType::Int(_) | SQLDataType::Integer(_) | SQLDataType::Int32  => DataType::Int32,
        SQLDataType::BigInt(_) | SQLDataType::Int64 => DataType::Int64,

        // ---------------------------------
        // unsigned integer
        // ---------------------------------
        SQLDataType::UnsignedInt2(_) => use_instead!(dtype, "`smallint unsigned` or `uint16`"),
        SQLDataType::UnsignedInt4(_) | SQLDataType::UnsignedMediumInt(_) => use_instead!(dtype, "`int unsigned` or `uint32`"),
        SQLDataType::UnsignedInt8(_) => use_instead!(dtype, "`bigint unsigned` or `uint64` for 64-bit unsigned integer, or `unsigned tinyint` for 8-bit unsigned integer"),
        SQLDataType::UnsignedTinyInt(_) => DataType::UInt8,
        SQLDataType::UnsignedSmallInt(_) | SQLDataType::UInt16 => DataType::UInt16,
        SQLDataType::UnsignedInt(_) | SQLDataType::UnsignedInteger(_) | SQLDataType::UInt32 => DataType::UInt32,
        SQLDataType::UnsignedBigInt(_) | SQLDataType::UInt64 => DataType::UInt64,
        // ---------------------------------
        // float
        // ---------------------------------
        SQLDataType::Float4 => use_instead!(dtype, "`float32` or `real`"),
        SQLDataType::Float8 => use_instead!(dtype, "`float64` or `double`"),
        SQLDataType::Double | SQLDataType::DoublePrecision | SQLDataType::Float64 => {
            DataType::Float64
        }
        SQLDataType::Float(n_bytes) => match n_bytes {
            Some(n) if (1u64..=24u64).contains(n) => DataType::Float32,
            Some(n) if (25u64..=53u64).contains(n) => DataType::Float64,
            Some(n) => {
                unsupported_sql_err!(
                    "unsupported `float` size (expected a value between 1 and 53, found {})",
                    n
                )
            }
            None => DataType::Float64,
        },
        SQLDataType::Real | SQLDataType::Float32 => DataType::Float32,

        // ---------------------------------
        // decimal
        // ---------------------------------
        SQLDataType::Dec(info) | SQLDataType::Numeric(info) |SQLDataType::Decimal(info) => match *info {
            ExactNumberInfo::PrecisionAndScale(p, s) => {
                DataType::Decimal128(p as usize, s as usize)
            }
            ExactNumberInfo::Precision(p) => DataType::Decimal128(p as usize, 0),
            ExactNumberInfo::None => DataType::Decimal128(38, 9),
        },
        // ---------------------------------
        // temporal
        // ---------------------------------
        SQLDataType::Date => DataType::Date,
        SQLDataType::Interval => DataType::Interval,
        SQLDataType::Time(precision, tz) => match tz {
            TimezoneInfo::None => DataType::Time(timeunit_from_precision(*precision)?),
            _ => unsupported_sql_err!("`time` with timezone is; found tz={}", tz),
        },
        SQLDataType::Datetime(_) => unsupported_sql_err!("`datetime` is not supported"),
        SQLDataType::Timestamp(prec, tz) => match tz {
            TimezoneInfo::None => {
                DataType::Timestamp(timeunit_from_precision(*prec)?, None)
            }
            _ => unsupported_sql_err!("`timestamp` with timezone"),
        },
        // ---------------------------------
        // string
        // ---------------------------------
        SQLDataType::Char(_)
        | SQLDataType::CharVarying(_)
        | SQLDataType::Character(_)
        | SQLDataType::CharacterVarying(_)
        | SQLDataType::Clob(_) => use_instead!(dtype, "`string`, `text`, or `varchar`"),
        SQLDataType::String(_) | SQLDataType::Text | SQLDataType::Varchar(_) => DataType::Utf8,
        // ---------------------------------
        // struct
        // ---------------------------------
        SQLDataType::Struct(fields, _brackets) => {
            // TODO: https://github.com/Eventual-Inc/Daft/issues/4448
            // if matches!(brackets, StructBracketKind::AngleBrackets) {
            //     use_instead!(dtype, "STRUCT(fields...)")
            // }
            let fields = fields
                .iter()
                .enumerate()
                .map(
                    |(
                        idx,
                        StructField {
                            field_name,
                            field_type,
                        },
                    )| {
                        let dtype = sql_dtype_to_dtype(field_type)?;
                        let name = match field_name {
                            Some(name) => name.to_string(),
                            None => format!("col_{idx}"),
                        };

                        Ok(Field::new(name, dtype))
                    },
                )
                .collect::<SQLPlannerResult<Vec<_>>>()?;
            DataType::Struct(fields)
        }
        SQLDataType::Custom(name, properties) => match name.to_string().to_lowercase().as_str() {
            "tensor" => match properties.as_slice() {
                [] => invalid_operation_err!("must specify inner datatype with 'tensor'. ex: `tensor(int)` or `tensor(int, 10, 10, 10)`"),
                [inner_dtype] => {
                    let inner_dtype = try_parse_dtype(inner_dtype)?;
                    DataType::Tensor(Box::new(inner_dtype))
                }
                [inner_dtype, rest @ ..] => {
                    let inner_dtype = try_parse_dtype(inner_dtype)?;
                    let rest = rest
                        .iter()
                        .map(|p| {
                            p.parse().map_err(|_| {
                                PlannerError::invalid_operation(
                                    "invalid tensor shape".to_string(),
                                )
                            })
                        })
                        .collect::<SQLPlannerResult<Vec<_>>>()?;
                    DataType::FixedShapeTensor(Box::new(inner_dtype), rest)
                }
            },
            "image" => match properties.as_slice() {
                [] => DataType::Image(None),
                [mode] => {
                    let mode = mode.parse().map_err(|_| {
                        PlannerError::invalid_operation("invalid image mode".to_string())
                    })?;
                    DataType::Image(Some(mode))

                },
                [mode, height, width] => {
                    let mode = mode.parse().map_err(|_| {
                        PlannerError::invalid_operation("invalid image mode".to_string())
                    })?;
                    let height = height.parse().map_err(|_| {
                        PlannerError::invalid_operation("invalid image height".to_string())
                    })?;
                    let width = width.parse().map_err(|_| {
                        PlannerError::invalid_operation("invalid image width".to_string())
                    })?;
                    DataType::FixedShapeImage(mode, height, width)
                }
                _ => invalid_operation_err!("invalid image properties"),
            },
            "embedding" => match properties.as_slice() {
                [inner_dtype, size] => {
                    let inner_dtype = try_parse_dtype(inner_dtype)?;
                    let Ok(size) = size.parse() else {
                        invalid_operation_err!("invalid embedding size, expected an integer")
                    };
                    DataType::Embedding(Box::new(inner_dtype), size)
                }
                _ => invalid_operation_err!(
                    "embedding must have datatype and size: ex: `embedding(int, 10)`"
                ),
            },
            other => unsupported_sql_err!("custom data type: {other}"),
        },
        other => unsupported_sql_err!("data type: {:?}", other),
    })
}

pub(crate) fn timeunit_from_precision(prec: Option<u64>) -> SQLPlannerResult<TimeUnit> {
    Ok(match prec {
        None => TimeUnit::Microseconds,
        Some(n) if (1u64..=3u64).contains(&n) => TimeUnit::Milliseconds,
        Some(n) if (4u64..=6u64).contains(&n) => TimeUnit::Microseconds,
        Some(n) if (7u64..=9u64).contains(&n) => TimeUnit::Nanoseconds,
        Some(n) => {
            unsupported_sql_err!(
                "invalid temporal type precision (expected 1-9, found {})",
                n
            )
        }
    })
}

#[cfg(test)]
mod test {
    use daft_core::prelude::{DataType, Field, ImageMode};
    use rstest::rstest;

    #[rstest]
    #[case("bool", DataType::Boolean)]
    #[case("Bool", DataType::Boolean)] // case insensitive
    #[case("BOOL", DataType::Boolean)] // case insensitive
    #[case("boolean", DataType::Boolean)]
    #[case("BOOLEAN", DataType::Boolean)] // case insensitive
    #[case("int16", DataType::Int16)]
    #[case("int", DataType::Int32)]
    #[case("integer", DataType::Int32)]
    #[case("int32", DataType::Int32)]
    #[case("int64", DataType::Int64)]
    #[case("uint16", DataType::UInt16)]
    #[case("integer unsigned", DataType::UInt32)]
    #[case("int unsigned", DataType::UInt32)]
    #[case("uint32", DataType::UInt32)]
    #[case("uint64", DataType::UInt64)]
    #[case("float32", DataType::Float32)]
    #[case("real", DataType::Float32)]
    #[case("float64", DataType::Float64)]
    #[case("double", DataType::Float64)]
    #[case("double precision", DataType::Float64)]
    #[case("float", DataType::Float64)]
    #[case("float(1)", DataType::Float32)]
    #[case("float(24)", DataType::Float32)]
    #[case("float(25)", DataType::Float64)]
    #[case("float(53)", DataType::Float64)]
    #[case("dec", DataType::Decimal128(38, 9))]
    #[case("decimal", DataType::Decimal128(38, 9))]
    #[case("decimal(10)", DataType::Decimal128(10, 0))]
    #[case("decimal(10, 2)", DataType::Decimal128(10, 2))]
    #[case("decimal(38, 9)", DataType::Decimal128(38, 9))]
    #[case("numeric", DataType::Decimal128(38, 9))]
    #[case("numeric(10)", DataType::Decimal128(10, 0))]
    #[case("numeric(10, 2)", DataType::Decimal128(10, 2))]
    #[case("numeric(38, 9)", DataType::Decimal128(38, 9))]
    #[case("date", DataType::Date)]
    #[case("tensor(float)", DataType::Tensor(Box::new(DataType::Float64)))]
    #[case("tensor(float, 10, 10, 10)", DataType::FixedShapeTensor(Box::new(DataType::Float64), vec![10, 10, 10]))]
    #[case("image", DataType::Image(None))]
    #[case("image(RGB)", DataType::Image(Some(ImageMode::RGB)))]
    #[case("image(RGBA)", DataType::Image(Some(ImageMode::RGBA)))]
    #[case("image(L)", DataType::Image(Some(ImageMode::L)))]
    #[case("imAgE(L, 10, 10)", DataType::FixedShapeImage(ImageMode::L, 10, 10))]
    #[case(
        "embedding(int, 10)",
        DataType::Embedding(Box::new(DataType::Int32), 10)
    )]
    #[case(
        "EMBEDDING(iNt, 10)", // case insensitive
        DataType::Embedding(Box::new(DataType::Int32), 10)
    )]
    #[case(
        "tensor(int, 10, 10, 10)[10]",
        DataType::FixedSizeList(Box::new(DataType::FixedShapeTensor(
            Box::new(DataType::Int32),
            vec![10, 10, 10]
        )), 10)
    )]
    #[case("int[]", DataType::List(Box::new(DataType::Int32)))]
    #[case("int[10]", DataType::FixedSizeList(Box::new(DataType::Int32), 10))]
    #[case(
        "int[10][10]",
        DataType::FixedSizeList(
            Box::new(DataType::FixedSizeList(Box::new(DataType::Int32), 10)),
            10
        )
    )]
    // TODO: https://github.com/Eventual-Inc/Daft/issues/4448
    // #[case(
    //     "struct(a bool, b int, c string)",
    //     DataType::Struct(vec![
    //         Field::new("a", DataType::Boolean),
    //         Field::new("b", DataType::Int32),
    //         Field::new("c", DataType::Utf8),
    //     ])
    // )]
    fn test_sql_datatype(#[case] sql: &str, #[case] expected: DataType) {
        let result = super::try_parse_dtype(sql).unwrap();
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case(
        "tensor",
        "must specify inner datatype with 'tensor'. ex: `tensor(int)` or `tensor(int, 10, 10, 10)`"
    )]
    #[case(
        "tensor()",
        "must specify inner datatype with 'tensor'. ex: `tensor(int)` or `tensor(int, 10, 10, 10)`"
    )]
    #[case("tensor(int, int)", "invalid tensor shape")]
    #[case("image(RGB, 10)", "invalid image properties")]
    #[case("image(RGB, 10, 10, 10)", "invalid image properties")]
    #[case("image(10)", "invalid image mode")]
    #[case("image(RGBBB)", "invalid image mode")]
    #[case(
        "embedding(int)",
        "embedding must have datatype and size: ex: `embedding(int, 10)`"
    )]
    #[case(
        "embedding()",
        "embedding must have datatype and size: ex: `embedding(int, 10)`"
    )]
    #[case(
        "embedding",
        "embedding must have datatype and size: ex: `embedding(int, 10)`"
    )]
    #[case("embedding(int, 1.11)", "invalid embedding size, expected an integer")]
    fn test_custom_datatype_err(#[case] sql: &str, #[case] expected: &str) {
        let result = super::try_parse_dtype(sql).unwrap_err().to_string();
        let e = format!("Invalid operation: {}", expected);
        assert_eq!(result, e);
    }

    #[rstest]
    #[case("array<int>", "array[..]")]
    #[case("bytea", "`binary` or `bytes`")]
    #[case("blob", "`binary` or `bytes`")]
    #[case("varbinary", "`binary` or `bytes`")]
    #[case("int2", "`int16` or `smallint`")]
    #[case("int4", "`int32`, `integer`, or `int`")]
    #[case("mediumint", "`int32`, `integer`, or `int`")]
    #[case(
        "int8",
        "`int64` or `bigint` for 64-bit integer, or `tinyint` for 8-bit integer"
    )]
    #[case("int2 unsigned", "`smallint unsigned` or `uint16`")]
    #[case("int4 unsigned", "`int unsigned` or `uint32`")]
    #[case("mediumint unsigned", "`int unsigned` or `uint32`")]
    #[case("int8 unsigned", "`bigint unsigned` or `uint64` for 64-bit unsigned integer, or `unsigned tinyint` for 8-bit unsigned integer")]
    #[case("float4", "`float32` or `real`")]
    #[case("char", "`string`, `text`, or `varchar`")]
    #[case("char varying", "`string`, `text`, or `varchar`")]
    #[case("character", "`string`, `text`, or `varchar`")]
    #[case("character varying", "`string`, `text`, or `varchar`")]
    #[case("clob", "`string`, `text`, or `varchar`")]
    fn test_sql_datatype_use_instead(#[case] sql: &str, #[case] expected: &str) {
        let result = super::try_parse_dtype(sql).unwrap_err().to_string();
        let e = format!(
            "Unsupported SQL: '`{}` is not supported, instead try using {expected}'",
            sql.to_ascii_uppercase()
        );
        assert_eq!(result, e);
    }
}
