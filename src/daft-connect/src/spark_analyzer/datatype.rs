use daft_schema::{dtype::DataType, field::Field, time_unit::TimeUnit};
use spark_connect::data_type::Kind;
use tracing::debug;

use crate::{
    ensure,
    error::{ConnectError, ConnectResult, Context},
    invalid_argument_err, not_yet_implemented,
};

pub fn to_spark_datatype(datatype: &DataType) -> spark_connect::DataType {
    macro_rules! simple_spark_type {
        ($kind:ident) => {
            spark_connect::DataType {
                kind: Some(Kind::$kind(spark_connect::data_type::$kind {
                    type_variation_reference: 0,
                })),
            }
        };
    }
    match datatype {
        DataType::Null => simple_spark_type!(Null),
        DataType::Boolean => simple_spark_type!(Boolean),
        DataType::Int8 => simple_spark_type!(Byte),
        DataType::Int16 => simple_spark_type!(Short),
        DataType::Int32 => simple_spark_type!(Integer),
        DataType::Int64 => simple_spark_type!(Long),
        DataType::UInt8 => simple_spark_type!(Byte),
        DataType::UInt16 => simple_spark_type!(Short),
        DataType::UInt32 => simple_spark_type!(Integer),
        DataType::UInt64 => simple_spark_type!(Long),
        DataType::Float32 => simple_spark_type!(Float),
        DataType::Float64 => simple_spark_type!(Double),
        DataType::Decimal128(precision, scale) => spark_connect::DataType {
            kind: Some(Kind::Decimal(spark_connect::data_type::Decimal {
                scale: Some(*scale as i32),
                precision: Some(*precision as i32),
                type_variation_reference: 0,
            })),
        },
        DataType::Timestamp(unit, _) => {
            debug!("Ignoring time unit {unit:?} for timestamp type");
            spark_connect::DataType {
                kind: Some(Kind::Timestamp(spark_connect::data_type::Timestamp {
                    type_variation_reference: 0,
                })),
            }
        }
        DataType::Date => simple_spark_type!(Date),
        DataType::Binary => simple_spark_type!(Binary),
        DataType::Utf8 => spark_connect::DataType {
            kind: Some(Kind::String(spark_connect::data_type::String {
                type_variation_reference: 0,
                collation: String::new(), // todo(correctness): is this correct?
            })),
        },
        DataType::Struct(fields) => spark_connect::DataType {
            kind: Some(Kind::Struct(spark_connect::data_type::Struct {
                fields: fields
                    .iter()
                    .map(|f| spark_connect::data_type::StructField {
                        name: f.name.clone(),
                        data_type: Some(to_spark_datatype(&f.dtype)),
                        nullable: true, // todo(correctness): is this correct?
                        metadata: None,
                    })
                    .collect(),
                type_variation_reference: 0,
            })),
        },
        _ => unimplemented!("Unsupported datatype: {datatype:?}"),
    }
}

// todo(test): add tests for this esp in Python
pub fn to_daft_datatype(datatype: &spark_connect::DataType) -> ConnectResult<DataType> {
    let Some(kind) = &datatype.kind else {
        invalid_argument_err!("DataType kind is required");
    };

    let type_variation_err = "Custom type variation reference not supported";

    macro_rules! simple_type_case {
        ($value:expr, $dtype:expr) => {{
            ensure!($value.type_variation_reference == 0, type_variation_err);
            Ok($dtype)
        }};
    }

    match kind {
        Kind::Null(value) => {
            simple_type_case!(value, DataType::Null)
        }
        Kind::Binary(value) => {
            simple_type_case!(value, DataType::Binary)
        }
        Kind::Boolean(value) => {
            simple_type_case!(value, DataType::Boolean)
        }
        Kind::Byte(value) => {
            simple_type_case!(value, DataType::Int8)
        }
        Kind::Short(value) => {
            simple_type_case!(value, DataType::Int16)
        }
        Kind::Integer(value) => {
            simple_type_case!(value, DataType::Int32)
        }
        Kind::Long(value) => {
            simple_type_case!(value, DataType::Int64)
        }
        Kind::Float(value) => {
            simple_type_case!(value, DataType::Float32)
        }
        Kind::Double(value) => {
            simple_type_case!(value, DataType::Float64)
        }
        Kind::Decimal(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);

            let Some(precision) = value.precision else {
                invalid_argument_err!("Decimal precision is required");
            };

            let Some(scale) = value.scale else {
                invalid_argument_err!("Decimal scale is required");
            };

            let precision = usize::try_from(precision)
                .wrap_err("Decimal precision must be a non-negative integer")?;

            let scale =
                usize::try_from(scale).wrap_err("Decimal scale must be a non-negative integer")?;

            Ok(DataType::Decimal128(precision, scale))
        }
        Kind::String(value) => {
            simple_type_case!(value, DataType::Utf8)
        }
        Kind::Char(value) => {
            simple_type_case!(value, DataType::Utf8)
        }
        Kind::VarChar(value) => {
            simple_type_case!(value, DataType::Utf8)
        }
        Kind::Date(value) => {
            simple_type_case!(value, DataType::Date)
        }
        Kind::Timestamp(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            // Using microseconds precision with no timezone info matches Spark's behavior.
            // Spark handles timezones at the session level rather than in the type itself.
            // See: https://www.databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html
            Ok(DataType::Timestamp(TimeUnit::Microseconds, None))
        }
        Kind::TimestampNtz(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            Ok(DataType::Timestamp(TimeUnit::Microseconds, None))
        }
        Kind::CalendarInterval(_) => not_yet_implemented!("Calendar interval type not supported"),
        Kind::YearMonthInterval(_) => {
            not_yet_implemented!("Year-month interval type not supported")
        }
        Kind::DayTimeInterval(_) => not_yet_implemented!("Day-time interval type not supported"),
        Kind::Array(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            let element_type = to_daft_datatype(
                value
                    .element_type
                    .as_ref()
                    .ok_or_else(|| ConnectError::invalid_argument("Array element type"))?,
            )?;
            Ok(DataType::List(Box::new(element_type)))
        }
        Kind::Struct(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            let fields = value
                .fields
                .iter()
                .map(|f| {
                    let field_type = to_daft_datatype(f.data_type.as_ref().ok_or_else(|| {
                        ConnectError::invalid_argument("Struct field type is required")
                    })?)?;
                    Ok(Field::new(&f.name, field_type))
                })
                .collect::<ConnectResult<Vec<_>>>()?;
            Ok(DataType::Struct(fields))
        }
        Kind::Map(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            let key_type = to_daft_datatype(
                value
                    .key_type
                    .as_ref()
                    .ok_or_else(|| ConnectError::invalid_argument("Map key type is required"))?,
            )?;
            let value_type =
                to_daft_datatype(value.value_type.as_ref().ok_or_else(|| {
                    ConnectError::invalid_argument("Map value type is required")
                })?)?;

            let map = DataType::Map {
                key: Box::new(key_type),
                value: Box::new(value_type),
            };

            Ok(map)
        }
        Kind::Variant(_) => not_yet_implemented!("Variant type not supported"),
        Kind::Udt(_) => not_yet_implemented!("User-defined type not supported"),
        Kind::Unparsed(_) => not_yet_implemented!("Unparsed type not supported"),
    }
}
