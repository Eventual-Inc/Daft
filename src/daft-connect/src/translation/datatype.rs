use daft_schema::{dtype::DataType, field::Field, time_unit::TimeUnit};
use eyre::{bail, ensure, WrapErr};
use spark_connect::data_type::Kind;
use tracing::warn;

pub fn to_spark_datatype(datatype: &DataType) -> spark_connect::DataType {
    match datatype {
        DataType::Null => spark_connect::DataType {
            kind: Some(Kind::Null(spark_connect::data_type::Null {
                type_variation_reference: 0,
            })),
        },
        DataType::Boolean => spark_connect::DataType {
            kind: Some(Kind::Boolean(spark_connect::data_type::Boolean {
                type_variation_reference: 0,
            })),
        },
        DataType::Int8 => spark_connect::DataType {
            kind: Some(Kind::Byte(spark_connect::data_type::Byte {
                type_variation_reference: 0,
            })),
        },
        DataType::Int16 => spark_connect::DataType {
            kind: Some(Kind::Short(spark_connect::data_type::Short {
                type_variation_reference: 0,
            })),
        },
        DataType::Int32 => spark_connect::DataType {
            kind: Some(Kind::Integer(spark_connect::data_type::Integer {
                type_variation_reference: 0,
            })),
        },
        DataType::Int64 => spark_connect::DataType {
            kind: Some(Kind::Long(spark_connect::data_type::Long {
                type_variation_reference: 0,
            })),
        },
        DataType::UInt8 => spark_connect::DataType {
            kind: Some(Kind::Byte(spark_connect::data_type::Byte {
                type_variation_reference: 0,
            })),
        },
        DataType::UInt16 => spark_connect::DataType {
            kind: Some(Kind::Short(spark_connect::data_type::Short {
                type_variation_reference: 0,
            })),
        },
        DataType::UInt32 => spark_connect::DataType {
            kind: Some(Kind::Integer(spark_connect::data_type::Integer {
                type_variation_reference: 0,
            })),
        },
        DataType::UInt64 => spark_connect::DataType {
            kind: Some(Kind::Long(spark_connect::data_type::Long {
                type_variation_reference: 0,
            })),
        },
        DataType::Float32 => spark_connect::DataType {
            kind: Some(Kind::Float(spark_connect::data_type::Float {
                type_variation_reference: 0,
            })),
        },
        DataType::Float64 => spark_connect::DataType {
            kind: Some(Kind::Double(spark_connect::data_type::Double {
                type_variation_reference: 0,
            })),
        },
        DataType::Decimal128(precision, scale) => spark_connect::DataType {
            kind: Some(Kind::Decimal(spark_connect::data_type::Decimal {
                scale: Some(*scale as i32),
                precision: Some(*precision as i32),
                type_variation_reference: 0,
            })),
        },
        DataType::Timestamp(unit, _) => {
            warn!("Ignoring time unit {unit:?} for timestamp type");
            spark_connect::DataType {
                kind: Some(Kind::Timestamp(spark_connect::data_type::Timestamp {
                    type_variation_reference: 0,
                })),
            }
        }
        DataType::Date => spark_connect::DataType {
            kind: Some(Kind::Date(spark_connect::data_type::Date {
                type_variation_reference: 0,
            })),
        },
        DataType::Binary => spark_connect::DataType {
            kind: Some(Kind::Binary(spark_connect::data_type::Binary {
                type_variation_reference: 0,
            })),
        },
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
pub fn to_daft_datatype(datatype: &spark_connect::DataType) -> eyre::Result<DataType> {
    let Some(kind) = &datatype.kind else {
        bail!("Datatype is required");
    };

    let type_variation_err = "Custom type variation reference not supported";

    match kind {
        Kind::Null(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            Ok(DataType::Null)
        }
        Kind::Binary(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            Ok(DataType::Binary)
        }
        Kind::Boolean(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            Ok(DataType::Boolean)
        }
        Kind::Byte(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            Ok(DataType::Int8)
        }
        Kind::Short(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            Ok(DataType::Int16)
        }
        Kind::Integer(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            Ok(DataType::Int32)
        }
        Kind::Long(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            Ok(DataType::Int64)
        }
        Kind::Float(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            Ok(DataType::Float32)
        }
        Kind::Double(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            Ok(DataType::Float64)
        }
        Kind::Decimal(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);

            let Some(precision) = value.precision else {
                bail!("Decimal precision is required");
            };

            let Some(scale) = value.scale else {
                bail!("Decimal scale is required");
            };

            let precision = usize::try_from(precision)
                .wrap_err("Decimal precision must be a non-negative integer")?;

            let scale =
                usize::try_from(scale).wrap_err("Decimal scale must be a non-negative integer")?;

            Ok(DataType::Decimal128(precision, scale))
        }
        Kind::String(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            Ok(DataType::Utf8)
        }
        Kind::Char(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            Ok(DataType::Utf8)
        }
        Kind::VarChar(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            Ok(DataType::Utf8)
        }
        Kind::Date(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            Ok(DataType::Date)
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
        Kind::CalendarInterval(_) => bail!("Calendar interval type not supported"),
        Kind::YearMonthInterval(_) => bail!("Year-month interval type not supported"),
        Kind::DayTimeInterval(_) => bail!("Day-time interval type not supported"),
        Kind::Array(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            let element_type = to_daft_datatype(
                value
                    .element_type
                    .as_ref()
                    .ok_or_else(|| eyre::eyre!("Array element type is required"))?,
            )?;
            Ok(DataType::List(Box::new(element_type)))
        }
        Kind::Struct(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            let fields = value
                .fields
                .iter()
                .map(|f| {
                    let field_type = to_daft_datatype(
                        f.data_type
                            .as_ref()
                            .ok_or_else(|| eyre::eyre!("Struct field type is required"))?,
                    )?;
                    Ok(Field::new(&f.name, field_type))
                })
                .collect::<eyre::Result<Vec<_>>>()?;
            Ok(DataType::Struct(fields))
        }
        Kind::Map(value) => {
            ensure!(value.type_variation_reference == 0, type_variation_err);
            let key_type = to_daft_datatype(
                value
                    .key_type
                    .as_ref()
                    .ok_or_else(|| eyre::eyre!("Map key type is required"))?,
            )?;
            let value_type = to_daft_datatype(
                value
                    .value_type
                    .as_ref()
                    .ok_or_else(|| eyre::eyre!("Map value type is required"))?,
            )?;

            let map = DataType::Map {
                key: Box::new(key_type),
                value: Box::new(value_type),
            };

            Ok(map)
        }
        Kind::Variant(_) => bail!("Variant type not supported"),
        Kind::Udt(_) => bail!("User-defined type not supported"),
        Kind::Unparsed(_) => bail!("Unparsed type not supported"),
    }
}
