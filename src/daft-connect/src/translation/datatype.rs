use daft_schema::dtype::DataType;
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
