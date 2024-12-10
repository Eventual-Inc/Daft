use color_eyre::Help;
use eyre::{bail, ensure, eyre};
use serde_json::Value;
use spark_connect::data_type::Kind;
use tracing::warn;

#[derive(Debug)]
enum TypeTag {
    Null,
    Binary,
    Boolean,
    Byte,
    Short,
    Integer,
    Long,
    Float,
    Double,
    Decimal,
    String,
    Char,
    VarChar,
    Date,
    Timestamp,
    TimestampNtz,
    CalendarInterval,
    YearMonthInterval,
    DayTimeInterval,
    Array,
    Struct,
    Map,
    Variant,
    Udt,
    Unparsed,
}

fn remove_type(input: &mut serde_json::Map<String, Value>) -> eyre::Result<TypeTag> {
    let Some(r#type) = input.remove("type") else {
        bail!("missing type");
    };

    let Value::String(r#type) = r#type else {
        bail!("expected type to be string; instead got {:?}", r#type);
    };

    let result = match r#type.as_str() {
        "null" => TypeTag::Null,
        "binary" => TypeTag::Binary,
        "boolean" => TypeTag::Boolean,
        "byte" => TypeTag::Byte,
        "short" => TypeTag::Short,
        "integer" => TypeTag::Integer,
        "long" => TypeTag::Long,
        "float" => TypeTag::Float,
        "double" => TypeTag::Double,
        "decimal" => TypeTag::Decimal,
        "string" => TypeTag::String,
        "char" => TypeTag::Char,
        "varchar" => TypeTag::VarChar,
        "date" => TypeTag::Date,
        "timestamp" => TypeTag::Timestamp,
        "timestamp_ntz" => TypeTag::TimestampNtz,
        "calendar_interval" => TypeTag::CalendarInterval,
        "yearmonthinterval" => TypeTag::YearMonthInterval,
        "daytimeinterval" => TypeTag::DayTimeInterval,
        "array" => TypeTag::Array,
        "struct" => TypeTag::Struct,
        "map" => TypeTag::Map,
        "variant" => TypeTag::Variant,
        "udt" => TypeTag::Udt,
        "unparsed" => TypeTag::Unparsed,
        other => bail!("unsupported type: {other}"),
    };

    Ok(result)
}

pub fn deser(value: Value) -> eyre::Result<spark_connect::DataType> {
    let Value::Object(input) = value else {
        bail!("expected object; instead got {:?}", value);
    };

    deser_helper(input)
}

fn deser_helper(
    mut input: serde_json::Map<String, serde_json::Value>,
) -> eyre::Result<spark_connect::DataType> {
    // {"fields":[{"metadata":{},"name":"id","nullable":true,"type":"long"}],"type":"struct"}

    let kind = remove_type(&mut input)?;

    let result = match kind {
        TypeTag::Null => Ok(Kind::Null(spark_connect::data_type::Null {
            type_variation_reference: 0,
        })),
        TypeTag::Binary => Ok(Kind::Binary(spark_connect::data_type::Binary {
            type_variation_reference: 0,
        })),
        TypeTag::Boolean => Ok(Kind::Boolean(spark_connect::data_type::Boolean {
            type_variation_reference: 0,
        })),
        TypeTag::Byte => Ok(Kind::Byte(spark_connect::data_type::Byte {
            type_variation_reference: 0,
        })),
        TypeTag::Short => Ok(Kind::Short(spark_connect::data_type::Short {
            type_variation_reference: 0,
        })),
        TypeTag::Integer => Ok(Kind::Integer(spark_connect::data_type::Integer {
            type_variation_reference: 0,
        })),
        TypeTag::Long => Ok(Kind::Long(spark_connect::data_type::Long {
            type_variation_reference: 0,
        })),
        TypeTag::Float => Ok(Kind::Float(spark_connect::data_type::Float {
            type_variation_reference: 0,
        })),
        TypeTag::Double => Ok(Kind::Double(spark_connect::data_type::Double {
            type_variation_reference: 0,
        })),
        TypeTag::Decimal => Ok(Kind::Decimal(spark_connect::data_type::Decimal {
            scale: None,
            precision: None,
            type_variation_reference: 0,
        })),
        TypeTag::String => Ok(Kind::String(spark_connect::data_type::String {
            type_variation_reference: 0,
            collation: String::new(),
        })),
        TypeTag::Char => Ok(Kind::Char(spark_connect::data_type::Char {
            type_variation_reference: 0,
            length: 1,
        })),
        TypeTag::VarChar => Ok(Kind::VarChar(spark_connect::data_type::VarChar {
            type_variation_reference: 0,
            length: 0,
        })),
        TypeTag::Date => Ok(Kind::Date(spark_connect::data_type::Date {
            type_variation_reference: 0,
        })),
        TypeTag::Timestamp => Ok(Kind::Timestamp(spark_connect::data_type::Timestamp {
            type_variation_reference: 0,
        })),
        TypeTag::TimestampNtz => Ok(Kind::TimestampNtz(spark_connect::data_type::TimestampNtz {
            type_variation_reference: 0,
        })),
        TypeTag::CalendarInterval => Ok(Kind::CalendarInterval(
            spark_connect::data_type::CalendarInterval {
                type_variation_reference: 0,
            },
        )),
        TypeTag::YearMonthInterval => Ok(Kind::YearMonthInterval(
            spark_connect::data_type::YearMonthInterval {
                type_variation_reference: 0,
                start_field: None,
                end_field: None,
            },
        )),
        TypeTag::DayTimeInterval => Ok(Kind::DayTimeInterval(
            spark_connect::data_type::DayTimeInterval {
                type_variation_reference: 0,
                start_field: None,
                end_field: None,
            },
        )),
        TypeTag::Array => Err(eyre!("Array type not supported"))
            .suggestion("Wait until we support arrays in Spark Connect"),
        TypeTag::Struct => deser_struct(input),
        TypeTag::Map => Err(eyre!("Map type not supported"))
            .suggestion("Wait until we support maps in Spark Connect"),
        TypeTag::Variant => Ok(Kind::Variant(spark_connect::data_type::Variant {
            type_variation_reference: 0,
        })),
        TypeTag::Udt => bail!("UDT type not supported"),
        TypeTag::Unparsed => bail!("Unparsed type not supported"),
    }?;

    let result = spark_connect::DataType { kind: Some(result) };

    Ok(result)
}

fn deser_struct(
    mut object: serde_json::Map<String, Value>,
) -> eyre::Result<spark_connect::data_type::Kind> {
    // {"fields":[{"metadata":{},"name":"id","nullable":true,"type":"long"}]}

    let Some(fields) = object.remove("fields") else {
        bail!("missing fields");
    };

    ensure!(object.is_empty(), "unexpected fields: {object:?}");

    let Value::Array(fields) = fields else {
        bail!("expected fields to be array");
    };

    let fields: Vec<_> = fields.into_iter().map(deser_struct_field).try_collect()?;

    Ok(Kind::Struct(spark_connect::data_type::Struct {
        fields,
        type_variation_reference: 0,
    }))
}

fn deser_struct_field(
    field: serde_json::Value,
) -> eyre::Result<spark_connect::data_type::StructField> {
    // {"metadata":{},"name":"id","nullable":true,"type":"long"}

    let Value::Object(mut object) = field else {
        bail!("expected object");
    };

    let Some(metadata) = object.remove("metadata") else {
        bail!("missing metadata");
    };

    warn!("ignoring metadata: {metadata:?}");

    let Some(name) = object.remove("name") else {
        bail!("missing name");
    };

    let Value::String(name) = name else {
        bail!("expected name to be string; instead got {:?}", name);
    };

    let Some(nullable) = object.remove("nullable") else {
        bail!("missing nullable");
    };

    let Value::Bool(nullable) = nullable else {
        bail!("expected nullable to be bool; instead got {:?}", nullable);
    };

    let inner = deser_helper(object)?;

    Ok(spark_connect::data_type::StructField {
        name,
        data_type: Some(inner),
        nullable,
        metadata: None,
    })
}
