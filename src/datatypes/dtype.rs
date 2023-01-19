use std::sync::Arc;

use arrow2::datatypes::DataType as ArrowType;

use crate::{
    datatypes::{field::Field, time_unit::TimeUnit},
    error::{DaftError, DaftResult},
};

pub type TimeZone = String;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataType {
    // Start ArrowTypes
    /// Null type
    Null,
    /// `true` and `false`.
    Boolean,
    /// An [`i8`]
    Int8,
    /// An [`i16`]
    Int16,
    /// An [`i32`]
    Int32,
    /// An [`i64`]
    Int64,
    /// An [`u8`]
    UInt8,
    /// An [`u16`]
    UInt16,
    /// An [`u32`]
    UInt32,
    /// An [`u64`]
    UInt64,
    /// An 16-bit float
    Float16,
    /// A [`f32`]
    Float32,
    /// A [`f64`]
    Float64,
    /// A [`i64`] representing a timestamp measured in [`TimeUnit`] with an optional timezone.
    ///
    /// Time is measured as a Unix epoch, counting the seconds from
    /// 00:00:00.000 on 1 January 1970, excluding leap seconds,
    /// as a 64-bit signed integer.
    ///
    /// The time zone is a string indicating the name of a time zone, one of:
    ///
    /// * As used in the Olson time zone database (the "tz database" or
    ///   "tzdata"), such as "America/New_York"
    /// * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30
    /// When the timezone is not specified, the timestamp is considered to have no timezone
    /// and is represented _as is_
    Timestamp(TimeUnit, Option<String>),
    /// An [`i32`] representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days.
    Date,
    /// A 64-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    /// Only [`TimeUnit::Microsecond`] and [`TimeUnit::Nanosecond`] are supported on this variant.
    Time(TimeUnit),
    /// Measure of elapsed time. This elapsed time is a physical duration (i.e. 1s as defined in S.I.)
    Duration(TimeUnit),
    /// Opaque binary data of variable length whose offsets are represented as [`i64`].
    Binary,
    /// A variable-length UTF-8 encoded string whose offsets are represented as [`i64`].
    Utf8,
    /// A list of some logical data type with a fixed number of elements.
    FixedSizeList(Box<Field>, usize),
    /// A list of some logical data type whose offsets are represented as [`i64`].
    List(Box<Field>),
    /// A nested [`DataType`] with a given number of [`Field`]s.
    Struct(Vec<Field>),
    // Stop ArrowTypes
    DaftType(Box<DataType>),
    PythonType(String),
    Unknown,
}

impl DataType {
    pub fn new_null() -> DataType {
        DataType::Null
    }

    pub fn to_arrow(&self) -> DaftResult<ArrowType> {
        match self {
            DataType::Null => Ok(ArrowType::Null),
            DataType::Boolean => Ok(ArrowType::Boolean),
            DataType::Int8 => Ok(ArrowType::Int8),
            DataType::Int16 => Ok(ArrowType::Int16),
            DataType::Int32 => Ok(ArrowType::Int32),
            DataType::Int64 => Ok(ArrowType::Int64),
            DataType::UInt8 => Ok(ArrowType::UInt8),
            DataType::UInt16 => Ok(ArrowType::UInt16),
            DataType::UInt32 => Ok(ArrowType::UInt32),
            DataType::UInt64 => Ok(ArrowType::UInt64),
            DataType::Float16 => Ok(ArrowType::Float16),
            DataType::Float32 => Ok(ArrowType::Float32),
            DataType::Float64 => Ok(ArrowType::Float64),
            DataType::Timestamp(unit, timezone) => {
                Ok(ArrowType::Timestamp(unit.to_arrow()?, timezone.clone()))
            }
            DataType::Date => Ok(ArrowType::Date32),
            DataType::Time(unit) => Ok(ArrowType::Time64(unit.to_arrow()?)),
            DataType::Duration(unit) => Ok(ArrowType::Duration(unit.to_arrow()?)),
            DataType::Binary => Ok(ArrowType::LargeBinary),
            DataType::Utf8 => Ok(ArrowType::LargeUtf8),
            DataType::FixedSizeList(field, size) => {
                Ok(ArrowType::FixedSizeList(Box::new(field.to_arrow()?), *size))
            }
            DataType::List(field) => Ok(ArrowType::List(Box::new(field.to_arrow()?))),
            DataType::Struct(fields) => Ok({
                let fields: DaftResult<Vec<arrow2::datatypes::Field>> =
                    fields.iter().map(|f| f.to_arrow()).collect();
                ArrowType::Struct(fields?)
            }),
            _ => Err(DaftError::TypeError(format!(
                "Can not convert {:?} into arrow type",
                self
            ))),
        }
    }

    #[inline]
    pub fn is_arrow(&self) -> bool {
        self.to_arrow().is_ok()
    }
}

impl From<&ArrowType> for DataType {
    fn from(item: &ArrowType) -> Self {
        match item {
            ArrowType::Null => DataType::Null,
            ArrowType::Boolean => DataType::Boolean,
            ArrowType::Int8 => DataType::Int8,
            ArrowType::Int16 => DataType::Int16,
            ArrowType::Int32 => DataType::Int32,
            ArrowType::Int64 => DataType::Int64,
            ArrowType::UInt8 => DataType::UInt8,
            ArrowType::UInt16 => DataType::UInt16,
            ArrowType::UInt32 => DataType::UInt32,
            ArrowType::UInt64 => DataType::UInt64,
            ArrowType::Float16 => DataType::Float16,
            ArrowType::Float32 => DataType::Float32,
            ArrowType::Float64 => DataType::Float64,
            ArrowType::Timestamp(unit, timezone) => {
                DataType::Timestamp(unit.into(), timezone.clone())
            }
            ArrowType::Date32 => DataType::Date,
            ArrowType::Date64 => DataType::Timestamp(TimeUnit::Milliseconds, None),
            ArrowType::Time32(timeunit) | ArrowType::Time64(timeunit) => {
                DataType::Time(timeunit.into())
            }
            ArrowType::Duration(timeunit) => DataType::Duration(timeunit.into()),
            ArrowType::Binary | ArrowType::LargeBinary => DataType::Binary,
            ArrowType::Utf8 | ArrowType::LargeUtf8 => DataType::Utf8,
            ArrowType::List(field) | ArrowType::LargeList(field) => {
                DataType::List(Box::new(field.as_ref().into()))
            }
            ArrowType::FixedSizeList(field, size) => {
                DataType::FixedSizeList(Box::new(field.as_ref().into()), *size)
            }
            ArrowType::Struct(fields) => {
                let fields: Vec<Field> = fields.iter().map(|fld| fld.into()).collect();
                DataType::Struct(fields)
            }
            _ => panic!("DataType :{item:?} is not supported"),
        }
    }
}
