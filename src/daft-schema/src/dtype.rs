use std::{
    collections::HashMap,
    fmt::{Display, Write},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use daft_arrow::datatypes::DataType as ArrowType;
use serde::{Deserialize, Serialize};

use crate::{field::Field, image_mode::ImageMode, media_type::MediaType, time_unit::TimeUnit};
pub type DaftDataType = DataType;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum DataType {
    // ArrowTypes:
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

    /// A [`f32`]
    Float32,

    /// A [`f64`]
    Float64,

    /// Fixed-precision decimal type.
    /// TODO: allow negative scale once Arrow2 allows it: https://github.com/jorgecarleitao/arrow2/issues/1518
    Decimal128(usize, usize),

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
    ///
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

    /// A duration of **relative** time (year, day, etc).
    /// This is not a physical duration, but a calendar duration.
    /// This differs from `Duration` in that it is not a fixed amount of time, and is affected by calendar events (leap years, daylight savings, etc.)
    Interval,

    /// Opaque binary data of variable length whose offsets are represented as [`i64`].
    Binary,

    /// Opaque binary data of fixed size. Enum parameter specifies the number of bytes per value.
    FixedSizeBinary(usize),

    /// A variable-length UTF-8 encoded string whose offsets are represented as [`i64`].
    Utf8,

    /// A list of some logical data type with a fixed number of elements.
    FixedSizeList(Box<DataType>, usize),

    /// A list of some logical data type whose offsets are represented as [`i64`].
    List(Box<DataType>),

    /// A nested [`DataType`] with a given number of [`Field`]s.
    Struct(Vec<Field>),

    /// A nested [`DataType`] that is represented as List<entries: Struct<key: K, value: V>>.
    Map {
        key: Box<DataType>,
        value: Box<DataType>,
    },

    /// Extension type.
    Extension(String, Box<DataType>, Option<String>),

    // Non-ArrowTypes:
    /// A logical type for embeddings.
    Embedding(Box<DataType>, usize),

    /// A logical type for images with variable shapes.
    Image(Option<ImageMode>),

    /// A logical type for images with the same size (height x width).
    FixedShapeImage(ImageMode, u32, u32),

    /// A logical type for tensors with variable shapes.
    Tensor(Box<DataType>),

    /// A logical type for tensors with the same shape.
    FixedShapeTensor(Box<DataType>, Vec<u64>),

    /// A logical type for sparse tensors with variable shapes.
    SparseTensor(Box<DataType>, bool),

    /// A logical type for sparse tensors with the same shape.
    FixedShapeSparseTensor(Box<DataType>, Vec<u64>, bool),

    #[cfg(feature = "python")]
    Python,

    Unknown,
    File(MediaType),
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "Null"),
            Self::Boolean => write!(f, "Bool"),
            Self::Int8 => write!(f, "Int8"),
            Self::Int16 => write!(f, "Int16"),
            Self::Int32 => write!(f, "Int32"),
            Self::Int64 => write!(f, "Int64"),
            Self::UInt8 => write!(f, "UInt8"),
            Self::UInt16 => write!(f, "UInt16"),
            Self::UInt32 => write!(f, "UInt32"),
            Self::UInt64 => write!(f, "UInt64"),
            Self::Float32 => write!(f, "Float32"),
            Self::Float64 => write!(f, "Float64"),
            Self::Decimal128(precision, scale) => {
                write!(f, "Decimal[precision: {precision}, scale: {scale}]")
            }
            Self::Timestamp(unit, timezone) => {
                if let Some(timezone) = timezone {
                    write!(f, "Timestamp[{unit}; {timezone}]")
                } else {
                    write!(f, "Timestamp[{unit}]")
                }
            }
            Self::Date => write!(f, "Date"),
            Self::Time(unit) => write!(f, "Time[{unit}]"),
            Self::Duration(unit) => write!(f, "Duration[{unit}]"),
            Self::Interval => write!(f, "Interval"),
            Self::Binary => write!(f, "Binary"),
            Self::FixedSizeBinary(size) => write!(f, "Binary[{size}]"),
            Self::Utf8 => write!(f, "String"),
            Self::FixedSizeList(child_dtype, size) => write!(f, "List[{child_dtype}; {size}]"),
            Self::List(child_dtype) => write!(f, "List[{child_dtype}]"),
            Self::Struct(fields) => {
                let mut contents = String::default();
                for (index, field) in fields.iter().enumerate() {
                    if index != 0 {
                        write!(&mut contents, ", ")?;
                    }
                    if !(field.name.is_empty() && field.dtype.is_null()) {
                        write!(&mut contents, "{}: {}", field.name, field.dtype)?;
                    }
                }

                write!(f, "Struct[{}]", contents)
            }
            Self::Map { key, value } => write!(f, "Map[{key}: {value}]"),
            Self::Extension(name, dtype, _) => write!(f, "Extension[{name}; {dtype}]"),
            Self::Embedding(dtype, size) => write!(f, "Embedding[{dtype}; {size}]"),
            Self::Image(mode) => {
                if let Some(mode) = mode {
                    write!(f, "Image[{mode}]")
                } else {
                    write!(f, "Image[MIXED]")
                }
            }
            Self::FixedShapeImage(mode, height, width) => {
                write!(f, "Image[{mode}; {height} x {width}]")
            }
            Self::Tensor(dtype) => write!(f, "Tensor[{dtype}]"),
            Self::FixedShapeTensor(dtype, shape) => write!(f, "Tensor[{dtype}; {shape:?}]"),
            Self::SparseTensor(dtype, indices_offset) => {
                write!(f, "SparseTensor[{dtype}; indices_offset: {indices_offset}]")
            }
            Self::FixedShapeSparseTensor(dtype, shape, indices_offset) => write!(
                f,
                "FixedShapeSparseTensor[{dtype}; {shape:?}; indices_offset: {indices_offset}]"
            ),
            #[cfg(feature = "python")]
            Self::Python => write!(f, "Python"),
            Self::Unknown => write!(f, "Unknown"),
            Self::File(format) => write!(f, "File[{format}]"),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct DataTypePayload {
    datatype: DataType,
    daft_version: String,
    daft_build_type: String,
}

impl DataTypePayload {
    pub fn new(datatype: &DataType) -> Self {
        Self {
            datatype: datatype.clone(),
            daft_version: common_version::VERSION.into(),
            daft_build_type: common_version::DAFT_BUILD_TYPE.into(),
        }
    }
}
const DAFT_SUPER_EXTENSION_NAME: &str = "daft.super_extension";

impl DataType {
    pub fn new_null() -> Self {
        Self::Null
    }

    pub fn new_list(datatype: Self) -> Self {
        Self::List(Box::new(datatype))
    }

    pub fn new_fixed_size_list(datatype: Self, size: usize) -> Self {
        Self::FixedSizeList(Box::new(datatype), size)
    }

    pub fn to_arrow_field(&self) -> DaftResult<arrow_schema::Field> {
        let dtype = match self {
            Self::Null => arrow_schema::DataType::Null,
            Self::Boolean => arrow_schema::DataType::Boolean,
            Self::Int8 => arrow_schema::DataType::Int8,
            Self::Int16 => arrow_schema::DataType::Int16,
            Self::Int32 => arrow_schema::DataType::Int32,
            Self::Int64 => arrow_schema::DataType::Int64,
            Self::UInt8 => arrow_schema::DataType::UInt8,
            Self::UInt16 => arrow_schema::DataType::UInt16,
            Self::UInt32 => arrow_schema::DataType::UInt32,
            Self::UInt64 => arrow_schema::DataType::UInt64,
            Self::Float32 => arrow_schema::DataType::Float32,
            Self::Float64 => arrow_schema::DataType::Float64,
            Self::Timestamp(unit, tz) => {
                arrow_schema::DataType::Timestamp(unit.to_arrow(), tz.clone().map(Arc::from))
            }
            Self::Duration(unit) => arrow_schema::DataType::Duration(unit.to_arrow()),
            Self::Interval => {
                arrow_schema::DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano)
            }
            Self::Binary => arrow_schema::DataType::LargeBinary,
            Self::FixedSizeBinary(size) => arrow_schema::DataType::FixedSizeBinary(*size as _),
            Self::Utf8 => arrow_schema::DataType::LargeUtf8,
            Self::List(f) => arrow_schema::DataType::LargeList(Arc::new(f.to_arrow_field()?)),
            Self::FixedSizeList(f, size) => {
                arrow_schema::DataType::FixedSizeList(Arc::new(f.to_arrow_field()?), *size as _)
            }
            Self::Struct(f) => arrow_schema::DataType::Struct(
                f.iter()
                    .map(|f| f.to_arrow())
                    .collect::<DaftResult<Vec<_>>>()?
                    .into(),
            ),
            Self::Map { key, value } => {
                // To comply with the Arrow spec, Neither the "entries" field nor the "key" field may be nullable.
                // See https://github.com/apache/arrow/blob/apache-arrow-20.0.0/format/Schema.fbs#L138
                let struct_type = arrow_schema::DataType::Struct(
                    vec![
                        arrow_schema::Field::new(
                            "key",
                            key.to_arrow_field()?.data_type().clone(),
                            false,
                        ),
                        arrow_schema::Field::new(
                            "value",
                            value.to_arrow_field()?.data_type().clone(),
                            true,
                        ),
                    ]
                    .into(),
                );
                let struct_field = arrow_schema::Field::new("entries", struct_type, false);

                arrow_schema::DataType::Map(Arc::new(struct_field), false)
            }
            Self::Decimal128(precision, scale) => {
                arrow_schema::DataType::Decimal128(*precision as _, *scale as _)
            }
            Self::Extension(name, d, metadata) => {
                let mut metadata_map = HashMap::new();
                metadata_map.insert("ARROW:extension:name".to_string(), name.clone());
                if let Some(metadata) = metadata {
                    metadata_map.insert("ARROW:extension:metadata".to_string(), metadata.clone());
                }

                return Ok(d.to_arrow_field()?.with_metadata(metadata_map));
            }
            Self::Date => arrow_schema::DataType::Date32,
            Self::Time(time_unit) => arrow_schema::DataType::Time64(time_unit.to_arrow()),
            Self::Embedding(..)
            | Self::Image(..)
            | Self::FixedShapeImage(..)
            | Self::Tensor(..)
            | Self::FixedShapeTensor(..)
            | Self::SparseTensor(..)
            | Self::FixedShapeSparseTensor(..)
            | Self::File(..) => {
                let physical = Box::new(self.to_physical());
                let logical_extension = Self::Extension(
                    DAFT_SUPER_EXTENSION_NAME.into(),
                    physical,
                    Some(self.to_json()?),
                );
                return logical_extension.to_arrow_field();
            }
            #[cfg(feature = "python")]
            Self::Python => {
                let physical = Box::new(Self::Binary);
                let logical_extension = Self::Extension(
                    DAFT_SUPER_EXTENSION_NAME.into(),
                    physical,
                    Some(self.to_json()?),
                );
                return logical_extension.to_arrow_field();
            }
            Self::Unknown => {
                return Err(DaftError::TypeError(format!(
                    "Can not convert {self:?} into arrow type"
                )));
            }
        };
        Ok(arrow_schema::Field::new("", dtype, true))
    }

    #[deprecated(note = "use `to_arrow_field` instead")]
    #[allow(deprecated, reason = "arrow2 migration")]
    pub fn to_arrow(&self) -> DaftResult<ArrowType> {
        match self {
            Self::Null => Ok(ArrowType::Null),
            Self::Boolean => Ok(ArrowType::Boolean),
            Self::Int8 => Ok(ArrowType::Int8),
            Self::Int16 => Ok(ArrowType::Int16),
            Self::Int32 => Ok(ArrowType::Int32),
            Self::Int64 => Ok(ArrowType::Int64),
            Self::UInt8 => Ok(ArrowType::UInt8),
            Self::UInt16 => Ok(ArrowType::UInt16),
            Self::UInt32 => Ok(ArrowType::UInt32),
            Self::UInt64 => Ok(ArrowType::UInt64),
            // DataType::Float16 => Ok(ArrowType::Float16),
            Self::Float32 => Ok(ArrowType::Float32),
            Self::Float64 => Ok(ArrowType::Float64),
            Self::Decimal128(precision, scale) => Ok(ArrowType::Decimal(*precision, *scale)),
            Self::Timestamp(unit, timezone) => {
                Ok(ArrowType::Timestamp(unit.to_arrow2(), timezone.clone()))
            }
            Self::Date => Ok(ArrowType::Date32),
            Self::Time(unit) => Ok(ArrowType::Time64(unit.to_arrow2())),
            Self::Duration(unit) => Ok(ArrowType::Duration(unit.to_arrow2())),
            Self::Interval => Ok(ArrowType::Interval(
                daft_arrow::datatypes::IntervalUnit::MonthDayNano,
            )),

            Self::Binary => Ok(ArrowType::LargeBinary),
            Self::FixedSizeBinary(size) => Ok(ArrowType::FixedSizeBinary(*size)),
            Self::Utf8 => Ok(ArrowType::LargeUtf8),
            Self::FixedSizeList(child_dtype, size) => Ok(ArrowType::FixedSizeList(
                Box::new(daft_arrow::datatypes::Field::new(
                    "item",
                    child_dtype.to_arrow()?,
                    true,
                )),
                *size,
            )),
            Self::List(field) => Ok(ArrowType::LargeList(Box::new(
                daft_arrow::datatypes::Field::new("item", field.to_arrow()?, true),
            ))),
            Self::Map { key, value } => {
                // To comply with the Arrow spec, Neither the "entries" field nor the "key" field may be nullable.
                // See https://github.com/apache/arrow/blob/apache-arrow-20.0.0/format/Schema.fbs#L138
                let struct_type = ArrowType::Struct(vec![
                    daft_arrow::datatypes::Field::new("key", key.to_arrow()?, false),
                    daft_arrow::datatypes::Field::new("value", value.to_arrow()?, true),
                ]);
                let struct_field = daft_arrow::datatypes::Field::new("entries", struct_type, false);

                Ok(ArrowType::map(struct_field, false))
            }
            Self::Struct(fields) => Ok({
                let fields = fields
                    .iter()
                    .map(|f| f.to_arrow2())
                    .collect::<DaftResult<Vec<daft_arrow::datatypes::Field>>>()?;
                ArrowType::Struct(fields)
            }),
            Self::Extension(name, dtype, metadata) => Ok(ArrowType::Extension(
                name.clone(),
                Box::new(dtype.to_arrow()?),
                metadata.clone(),
            )),
            Self::Embedding(..)
            | Self::Image(..)
            | Self::FixedShapeImage(..)
            | Self::Tensor(..)
            | Self::FixedShapeTensor(..)
            | Self::SparseTensor(..)
            | Self::FixedShapeSparseTensor(..)
            | Self::File(..) => {
                let physical = Box::new(self.to_physical());
                let logical_extension = Self::Extension(
                    DAFT_SUPER_EXTENSION_NAME.into(),
                    physical,
                    Some(self.to_json()?),
                );
                logical_extension.to_arrow()
            }
            #[cfg(feature = "python")]
            Self::Python => {
                let physical = Box::new(Self::Binary);
                let logical_extension = Self::Extension(
                    DAFT_SUPER_EXTENSION_NAME.into(),
                    physical,
                    Some(self.to_json()?),
                );
                logical_extension.to_arrow()
            }
            Self::Unknown => Err(DaftError::TypeError(format!(
                "Can not convert {self:?} into arrow type"
            ))),
        }
    }

    pub fn to_physical(&self) -> Self {
        use DataType::*;
        match self {
            Date => Int32,
            Duration(_) | Timestamp(..) | Time(_) => Int64,

            List(child_dtype) => List(Box::new(child_dtype.to_physical())),
            FixedSizeList(child_dtype, size) => {
                FixedSizeList(Box::new(child_dtype.to_physical()), *size)
            }
            Struct(fields) => Struct(fields.iter().map(|field| field.to_physical()).collect()),
            Map { key, value } => List(Box::new(Struct(vec![
                Field::new("key", key.to_physical()),
                Field::new("value", value.to_physical()),
            ]))),
            Embedding(dtype, size) => FixedSizeList(Box::new(dtype.to_physical()), *size),
            Image(mode) => Struct(vec![
                Field::new(
                    "data",
                    List(Box::new(mode.map_or(Self::UInt8, |m| m.get_dtype()))),
                ),
                Field::new("channel", UInt16),
                Field::new("height", UInt32),
                Field::new("width", UInt32),
                Field::new("mode", UInt8),
            ]),
            FixedShapeImage(mode, height, width) => FixedSizeList(
                Box::new(mode.get_dtype()),
                usize::try_from(mode.num_channels() as u32 * height * width).unwrap(),
            ),
            Tensor(dtype) => Struct(vec![
                Field::new("data", List(Box::new(*dtype.clone()))),
                Field::new("shape", List(Box::new(Self::UInt64))),
            ]),
            FixedShapeTensor(dtype, shape) => FixedSizeList(
                Box::new(*dtype.clone()),
                usize::try_from(shape.iter().product::<u64>()).unwrap(),
            ),
            SparseTensor(dtype, _) => Struct(vec![
                Field::new("values", List(Box::new(*dtype.clone()))),
                Field::new("indices", List(Box::new(Self::UInt64))),
                Field::new("shape", List(Box::new(Self::UInt64))),
            ]),
            FixedShapeSparseTensor(dtype, shape, _) => Struct(vec![
                Field::new("values", List(Box::new(*dtype.clone()))),
                {
                    let largest_index = std::cmp::max(shape.iter().product::<u64>(), 1) - 1;
                    let minimal_indices_dtype = {
                        if u8::try_from(largest_index).is_ok() {
                            Self::UInt8
                        } else if u16::try_from(largest_index).is_ok() {
                            Self::UInt16
                        } else if u32::try_from(largest_index).is_ok() {
                            Self::UInt32
                        } else {
                            Self::UInt64
                        }
                    };
                    Field::new("indices", List(Box::new(minimal_indices_dtype)))
                },
            ]),
            File(..) => Struct(vec![
                Field::new("url", Utf8),
                Field::new("io_config", Binary),
            ]),
            _ => {
                assert!(self.is_physical());
                self.clone()
            }
        }
    }

    /// Check if this datatype can be converted into an Arrow datatype.
    /// This includes checking if the associated arrays can be converted into Arrow arrays.
    #[inline]
    /// Is this DataType convertible to Arrow?
    pub fn is_arrow(&self) -> bool {
        #[allow(deprecated, reason = "arrow2 migration")]
        self.to_arrow().is_ok()
    }

    #[inline]
    pub fn is_numeric(&self) -> bool {
        match self {
            Self::Int8
            | Self::Int16
            | Self::Int32
            | Self::Int64
            | Self::UInt8
            | Self::UInt16
            | Self::UInt32
            | Self::UInt64
            // DataType::Float16
            | Self::Float32
            | Self::Float64 => true,
            Self::Extension(_, inner, _) => inner.is_numeric(),
            _ => false
        }
    }

    #[inline]
    pub fn is_primitive(&self) -> bool {
        match self {
            Self::Int8
            | Self::Int16
            | Self::Int32
            | Self::Int64
            | Self::UInt8
            | Self::UInt16
            | Self::UInt32
            | Self::UInt64
            // DataType::Float16
            | Self::Float32
            | Self::Float64
            | Self::Decimal128(..) => true,
            Self::Extension(_, inner, _) => inner.is_primitive(),
            _ => false
        }
    }

    #[inline]
    pub fn assert_is_numeric(&self) -> DaftResult<()> {
        if self.is_numeric() {
            Ok(())
        } else {
            Err(DaftError::TypeError(format!(
                "Numeric mean is not implemented for type {}",
                self,
            )))
        }
    }

    #[inline]
    pub fn is_fixed_size_numeric(&self) -> bool {
        match self {
            Self::FixedSizeList(dtype, ..)
            | Self::Embedding(dtype, ..)
            | Self::FixedShapeTensor(dtype, ..)
            | Self::FixedShapeSparseTensor(dtype, ..) => dtype.is_numeric(),
            _ => false,
        }
    }

    #[inline]
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::UInt8
                | Self::UInt16
                | Self::UInt32
                | Self::UInt64
        )
    }

    #[inline]
    pub fn is_floating(&self) -> bool {
        matches!(
            self,
            // DataType::Float16 |
            Self::Float32 | Self::Float64
        )
    }

    #[inline]
    pub fn is_temporal(&self) -> bool {
        match self {
            Self::Date | Self::Timestamp(..) => true,
            Self::Extension(_, inner, _) => inner.is_temporal(),
            _ => false,
        }
    }

    #[inline]
    pub fn is_embedding(&self) -> bool {
        matches!(self, Self::Embedding(..))
    }

    #[inline]
    pub fn is_tensor(&self) -> bool {
        matches!(self, Self::Tensor(..))
    }

    #[inline]
    pub fn is_sparse_tensor(&self) -> bool {
        matches!(self, Self::SparseTensor(..))
    }

    #[inline]
    pub fn is_fixed_shape_tensor(&self) -> bool {
        matches!(self, Self::FixedShapeTensor(..))
    }

    #[inline]
    pub fn is_fixed_shape_sparse_tensor(&self) -> bool {
        matches!(self, Self::FixedShapeSparseTensor(..))
    }

    #[inline]
    pub fn is_image(&self) -> bool {
        matches!(self, Self::Image(..))
    }

    #[inline]
    pub fn is_fixed_shape_image(&self) -> bool {
        matches!(self, Self::FixedShapeImage(..))
    }

    #[inline]
    pub fn is_map(&self) -> bool {
        matches!(self, Self::Map { .. })
    }

    #[inline]
    pub fn is_list(&self) -> bool {
        matches!(self, Self::List(..))
    }

    #[inline]
    pub fn is_string(&self) -> bool {
        matches!(self, Self::Utf8)
    }

    #[inline]
    pub fn is_boolean(&self) -> bool {
        matches!(self, Self::Boolean)
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        match self {
            Self::Null => true,
            Self::Extension(_, inner, _) => inner.is_null(),
            _ => false,
        }
    }
    #[inline]
    pub fn is_null_or<F: Fn(&Self) -> bool>(&self, f: F) -> bool {
        match self {
            Self::Null => true,
            Self::Extension(_, inner, _) => inner.is_null_or(f),
            _ => f(self),
        }
    }

    #[inline]
    pub fn is_int8(&self) -> bool {
        matches!(self, Self::Int8)
    }

    #[inline]
    pub fn is_int16(&self) -> bool {
        matches!(self, Self::Int16)
    }

    #[inline]
    pub fn is_int32(&self) -> bool {
        matches!(self, Self::Int32)
    }

    #[inline]
    pub fn is_int64(&self) -> bool {
        matches!(self, Self::Int64)
    }

    #[inline]
    pub fn is_uint8(&self) -> bool {
        matches!(self, Self::UInt8)
    }

    #[inline]
    pub fn is_uint16(&self) -> bool {
        matches!(self, Self::UInt16)
    }

    #[inline]
    pub fn is_uint32(&self) -> bool {
        matches!(self, Self::UInt32)
    }

    #[inline]
    pub fn is_uint64(&self) -> bool {
        matches!(self, Self::UInt64)
    }

    #[inline]
    pub fn is_float32(&self) -> bool {
        matches!(self, Self::Float32)
    }

    #[inline]
    pub fn is_float64(&self) -> bool {
        matches!(self, Self::Float64)
    }

    #[inline]
    pub fn is_decimal128(&self) -> bool {
        matches!(self, Self::Decimal128(_, _))
    }

    #[inline]
    pub fn is_timestamp(&self) -> bool {
        matches!(self, Self::Timestamp(..))
    }

    #[inline]
    pub fn is_date(&self) -> bool {
        matches!(self, Self::Date)
    }

    #[inline]
    pub fn is_time(&self) -> bool {
        matches!(self, Self::Time(..))
    }

    #[inline]
    pub fn is_duration(&self) -> bool {
        matches!(self, Self::Duration(..))
    }

    #[inline]
    pub fn is_interval(&self) -> bool {
        matches!(self, Self::Interval)
    }

    #[inline]
    pub fn is_binary(&self) -> bool {
        matches!(self, Self::Binary)
    }

    #[inline]
    pub fn is_fixed_size_binary(&self) -> bool {
        matches!(self, Self::FixedSizeBinary(_))
    }

    #[inline]
    pub fn is_fixed_size_list(&self) -> bool {
        matches!(self, Self::FixedSizeList(..))
    }

    #[inline]
    pub fn is_struct(&self) -> bool {
        matches!(self, Self::Struct(..))
    }

    #[inline]
    pub fn is_extension(&self) -> bool {
        matches!(self, Self::Extension(..))
    }

    #[inline]
    pub fn is_python(&self) -> bool {
        match self {
            #[cfg(feature = "python")]
            Self::Python => true,
            Self::Extension(_, inner, _) => inner.is_python(),
            _ => false,
        }
    }

    #[inline]
    pub fn is_file(&self) -> bool {
        match self {
            Self::File(..) => true,
            Self::Extension(_, inner, _) => inner.is_file(),
            _ => false,
        }
    }

    #[inline]
    pub fn to_floating_representation(&self) -> DaftResult<Self> {
        let data_type = match self {
            // All numeric types that coerce to `f32`
            Self::Int8 => Self::Float32,
            Self::Int16 => Self::Float32,
            Self::UInt8 => Self::Float32,
            Self::UInt16 => Self::Float32,
            Self::Float32 => Self::Float32,

            // All numeric types that coerce to `f64`
            Self::Int32 => Self::Float64,
            Self::Int64 => Self::Float64,
            Self::UInt32 => Self::Float64,
            Self::UInt64 => Self::Float64,
            Self::Float64 => Self::Float64,

            _ => {
                return Err(DaftError::TypeError(format!(
                    "Expected input to be numeric, instead got {}",
                    self,
                )));
            }
        };
        Ok(data_type)
    }

    pub fn estimate_size_bytes(&self) -> Option<f64> {
        const VARIABLE_TYPE_SIZE: f64 = 20.;
        const DEFAULT_LIST_LEN: f64 = 4.;

        let elem_size = match self.to_physical() {
            Self::Null => Some(0.),
            Self::Boolean => Some(0.125),
            Self::Int8 => Some(1.),
            Self::Int16 => Some(2.),
            Self::Int32 => Some(4.),
            Self::Int64 => Some(8.),
            Self::Decimal128(..) => Some(16.),
            Self::UInt8 => Some(1.),
            Self::UInt16 => Some(2.),
            Self::UInt32 => Some(4.),
            Self::UInt64 => Some(8.),
            Self::Float32 => Some(4.),
            Self::Float64 => Some(8.),
            Self::Utf8 => Some(VARIABLE_TYPE_SIZE),
            Self::Binary => Some(VARIABLE_TYPE_SIZE),
            Self::FixedSizeBinary(size) => Some(size as f64),
            Self::FixedSizeList(dtype, len) => {
                dtype.estimate_size_bytes().map(|b| b * (len as f64))
            }
            Self::List(dtype) => dtype.estimate_size_bytes().map(|b| b * DEFAULT_LIST_LEN),
            Self::Struct(fields) => Some(
                fields
                    .iter()
                    .map(|f| f.dtype.estimate_size_bytes().unwrap_or(0f64))
                    .sum(),
            ),
            Self::Extension(_, dtype, _) => dtype.estimate_size_bytes(),
            _ => None,
        };
        // add bitmap
        elem_size.map(|e| e + 0.125)
    }

    #[inline]
    pub fn is_logical(&self) -> bool {
        matches!(
            self,
            Self::Date
                | Self::Time(..)
                | Self::Timestamp(..)
                | Self::Duration(..)
                | Self::Embedding(..)
                | Self::Image(..)
                | Self::FixedShapeImage(..)
                | Self::Tensor(..)
                | Self::FixedShapeTensor(..)
                | Self::SparseTensor(..)
                | Self::FixedShapeSparseTensor(..)
                | Self::Map { .. }
                | Self::File(..)
        )
    }

    #[inline]
    pub fn is_physical(&self) -> bool {
        !self.is_logical()
    }

    #[inline]
    pub fn is_nested(&self) -> bool {
        let p: Self = self.to_physical();
        matches!(
            p,
            Self::List(..) | Self::FixedSizeList(..) | Self::Struct(..) | Self::Map { .. }
        )
    }

    pub fn to_json(&self) -> DaftResult<String> {
        let payload = DataTypePayload::new(self);
        Ok(serde_json::to_string(&payload)?)
    }

    pub fn from_json(input: &str) -> DaftResult<Self> {
        let val: DataTypePayload = serde_json::from_str(input)?;
        Ok(val.datatype)
    }

    /// If the datatype variant has a `size` property, return it.
    /// For example, `FixedSizeBinary` and `FixedSizeList` have a size property.
    pub fn fixed_size(&self) -> DaftResult<usize> {
        match self {
            Self::FixedSizeBinary(size) => Ok(*size),
            Self::FixedSizeList(_, size) => Ok(*size),
            Self::Embedding(_, size) => Ok(*size),
            _ => Err(DaftError::TypeError(format!(
                "DataType {self:?} does not have a `fixed_size` property",
            ))),
        }
    }
    /// if the datatype variant has a shape, return it.
    /// For example, `FixedShapeImage` and `FixedShapeTensor` have a fixed shape.
    pub fn fixed_shape(&self) -> DaftResult<Vec<u64>> {
        match self {
            Self::FixedShapeImage(_, height, width) => Ok(vec![*height as u64, *width as u64]),
            Self::FixedShapeTensor(_, shape) => Ok(shape.clone()),
            Self::FixedShapeSparseTensor(_, shape, _) => Ok(shape.clone()),
            _ => Err(DaftError::TypeError(format!(
                "DataType {self:?} does not have a `fixed_shape` property",
            ))),
        }
    }

    /// if the datatype contains a timeunit, return it.
    pub fn time_unit(&self) -> DaftResult<TimeUnit> {
        match self {
            Self::Timestamp(unit, _) => Ok(*unit),
            Self::Time(unit) => Ok(*unit),
            Self::Duration(unit) => Ok(*unit),
            _ => Err(DaftError::TypeError(format!(
                "DataType {self:?} does not have a `time_unit` property",
            ))),
        }
    }

    /// if the datatype contains a timezone, return it.
    pub fn time_zone(&self) -> DaftResult<Option<&str>> {
        match self {
            Self::Timestamp(_, timezone) => Ok(timezone.as_deref()),
            _ => Err(DaftError::TypeError(format!(
                "DataType {self:?} does not have a `time_zone` property",
            ))),
        }
    }

    /// if the datatype contains an image mode, return it.
    /// For example, `Image` and `FixedShapeImage` have an image mode.
    pub fn image_mode(&self) -> DaftResult<Option<ImageMode>> {
        match self {
            Self::Image(mode) => Ok(*mode),
            Self::FixedShapeImage(mode, ..) => Ok(Some(*mode)),
            _ => Err(DaftError::TypeError(format!(
                "DataType {self:?} does not have an `image_mode` property",
            ))),
        }
    }

    /// if the datatype contains an inner datatype, return it.
    /// For example, `List` and `FixedSizeList` have an inner datatype.
    pub fn dtype(&self) -> DaftResult<&Self> {
        match self {
            Self::List(dtype) | Self::FixedSizeList(dtype, _) => Ok(dtype),
            Self::Embedding(dtype, _) => Ok(dtype),
            Self::Extension(_, dtype, _) => Ok(dtype),
            Self::Tensor(dtype) => Ok(dtype),
            Self::SparseTensor(dtype, _) => Ok(dtype),
            Self::FixedShapeTensor(dtype, _) => Ok(dtype),
            Self::FixedShapeSparseTensor(dtype, _, _) => Ok(dtype),
            _ => Err(DaftError::TypeError(format!(
                "DataType {self:?} does not have a `dtype` property",
            ))),
        }
    }

    /// if the datatype is a struct, return its fields.
    pub fn fields(&self) -> DaftResult<&[Field]> {
        match self {
            Self::Struct(fields) => Ok(fields),
            _ => Err(DaftError::TypeError(format!(
                "DataType {self:?} does not have a `fields` property",
            ))),
        }
    }

    /// if the datatype is a decimal, return its precision.
    pub fn precision(&self) -> DaftResult<usize> {
        match self {
            Self::Decimal128(precision, _) => Ok(*precision),
            _ => Err(DaftError::TypeError(format!(
                "DataType {self:?} does not have a `precision` property",
            ))),
        }
    }

    /// if the datatype is a decimal, return its scale.
    pub fn scale(&self) -> DaftResult<usize> {
        match self {
            Self::Decimal128(_, scale) => Ok(*scale),
            _ => Err(DaftError::TypeError(format!(
                "DataType {self:?} does not have a `scale` property",
            ))),
        }
    }

    /// if the datatype is a sparse tensor, return whether it uses offset indices.
    pub fn use_offset_indices(&self) -> DaftResult<bool> {
        match self {
            Self::SparseTensor(_, use_offset) => Ok(*use_offset),
            Self::FixedShapeSparseTensor(_, _, use_offset) => Ok(*use_offset),
            _ => Err(DaftError::TypeError(format!(
                "DataType {self:?} does not have a `use_offset_indices` property",
            ))),
        }
    }

    /// if the datatype is a map, return its key type.
    pub fn key_type(&self) -> DaftResult<&Self> {
        match self {
            Self::Map { key, .. } => Ok(key),
            _ => Err(DaftError::TypeError(format!(
                "DataType {self:?} does not have a `key_type` property",
            ))),
        }
    }

    /// if the datatype is a map, return its value type.
    pub fn value_type(&self) -> DaftResult<&Self> {
        match self {
            Self::Map { value, .. } => Ok(value),
            _ => Err(DaftError::TypeError(format!(
                "DataType {self:?} does not have a `value_type` property",
            ))),
        }
    }
}

#[expect(
    clippy::fallible_impl_from,
    reason = "https://github.com/Eventual-Inc/Daft/issues/3015"
)]
impl From<&ArrowType> for DataType {
    fn from(item: &ArrowType) -> Self {
        match item {
            ArrowType::Null => Self::Null,
            ArrowType::Boolean => Self::Boolean,
            ArrowType::Int8 => Self::Int8,
            ArrowType::Int16 => Self::Int16,
            ArrowType::Int32 => Self::Int32,
            ArrowType::Int64 => Self::Int64,
            ArrowType::UInt8 => Self::UInt8,
            ArrowType::UInt16 => Self::UInt16,
            ArrowType::UInt32 => Self::UInt32,
            ArrowType::UInt64 => Self::UInt64,
            // ArrowType::Float16 => DataType::Float16,
            ArrowType::Float32 => Self::Float32,
            ArrowType::Float64 => Self::Float64,
            ArrowType::Timestamp(unit, timezone) => Self::Timestamp(unit.into(), timezone.clone()),
            ArrowType::Date32 => Self::Date,
            ArrowType::Date64 => Self::Timestamp(TimeUnit::Milliseconds, None),
            ArrowType::Time32(timeunit) | ArrowType::Time64(timeunit) => {
                Self::Time(timeunit.into())
            }
            ArrowType::Duration(timeunit) => Self::Duration(timeunit.into()),
            ArrowType::Interval(_) => Self::Interval,
            ArrowType::FixedSizeBinary(size) => Self::FixedSizeBinary(*size),
            ArrowType::Binary | ArrowType::LargeBinary => Self::Binary,
            ArrowType::Utf8 | ArrowType::LargeUtf8 => Self::Utf8,
            ArrowType::Decimal(precision, scale) => Self::Decimal128(*precision, *scale),
            ArrowType::List(field) | ArrowType::LargeList(field) => {
                Self::List(Box::new(field.as_ref().data_type().into()))
            }
            ArrowType::FixedSizeList(field, size) => {
                Self::FixedSizeList(Box::new(field.as_ref().data_type().into()), *size)
            }
            ArrowType::Map(field, ..) => {
                // todo: TryFrom in future? want in second pass maybe

                // field should be a struct
                let ArrowType::Struct(fields) = &field.data_type else {
                    panic!("Map should have a struct as its key")
                };

                let [key, value] = fields.as_slice() else {
                    panic!("Map should have two fields")
                };

                let key = &key.data_type;
                let value = &value.data_type;

                let key = Self::from(key);
                let value = Self::from(value);

                let key = Box::new(key);
                let value = Box::new(value);

                Self::Map { key, value }
            }
            ArrowType::Struct(fields) => {
                let fields: Vec<Field> = fields.iter().map(|fld| fld.into()).collect();
                Self::Struct(fields)
            }
            ArrowType::Extension(name, dtype, metadata) => {
                if name == DAFT_SUPER_EXTENSION_NAME
                    && let Some(metadata) = metadata
                    && let Ok(daft_extension) = Self::from_json(metadata.as_str())
                {
                    return daft_extension;
                }
                Self::Extension(
                    name.clone(),
                    Box::new(dtype.as_ref().into()),
                    metadata.clone(),
                )
            }

            _ => panic!("DataType :{item:?} is not supported"),
        }
    }
}

impl From<&ImageMode> for DataType {
    fn from(mode: &ImageMode) -> Self {
        use ImageMode::*;

        match mode {
            L16 | LA16 | RGB16 | RGBA16 => Self::UInt16,
            RGB32F | RGBA32F => Self::Float32,
            _ => Self::UInt8,
        }
    }
}
