use std::fmt::{Display, Formatter, Result};

use arrow2::datatypes::DataType as ArrowType;

use crate::datatypes::{field::Field, image_mode::ImageMode, time_unit::TimeUnit};

use common_error::{DaftError, DaftResult};

use serde::{Deserialize, Serialize};

// pub type TimeZone = String;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
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
    /// An [`i128`]
    Int128,
    /// An [`u8`]
    UInt8,
    /// An [`u16`]
    UInt16,
    /// An [`u32`]
    UInt32,
    /// An [`u64`]
    UInt64,
    /// An 16-bit float
    // Float16,
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
    Map(Box<DataType>),
    /// Extension type.
    Extension(String, Box<DataType>, Option<String>),
    // Stop ArrowTypes
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
    #[cfg(feature = "python")]
    Python,
    Unknown,
}

#[derive(Serialize, Deserialize)]
struct DataTypePayload {
    datatype: DataType,
    daft_version: String,
    daft_build_type: String,
}

impl DataTypePayload {
    pub fn new(datatype: &DataType) -> Self {
        DataTypePayload {
            datatype: datatype.clone(),
            daft_version: crate::VERSION.into(),
            daft_build_type: crate::DAFT_BUILD_TYPE.into(),
        }
    }
}
const DAFT_SUPER_EXTENSION_NAME: &str = "daft.super_extension";

impl DataType {
    pub fn new_null() -> DataType {
        DataType::Null
    }

    pub fn new_list(datatype: DataType) -> DataType {
        DataType::List(Box::new(datatype))
    }

    pub fn new_fixed_size_list(datatype: DataType, size: usize) -> DataType {
        DataType::FixedSizeList(Box::new(datatype), size)
    }

    pub fn to_arrow(&self) -> DaftResult<ArrowType> {
        match self {
            DataType::Null => Ok(ArrowType::Null),
            DataType::Boolean => Ok(ArrowType::Boolean),
            DataType::Int8 => Ok(ArrowType::Int8),
            DataType::Int16 => Ok(ArrowType::Int16),
            DataType::Int32 => Ok(ArrowType::Int32),
            DataType::Int64 => Ok(ArrowType::Int64),
            // Must maintain same default mapping as Arrow2, otherwise this will throw errors in
            // DataArray<Int128Type>::new() which makes strong assumptions about the arrow/Daft types
            // https://github.com/jorgecarleitao/arrow2/blob/b0734542c2fef5d2d0c7b6ffce5d094de371168a/src/datatypes/mod.rs#L493
            DataType::Int128 => Ok(ArrowType::Decimal(32, 32)),
            DataType::UInt8 => Ok(ArrowType::UInt8),
            DataType::UInt16 => Ok(ArrowType::UInt16),
            DataType::UInt32 => Ok(ArrowType::UInt32),
            DataType::UInt64 => Ok(ArrowType::UInt64),
            // DataType::Float16 => Ok(ArrowType::Float16),
            DataType::Float32 => Ok(ArrowType::Float32),
            DataType::Float64 => Ok(ArrowType::Float64),
            DataType::Decimal128(precision, scale) => Ok(ArrowType::Decimal(*precision, *scale)),
            DataType::Timestamp(unit, timezone) => {
                Ok(ArrowType::Timestamp(unit.to_arrow(), timezone.clone()))
            }
            DataType::Date => Ok(ArrowType::Date32),
            DataType::Time(unit) => Ok(ArrowType::Time64(unit.to_arrow())),
            DataType::Duration(unit) => Ok(ArrowType::Duration(unit.to_arrow())),
            DataType::Binary => Ok(ArrowType::LargeBinary),
            DataType::FixedSizeBinary(size) => Ok(ArrowType::FixedSizeBinary(*size)),
            DataType::Utf8 => Ok(ArrowType::LargeUtf8),
            DataType::FixedSizeList(child_dtype, size) => Ok(ArrowType::FixedSizeList(
                Box::new(arrow2::datatypes::Field::new(
                    "item",
                    child_dtype.to_arrow()?,
                    true,
                )),
                *size,
            )),
            DataType::List(field) => Ok(ArrowType::LargeList(Box::new(
                arrow2::datatypes::Field::new("item", field.to_arrow()?, true),
            ))),
            DataType::Map(field) => Ok(ArrowType::Map(
                Box::new(arrow2::datatypes::Field::new(
                    "item",
                    field.to_arrow()?,
                    true,
                )),
                false,
            )),
            DataType::Struct(fields) => Ok({
                let fields = fields
                    .iter()
                    .map(|f| f.to_arrow())
                    .collect::<DaftResult<Vec<arrow2::datatypes::Field>>>()?;
                ArrowType::Struct(fields)
            }),
            DataType::Extension(name, dtype, metadata) => Ok(ArrowType::Extension(
                name.clone(),
                Box::new(dtype.to_arrow()?),
                metadata.clone(),
            )),
            DataType::Embedding(..)
            | DataType::Image(..)
            | DataType::FixedShapeImage(..)
            | DataType::Tensor(..)
            | DataType::FixedShapeTensor(..) => {
                let physical = Box::new(self.to_physical());
                let logical_extension = DataType::Extension(
                    DAFT_SUPER_EXTENSION_NAME.into(),
                    physical,
                    Some(self.to_json()?),
                );
                logical_extension.to_arrow()
            }
            #[cfg(feature = "python")]
            DataType::Python => Err(DaftError::TypeError(format!(
                "Can not convert {self:?} into arrow type"
            ))),
            DataType::Unknown => Err(DaftError::TypeError(format!(
                "Can not convert {self:?} into arrow type"
            ))),
        }
    }

    pub fn to_physical(&self) -> DataType {
        use DataType::*;
        match self {
            Decimal128(..) => Int128,
            Date => Int32,
            Duration(_) | Timestamp(..) | Time(_) => Int64,
            List(child_dtype) => List(Box::new(child_dtype.to_physical())),
            FixedSizeList(child_dtype, size) => {
                FixedSizeList(Box::new(child_dtype.to_physical()), *size)
            }
            Map(child_dtype) => List(Box::new(child_dtype.to_physical())),
            Embedding(dtype, size) => FixedSizeList(Box::new(dtype.to_physical()), *size),
            Image(mode) => Struct(vec![
                Field::new(
                    "data",
                    List(Box::new(mode.map_or(DataType::UInt8, |m| m.get_dtype()))),
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
                Field::new("shape", List(Box::new(DataType::UInt64))),
            ]),
            FixedShapeTensor(dtype, shape) => FixedSizeList(
                Box::new(*dtype.clone()),
                usize::try_from(shape.iter().product::<u64>()).unwrap(),
            ),
            _ => {
                assert!(self.is_physical());
                self.clone()
            }
        }
    }

    #[inline]
    pub fn nested_dtype(&self) -> Option<&DataType> {
        match self {
            DataType::Map(dtype)
            | DataType::List(dtype)
            | DataType::FixedSizeList(dtype, _)
            | DataType::FixedShapeTensor(dtype, _)
            | DataType::Tensor(dtype) => Some(dtype),
            _ => None,
        }
    }

    #[inline]
    pub fn is_arrow(&self) -> bool {
        self.to_arrow().is_ok()
    }

    #[inline]
    pub fn is_numeric(&self) -> bool {
        match self {
             DataType::Int8
             | DataType::Int16
             | DataType::Int32
             | DataType::Int64
             | DataType::Int128
             | DataType::UInt8
             | DataType::UInt16
             | DataType::UInt32
             | DataType::UInt64
             // DataType::Float16
             | DataType::Float32
             | DataType::Float64 => true,
             DataType::Extension(_, inner, _) => inner.is_numeric(),
             _ => false
         }
    }

    #[inline]
    pub fn is_fixed_size_numeric(&self) -> bool {
        match self {
            DataType::FixedSizeList(dtype, ..)
            | DataType::Embedding(dtype, ..)
            | DataType::FixedShapeTensor(dtype, ..) => dtype.is_numeric(),
            _ => false,
        }
    }

    #[inline]
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            DataType::FixedSizeList(_, size) => Some(*size),
            DataType::Embedding(_, size) => Some(*size),
            _ => None,
        }
    }

    #[inline]
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Int128
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        )
    }

    #[inline]
    pub fn is_floating(&self) -> bool {
        matches!(
            self,
            // DataType::Float16 |
            DataType::Float32 | DataType::Float64
        )
    }

    #[inline]
    pub fn is_temporal(&self) -> bool {
        match self {
            DataType::Date | DataType::Timestamp(..) => true,
            DataType::Extension(_, inner, _) => inner.is_temporal(),
            _ => false,
        }
    }

    #[inline]
    pub fn is_tensor(&self) -> bool {
        matches!(self, DataType::Tensor(..))
    }

    #[inline]
    pub fn is_fixed_shape_tensor(&self) -> bool {
        matches!(self, DataType::FixedShapeTensor(..))
    }

    #[inline]
    pub fn is_image(&self) -> bool {
        matches!(self, DataType::Image(..))
    }

    #[inline]
    pub fn is_fixed_shape_image(&self) -> bool {
        matches!(self, DataType::FixedShapeImage(..))
    }

    #[inline]
    pub fn is_map(&self) -> bool {
        matches!(self, DataType::Map(..))
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        match self {
            DataType::Null => true,
            DataType::Extension(_, inner, _) => inner.is_null(),
            _ => false,
        }
    }

    #[inline]
    pub fn is_extension(&self) -> bool {
        matches!(self, DataType::Extension(..))
    }

    #[inline]
    pub fn is_python(&self) -> bool {
        match self {
            #[cfg(feature = "python")]
            DataType::Python => true,
            DataType::Extension(_, inner, _) => inner.is_python(),
            _ => false,
        }
    }

    pub fn estimate_size_bytes(&self) -> Option<f64> {
        const VARIABLE_TYPE_SIZE: f64 = 20.;
        const DEFAULT_LIST_LEN: f64 = 4.;

        let elem_size = match self.to_physical() {
            DataType::Null => Some(0.),
            DataType::Boolean => Some(0.125),
            DataType::Int8 => Some(1.),
            DataType::Int16 => Some(2.),
            DataType::Int32 => Some(4.),
            DataType::Int64 => Some(8.),
            DataType::Int128 => Some(16.),
            DataType::UInt8 => Some(1.),
            DataType::UInt16 => Some(2.),
            DataType::UInt32 => Some(4.),
            DataType::UInt64 => Some(8.),
            DataType::Float32 => Some(4.),
            DataType::Float64 => Some(8.),
            DataType::Utf8 => Some(VARIABLE_TYPE_SIZE),
            DataType::Binary => Some(VARIABLE_TYPE_SIZE),
            DataType::FixedSizeBinary(size) => Some(size as f64),
            DataType::FixedSizeList(dtype, len) => {
                dtype.estimate_size_bytes().map(|b| b * (len as f64))
            }
            DataType::List(dtype) => dtype.estimate_size_bytes().map(|b| b * DEFAULT_LIST_LEN),
            DataType::Struct(fields) => Some(
                fields
                    .iter()
                    .map(|f| f.dtype.estimate_size_bytes().unwrap_or(0f64))
                    .sum(),
            ),
            DataType::Extension(_, dtype, _) => dtype.estimate_size_bytes(),
            _ => None,
        };
        // add bitmap
        elem_size.map(|e| e + 0.125)
    }

    #[inline]
    pub fn is_logical(&self) -> bool {
        matches!(
            self,
            DataType::Decimal128(..)
                | DataType::Date
                | DataType::Time(..)
                | DataType::Timestamp(..)
                | DataType::Duration(..)
                | DataType::Embedding(..)
                | DataType::Image(..)
                | DataType::FixedShapeImage(..)
                | DataType::Tensor(..)
                | DataType::FixedShapeTensor(..)
                | DataType::Map(..)
        )
    }

    #[inline]
    pub fn is_physical(&self) -> bool {
        !self.is_logical()
    }

    #[inline]
    pub fn is_nested(&self) -> bool {
        let p: DataType = self.to_physical();
        matches!(
            p,
            DataType::List(..)
                | DataType::FixedSizeList(..)
                | DataType::Struct(..)
                | DataType::Map(..)
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
            // ArrowType::Float16 => DataType::Float16,
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
            ArrowType::FixedSizeBinary(size) => DataType::FixedSizeBinary(*size),
            ArrowType::Binary | ArrowType::LargeBinary => DataType::Binary,
            ArrowType::Utf8 | ArrowType::LargeUtf8 => DataType::Utf8,
            ArrowType::Decimal(precision, scale) => DataType::Decimal128(*precision, *scale),
            ArrowType::List(field) | ArrowType::LargeList(field) => {
                DataType::List(Box::new(field.as_ref().data_type().into()))
            }
            ArrowType::FixedSizeList(field, size) => {
                DataType::FixedSizeList(Box::new(field.as_ref().data_type().into()), *size)
            }
            ArrowType::Map(field, ..) => DataType::Map(Box::new(field.as_ref().data_type().into())),
            ArrowType::Struct(fields) => {
                let fields: Vec<Field> = fields.iter().map(|fld| fld.into()).collect();
                DataType::Struct(fields)
            }
            ArrowType::Extension(name, dtype, metadata) => {
                if name == DAFT_SUPER_EXTENSION_NAME {
                    if let Some(metadata) = metadata {
                        if let Ok(daft_extension) = Self::from_json(metadata.as_str()) {
                            return daft_extension;
                        }
                    }
                }
                DataType::Extension(
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
            L16 | LA16 | RGB16 | RGBA16 => DataType::UInt16,
            RGB32F | RGBA32F => DataType::Float32,
            _ => DataType::UInt8,
        }
    }
}

impl Display for DataType {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
        match self {
            DataType::List(nested) => write!(f, "List[{}]", nested),
            DataType::FixedSizeList(inner, size) => {
                write!(f, "FixedSizeList[{}; {}]", inner, size)
            }
            DataType::Map(inner, ..) => {
                write!(f, "Map[{}]", inner)
            }
            DataType::Struct(fields) => {
                let fields: String = fields
                    .iter()
                    .filter_map(|f| {
                        if f.name.is_empty() && f.dtype == DataType::Null {
                            None
                        } else {
                            Some(format!("{}: {}", f.name, f.dtype))
                        }
                    })
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(f, "Struct[{fields}]")
            }
            DataType::Embedding(inner, size) => {
                write!(f, "Embedding[{}; {}]", inner, size)
            }
            DataType::Image(mode) => {
                write!(
                    f,
                    "Image[{}]",
                    mode.map_or("MIXED".to_string(), |m| m.to_string())
                )
            }
            DataType::FixedShapeImage(mode, height, width) => {
                write!(f, "Image[{}; {} x {}]", mode, height, width)
            }
            _ => write!(f, "{self:?}"),
        }
    }
}
