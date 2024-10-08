use std::fmt::Write;

use arrow2::datatypes::DataType as ArrowType;
use common_error::{DaftError, DaftResult};
use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::{field::Field, image_mode::ImageMode, time_unit::TimeUnit};

pub type DaftDataType = DataType;

#[derive(Clone, Debug, Display, PartialEq, Eq, Serialize, Deserialize, Hash)]
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

    /// A [`f32`]
    Float32,

    /// A [`f64`]
    Float64,

    /// Fixed-precision decimal type.
    /// TODO: allow negative scale once Arrow2 allows it: https://github.com/jorgecarleitao/arrow2/issues/1518
    #[display("Decimal(precision={_0}, scale={_1})")]
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
    #[display("Timestamp({_0}, {_1:?})")]
    Timestamp(TimeUnit, Option<String>),

    /// An [`i32`] representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days.
    Date,

    /// A 64-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    /// Only [`TimeUnit::Microsecond`] and [`TimeUnit::Nanosecond`] are supported on this variant.
    #[display("Time({_0})")]
    Time(TimeUnit),

    /// Measure of elapsed time. This elapsed time is a physical duration (i.e. 1s as defined in S.I.)
    #[display("Duration[{_0}]")]
    Duration(TimeUnit),

    /// Opaque binary data of variable length whose offsets are represented as [`i64`].
    Binary,

    /// Opaque binary data of fixed size. Enum parameter specifies the number of bytes per value.
    #[display("FixedSizeBinary[{_0}]")]
    FixedSizeBinary(usize),

    /// A variable-length UTF-8 encoded string whose offsets are represented as [`i64`].
    Utf8,

    /// A list of some logical data type with a fixed number of elements.
    #[display("FixedSizeList[{_0}; {_1}]")]
    FixedSizeList(Box<DataType>, usize),

    /// A list of some logical data type whose offsets are represented as [`i64`].
    #[display("List[{_0}]")]
    List(Box<DataType>),

    /// A nested [`DataType`] with a given number of [`Field`]s.
    #[display("Struct[{}]", format_struct(_0)?)]
    Struct(Vec<Field>),

    /// A nested [`DataType`] that is represented as List<entries: Struct<key: K, value: V>>.
    #[display("Map[{key}: {value}]")]
    Map {
        key: Box<DataType>,
        value: Box<DataType>,
    },

    /// Extension type.
    #[display("{_1}")]
    Extension(String, Box<DataType>, Option<String>),

    // Non-ArrowTypes:
    /// A logical type for embeddings.
    #[display("Embedding[{_0}; {_1}]")]
    Embedding(Box<DataType>, usize),

    /// A logical type for images with variable shapes.
    #[display("Image[{}]", _0.map_or_else(|| "MIXED".to_string(), |mode| mode.to_string()))]
    Image(Option<ImageMode>),

    /// A logical type for images with the same size (height x width).
    #[display("Image[{_0}; {_1} x {_2}]")]
    FixedShapeImage(ImageMode, u32, u32),

    /// A logical type for tensors with variable shapes.
    #[display("Tensor({_0})")]
    Tensor(Box<DataType>),

    /// A logical type for tensors with the same shape.
    #[display("FixedShapeTensor[{_0}; {_1:?}]")]
    FixedShapeTensor(Box<DataType>, Vec<u64>),

    /// A logical type for sparse tensors with variable shapes.
    #[display("SparseTensor({_0})")]
    SparseTensor(Box<DataType>),

    /// A logical type for sparse tensors with the same shape.
    #[display("FixedShapeSparseTensor[{_0}; {_1:?}]")]
    FixedShapeSparseTensor(Box<DataType>, Vec<u64>),

    #[cfg(feature = "python")]
    Python,

    Unknown,
}

fn format_struct(fields: &[Field]) -> std::result::Result<String, std::fmt::Error> {
    let mut f = String::default();
    for (index, field) in fields.iter().enumerate() {
        if index != 0 {
            write!(&mut f, ", ")?;
        }
        if !(field.name.is_empty() && field.dtype.is_null()) {
            write!(&mut f, "{}: {}", field.name, field.dtype)?;
        }
    }
    Ok(f)
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

    pub fn to_arrow(&self) -> DaftResult<ArrowType> {
        match self {
            Self::Null => Ok(ArrowType::Null),
            Self::Boolean => Ok(ArrowType::Boolean),
            Self::Int8 => Ok(ArrowType::Int8),
            Self::Int16 => Ok(ArrowType::Int16),
            Self::Int32 => Ok(ArrowType::Int32),
            Self::Int64 => Ok(ArrowType::Int64),
            // Must maintain same default mapping as Arrow2, otherwise this will throw errors in
            // DataArray<Int128Type>::new() which makes strong assumptions about the arrow/Daft types
            // https://github.com/jorgecarleitao/arrow2/blob/b0734542c2fef5d2d0c7b6ffce5d094de371168a/src/datatypes/mod.rs#L493
            Self::Int128 => Ok(ArrowType::Decimal(32, 32)),
            Self::UInt8 => Ok(ArrowType::UInt8),
            Self::UInt16 => Ok(ArrowType::UInt16),
            Self::UInt32 => Ok(ArrowType::UInt32),
            Self::UInt64 => Ok(ArrowType::UInt64),
            // DataType::Float16 => Ok(ArrowType::Float16),
            Self::Float32 => Ok(ArrowType::Float32),
            Self::Float64 => Ok(ArrowType::Float64),
            Self::Decimal128(precision, scale) => Ok(ArrowType::Decimal(*precision, *scale)),
            Self::Timestamp(unit, timezone) => {
                Ok(ArrowType::Timestamp(unit.to_arrow(), timezone.clone()))
            }
            Self::Date => Ok(ArrowType::Date32),
            Self::Time(unit) => Ok(ArrowType::Time64(unit.to_arrow())),
            Self::Duration(unit) => Ok(ArrowType::Duration(unit.to_arrow())),
            Self::Binary => Ok(ArrowType::LargeBinary),
            Self::FixedSizeBinary(size) => Ok(ArrowType::FixedSizeBinary(*size)),
            Self::Utf8 => Ok(ArrowType::LargeUtf8),
            Self::FixedSizeList(child_dtype, size) => Ok(ArrowType::FixedSizeList(
                Box::new(arrow2::datatypes::Field::new(
                    "item",
                    child_dtype.to_arrow()?,
                    true,
                )),
                *size,
            )),
            Self::List(field) => Ok(ArrowType::LargeList(Box::new(
                arrow2::datatypes::Field::new("item", field.to_arrow()?, true),
            ))),
            Self::Map { key, value } => {
                let struct_type = ArrowType::Struct(vec![
                    // We never allow null keys in maps for several reasons:
                    // 1. Null typically represents the absence of a value, which doesn't make sense for a key.
                    // 2. Null comparisons can be problematic (similar to how f64::NAN != f64::NAN).
                    // 3. It maintains consistency with common map implementations in arrow (no null keys).
                    // 4. It simplifies map operations
                    //
                    // This decision aligns with the thoughts of team members like Jay and Sammy, who argue that:
                    // - Nulls in keys could lead to unintuitive behavior
                    // - If users need to count or group by null values, they can use other constructs like
                    //   group_by operations on non-map types, which offer more explicit control.
                    //
                    // By disallowing null keys, we encourage more robust data modeling practices and
                    // provide a clearer semantic meaning for map types in our system.
                    arrow2::datatypes::Field::new("key", key.to_arrow()?, true),
                    arrow2::datatypes::Field::new("value", value.to_arrow()?, true),
                ]);

                let struct_field = arrow2::datatypes::Field::new("entries", struct_type, true);

                Ok(ArrowType::map(struct_field, false))
            }
            Self::Struct(fields) => Ok({
                let fields = fields
                    .iter()
                    .map(|f| f.to_arrow())
                    .collect::<DaftResult<Vec<arrow2::datatypes::Field>>>()?;
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
            | Self::FixedShapeSparseTensor(..) => {
                let physical = Box::new(self.to_physical());
                let logical_extension = Self::Extension(
                    DAFT_SUPER_EXTENSION_NAME.into(),
                    physical,
                    Some(self.to_json()?),
                );
                logical_extension.to_arrow()
            }
            #[cfg(feature = "python")]
            Self::Python => Err(DaftError::TypeError(format!(
                "Can not convert {self:?} into arrow type"
            ))),
            Self::Unknown => Err(DaftError::TypeError(format!(
                "Can not convert {self:?} into arrow type"
            ))),
        }
    }

    pub fn to_physical(&self) -> Self {
        use DataType::*;
        match self {
            Decimal128(..) => Int128,
            Date => Int32,
            Duration(_) | Timestamp(..) | Time(_) => Int64,
            List(child_dtype) => List(Box::new(child_dtype.to_physical())),
            FixedSizeList(child_dtype, size) => {
                FixedSizeList(Box::new(child_dtype.to_physical()), *size)
            }
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
            SparseTensor(dtype) => Struct(vec![
                Field::new("values", List(Box::new(*dtype.clone()))),
                Field::new("indices", List(Box::new(Self::UInt64))),
                Field::new("shape", List(Box::new(Self::UInt64))),
            ]),
            FixedShapeSparseTensor(dtype, _) => Struct(vec![
                Field::new("values", List(Box::new(*dtype.clone()))),
                Field::new("indices", List(Box::new(Self::UInt64))),
            ]),
            _ => {
                assert!(self.is_physical());
                self.clone()
            }
        }
    }

    #[inline]
    pub fn is_arrow(&self) -> bool {
        self.to_arrow().is_ok()
    }

    #[inline]
    pub fn is_numeric(&self) -> bool {
        match self {
            Self::Int8
            | Self::Int16
            | Self::Int32
            | Self::Int64
            | Self::Int128
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
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            Self::FixedSizeList(_, size) => Some(*size),
            Self::Embedding(_, size) => Some(*size),
            _ => None,
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
                | Self::Int128
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
                )))
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
            Self::Int128 => Some(16.),
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
            Self::Decimal128(..)
                | Self::Date
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
}

#[expect(
    clippy::fallible_impl_from,
    reason = "https://github.com/Eventual-Inc/Daft/issues/3015"
)]
impl From<&ArrowType> for DataType {
    fn from(item: &ArrowType) -> Self {
        let result = match item {
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
                if name == DAFT_SUPER_EXTENSION_NAME {
                    if let Some(metadata) = metadata {
                        if let Ok(daft_extension) = Self::from_json(metadata.as_str()) {
                            return daft_extension;
                        }
                    }
                }
                Self::Extension(
                    name.clone(),
                    Box::new(dtype.as_ref().into()),
                    metadata.clone(),
                )
            }

            _ => panic!("DataType :{item:?} is not supported"),
        };

        result
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
