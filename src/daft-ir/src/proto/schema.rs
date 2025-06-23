use super::{from_proto, from_proto_box, ProtoError, ProtoResult, ToFromProto};
use crate::{from_proto_err, proto::UNIT, to_proto_err};

/// Export daft_ir types under an `ir` namespace to concisely disambiguate domains.
#[rustfmt::skip]
mod ir {
    pub use crate::schema::*;
}

/// Export daft_proto types under a `proto` namespace because prost is heinous.
#[rustfmt::skip]
mod proto {
    pub use daft_proto::protos::daft::v1::*;
    pub use daft_proto::protos::daft::v1::data_type::Variant as DataTypeVariant;
}

/// Conversion logic for daft's Schema.
impl ToFromProto for ir::Schema {
    type Message = proto::Schema;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let fields = message
            .fields
            .into_iter()
            .map(ir::Field::from_proto)
            .collect::<ProtoResult<Vec<_>>>()?;
        Ok(Self::new(fields))
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let fields = self
            .fields()
            .iter()
            .map(|f| f.to_proto())
            .collect::<ProtoResult<Vec<_>>>()?;
        Ok(Self::Message { fields })
    }
}

/// Conversion logic for daft's DataType.
impl ToFromProto for ir::DataType {
    type Message = proto::DataType;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let variant = message
            .variant
            .ok_or_else(|| ProtoError::FromProto("Expected variant to be non-null".to_string()))?;
        Ok(match variant {
            proto::DataTypeVariant::Null(_) => Self::Null,
            proto::DataTypeVariant::Boolean(_) => Self::Boolean,
            proto::DataTypeVariant::Int8(_) => Self::Int8,
            proto::DataTypeVariant::Int16(_) => Self::Int16,
            proto::DataTypeVariant::Int32(_) => Self::Int32,
            proto::DataTypeVariant::Int64(_) => Self::Int64,
            proto::DataTypeVariant::Uint8(_) => Self::UInt8,
            proto::DataTypeVariant::Uint16(_) => Self::UInt16,
            proto::DataTypeVariant::Uint32(_) => Self::UInt32,
            proto::DataTypeVariant::Uint64(_) => Self::UInt64,
            proto::DataTypeVariant::Float32(_) => Self::Float32,
            proto::DataTypeVariant::Float64(_) => Self::Float64,
            proto::DataTypeVariant::Decimal128(decimal128) => {
                Self::Decimal128(decimal128.precision as usize, decimal128.scale as usize)
            }
            proto::DataTypeVariant::Timestamp(timestamp) => {
                let time_unit = ir::TimeUnit::from_proto(timestamp.unit)?;
                Self::Timestamp(time_unit, timestamp.timezone)
            }
            proto::DataTypeVariant::Date(_) => Self::Date,
            proto::DataTypeVariant::Time(time) => {
                let time_unit = ir::TimeUnit::from_proto(time.unit)?;
                Self::Time(time_unit)
            }
            proto::DataTypeVariant::Duration(duration) => {
                let time_unit = ir::TimeUnit::from_proto(duration.unit)?;
                Self::Duration(time_unit)
            }
            proto::DataTypeVariant::Interval(_) => Self::Interval,
            proto::DataTypeVariant::Binary(_) => Self::Binary,
            proto::DataTypeVariant::FixedSizeBinary(fixed_size_binary) => {
                Self::FixedSizeBinary(fixed_size_binary.size as usize)
            }
            proto::DataTypeVariant::Utf8(_) => Self::Utf8,
            proto::DataTypeVariant::FixedSizeList(fixed_size_list) => {
                let element_type = from_proto_box(fixed_size_list.element_type)?;
                let size: usize = fixed_size_list.size as usize;
                Self::FixedSizeList(element_type, size)
            }
            proto::DataTypeVariant::List(list) => {
                let element_type = from_proto_box(list.element_type)?;
                Self::List(element_type)
            }
            proto::DataTypeVariant::Struct(struct_) => {
                let fields = struct_
                    .fields
                    .into_iter()
                    .map(ir::Field::from_proto)
                    .collect::<ProtoResult<Vec<_>>>()?;
                Self::Struct(fields)
            }
            proto::DataTypeVariant::Map(map) => {
                let key_type = from_proto_box(map.key_type)?;
                let val_type = from_proto_box(map.value_type)?;
                Self::Map {
                    key: key_type,
                    value: val_type,
                }
            }
            proto::DataTypeVariant::Extension(_) => {
                from_proto_err!("The extension type is not supported.")
            }
            proto::DataTypeVariant::Embedding(embedding) => {
                let element_type = from_proto_box(embedding.element_type)?;
                let size = embedding.size as usize;
                Self::Embedding(element_type, size)
            }
            proto::DataTypeVariant::Image(image) => {
                let mode = image.mode.map(ir::ImageMode::from_proto).transpose()?;
                Self::Image(mode)
            }
            proto::DataTypeVariant::FixedShapeImage(fixed_shape_image) => {
                let mode = ir::ImageMode::from_proto(fixed_shape_image.mode)?;
                Self::FixedShapeImage(mode, fixed_shape_image.height, fixed_shape_image.width)
            }
            proto::DataTypeVariant::Tensor(tensor) => {
                let element_type = from_proto_box(tensor.element_type)?;
                Self::Tensor(element_type)
            }
            proto::DataTypeVariant::FixedShapeTensor(fixed_shape_tensor) => {
                let element_type = from_proto_box(fixed_shape_tensor.element_type)?;
                Self::FixedShapeTensor(element_type, fixed_shape_tensor.shape)
            }
            proto::DataTypeVariant::SparseTensor(sparse_tensor) => {
                let element_type = from_proto_box(sparse_tensor.element_type)?;
                Self::SparseTensor(element_type, sparse_tensor.indices_offset)
            }
            proto::DataTypeVariant::FixedShapeSparseTensor(fixed_shape_sparse_tensor) => {
                let element_type = from_proto_box(fixed_shape_sparse_tensor.element_type)?;
                Self::FixedShapeSparseTensor(
                    element_type,
                    fixed_shape_sparse_tensor.shape,
                    fixed_shape_sparse_tensor.indices_offset,
                )
            }
            proto::DataTypeVariant::Unknown(_) => Self::Unknown,
            #[cfg(feature = "python")]
            proto::DataTypeVariant::Python(_) => Self::Python,
            #[cfg(not(feature = "python"))]
            proto::DataTypeVariant::Python(_) => todo!("no python!"),
        })
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let variant = match self {
            Self::Null => proto::DataTypeVariant::Null(UNIT),
            Self::Boolean => proto::DataTypeVariant::Boolean(UNIT),
            Self::Int8 => proto::DataTypeVariant::Int8(UNIT),
            Self::Int16 => proto::DataTypeVariant::Int16(UNIT),
            Self::Int32 => proto::DataTypeVariant::Int32(UNIT),
            Self::Int64 => proto::DataTypeVariant::Int64(UNIT),
            Self::UInt8 => proto::DataTypeVariant::Uint8(UNIT),
            Self::UInt32 => proto::DataTypeVariant::Uint32(UNIT),
            Self::UInt64 => proto::DataTypeVariant::Uint64(UNIT),
            Self::UInt16 => proto::DataTypeVariant::Uint16(UNIT),
            Self::Float32 => proto::DataTypeVariant::Float32(UNIT),
            Self::Float64 => proto::DataTypeVariant::Float64(UNIT),
            Self::Decimal128(precision, scale) => {
                proto::DataTypeVariant::Decimal128(proto::data_type::Decimal128 {
                    precision: *precision as u64,
                    scale: *scale as u64,
                })
            }
            Self::Timestamp(time_unit, timezone) => {
                proto::DataTypeVariant::Timestamp(proto::data_type::Timestamp {
                    unit: time_unit.to_proto()?,
                    timezone: timezone.clone(),
                })
            }
            Self::Date => proto::DataTypeVariant::Date(UNIT),
            Self::Time(time_unit) => proto::DataTypeVariant::Time(proto::data_type::Time {
                unit: time_unit.to_proto()?,
            }),
            Self::Duration(time_unit) => {
                proto::DataTypeVariant::Duration(proto::data_type::Duration {
                    unit: time_unit.to_proto()?,
                })
            }
            Self::Interval => proto::DataTypeVariant::Interval(UNIT),
            Self::Binary => proto::DataTypeVariant::Binary(UNIT),
            Self::FixedSizeBinary(size) => {
                proto::DataTypeVariant::FixedSizeBinary(proto::data_type::FixedSizeBinary {
                    size: *size as u64,
                })
            }
            Self::Utf8 => proto::DataTypeVariant::Utf8(UNIT),
            Self::FixedSizeList(data_type, size) => proto::DataTypeVariant::FixedSizeList(
                proto::data_type::FixedSizeList {
                    element_type: Some(data_type.to_proto()?.into()),
                    size: *size as u64,
                }
                .into(),
            ),
            Self::List(data_type) => proto::DataTypeVariant::List(
                proto::data_type::List {
                    element_type: Some(data_type.to_proto()?.into()),
                }
                .into(),
            ),
            Self::Struct(fields) => proto::DataTypeVariant::Struct(proto::data_type::Struct {
                fields: fields
                    .iter()
                    .map(|f| f.to_proto())
                    .collect::<ProtoResult<Vec<_>>>()?,
            }),
            Self::Map { key, value } => proto::DataTypeVariant::Map(
                proto::data_type::Map {
                    key_type: Some(key.to_proto()?.into()),
                    value_type: Some(value.to_proto()?.into()),
                }
                .into(),
            ),
            Self::Extension(..) => {
                to_proto_err!("The extension type is not supported.")
            }
            Self::Embedding(data_type, size) => proto::DataTypeVariant::Embedding(
                proto::data_type::Embedding {
                    element_type: Some(data_type.to_proto()?.into()),
                    size: *size as u64,
                }
                .into(),
            ),
            Self::Image(image_mode) => {
                let image_mode = image_mode.map(|m| m.to_proto()).transpose()?;
                let image = proto::data_type::Image { mode: image_mode };
                proto::DataTypeVariant::Image(image)
            }
            Self::FixedShapeImage(image_mode, height, width) => {
                proto::DataTypeVariant::FixedShapeImage(proto::data_type::FixedShapeImage {
                    mode: image_mode.to_proto()?,
                    height: *height,
                    width: *width,
                })
            }
            Self::Tensor(data_type) => proto::DataTypeVariant::Tensor(
                proto::data_type::Tensor {
                    element_type: Some(data_type.to_proto()?.into()),
                }
                .into(),
            ),
            Self::FixedShapeTensor(data_type, shape) => proto::DataTypeVariant::FixedShapeTensor(
                proto::data_type::FixedShapeTensor {
                    element_type: Some(data_type.to_proto()?.into()),
                    shape: shape.clone(),
                }
                .into(),
            ),
            Self::SparseTensor(data_type, indices_offset) => proto::DataTypeVariant::SparseTensor(
                proto::data_type::SparseTensor {
                    element_type: Some(data_type.to_proto()?.into()),
                    indices_offset: *indices_offset,
                }
                .into(),
            ),
            Self::FixedShapeSparseTensor(data_type, shape, indices_offset) => {
                proto::DataTypeVariant::FixedShapeSparseTensor(
                    proto::data_type::FixedShapeSparseTensor {
                        element_type: Some(data_type.to_proto()?.into()),
                        shape: shape.clone(),
                        indices_offset: *indices_offset,
                    }
                    .into(),
                )
            }
            Self::Unknown => proto::DataTypeVariant::Unknown(UNIT),
            #[cfg(feature = "python")]
            Self::Python => proto::DataTypeVariant::Python(UNIT),
        };
        Ok(Self::Message {
            variant: Some(variant),
        })
    }
}

impl ToFromProto for ir::Field {
    type Message = proto::Field;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let name = message.name;
        let data_type = from_proto(message.data_type)?;
        Ok(Self::new(name, data_type))
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        Ok(Self::Message {
            name: self.name.clone(),
            data_type: self.dtype.to_proto()?.into(),
        })
    }
}

impl ToFromProto for ir::TimeUnit {
    type Message = i32;

    fn from_proto(message: Self::Message) -> ProtoResult<Self> {
        let time_unit = proto::TimeUnit::try_from(message).unwrap_or(proto::TimeUnit::Unspecified);
        Ok(match time_unit {
            proto::TimeUnit::Unspecified => Self::Nanoseconds, // default fallback
            proto::TimeUnit::Nanoseconds => Self::Nanoseconds,
            proto::TimeUnit::Microseconds => Self::Microseconds,
            proto::TimeUnit::Seconds => Self::Seconds,
        })
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let time_unit = match self {
            Self::Nanoseconds => proto::TimeUnit::Nanoseconds,
            Self::Microseconds => proto::TimeUnit::Microseconds,
            Self::Milliseconds => to_proto_err!("No milliseconds!?"),
            Self::Seconds => proto::TimeUnit::Seconds,
        };
        Ok(time_unit as i32)
    }
}

impl ToFromProto for ir::ImageMode {
    type Message = i32;

    fn from_proto(message: Self::Message) -> ProtoResult<Self> {
        let image_mode =
            proto::ImageMode::try_from(message).unwrap_or(proto::ImageMode::Unspecified);
        Ok(match image_mode {
            proto::ImageMode::Unspecified => Self::L, // default fallback
            proto::ImageMode::L => Self::L,
            proto::ImageMode::La => Self::LA,
            proto::ImageMode::Rgb => Self::RGB,
            proto::ImageMode::Rgba => Self::RGBA,
            proto::ImageMode::L16 => Self::L16,
            proto::ImageMode::La16 => Self::LA16,
            proto::ImageMode::Rgb16 => Self::RGB16,
            proto::ImageMode::Rgba16 => Self::RGBA16,
            proto::ImageMode::Rgb32f => Self::RGB32F,
            proto::ImageMode::Rgba32f => Self::RGBA32F,
        })
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let image_mode = match self {
            Self::L => proto::ImageMode::L,
            Self::LA => proto::ImageMode::La,
            Self::RGB => proto::ImageMode::Rgb,
            Self::RGBA => proto::ImageMode::Rgba,
            Self::L16 => proto::ImageMode::L16,
            Self::LA16 => proto::ImageMode::La16,
            Self::RGB16 => proto::ImageMode::Rgb16,
            Self::RGBA16 => proto::ImageMode::Rgba16,
            Self::RGB32F => proto::ImageMode::Rgb32f,
            Self::RGBA32F => proto::ImageMode::Rgba32f,
        };
        Ok(image_mode as i32)
    }
}
