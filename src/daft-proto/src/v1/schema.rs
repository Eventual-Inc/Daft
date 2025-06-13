use daft_ir::schema::DataType;

use crate::{
    protos::{from_proto, FromToProto, ProtoResult},
    to_proto_err,
};

/// Export daft_ir types under an `ir` namespace to concisely disambiguate domains.
#[rustfmt::skip]
mod ir {
    pub use daft_ir::schema::daft_schema::image_mode::ImageMode;
    pub use daft_ir::schema::daft_schema::time_unit::TimeUnit;
    pub use daft_ir::schema::*;
}

/// Export daft_proto types under a `proto` namespace because prost is heinous.
#[rustfmt::skip]
mod proto {
    pub use crate::protos::schema::data_type::Variant as DataTypeVariant;
    pub use crate::protos::schema::data_type::*;
    pub use crate::protos::schema::*;
}

/// Conversion logic for daft's Schema.
impl FromToProto for ir::Schema {
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
        Ok(ir::Schema::new(fields))
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
impl FromToProto for ir::DataType {
    type Message = proto::DataType;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let variant = message.variant.ok_or_else(|| {
            crate::protos::ProtoError::FromProtoError("Expected variant to be non-null".to_string())
        })?;
        Ok(match variant {
            proto::DataTypeVariant::Null(_) => ir::DataType::Null,
            proto::DataTypeVariant::Boolean(_) => ir::DataType::Boolean,
            proto::DataTypeVariant::Int8(_) => ir::DataType::Int8,
            proto::DataTypeVariant::Int16(_) => ir::DataType::Int16,
            proto::DataTypeVariant::Int32(_) => ir::DataType::Int32,
            proto::DataTypeVariant::Int64(_) => ir::DataType::Int64,
            proto::DataTypeVariant::Uint8(_) => ir::DataType::UInt8,
            proto::DataTypeVariant::Uint16(_) => ir::DataType::UInt16,
            proto::DataTypeVariant::Uint32(_) => ir::DataType::UInt32,
            proto::DataTypeVariant::Uint64(_) => ir::DataType::UInt64,
            proto::DataTypeVariant::Float32(_) => ir::DataType::Float32,
            proto::DataTypeVariant::Float64(_) => ir::DataType::Float64,
            proto::DataTypeVariant::Decimal128(decimal128) => {
                todo!();
                // ir::DataType::Decimal128(decimal128.precision as i32, decimal128.scale as i32)
            }
            proto::DataTypeVariant::Timestamp(timestamp) => {
                let time_unit = ir::TimeUnit::from_proto(timestamp.unit)?;
                ir::DataType::Timestamp(time_unit, timestamp.timezone)
            }
            proto::DataTypeVariant::Date(_) => ir::DataType::Date,
            proto::DataTypeVariant::Time(time) => {
                let time_unit = ir::TimeUnit::from_proto(time.unit)?;
                ir::DataType::Time(time_unit)
            }
            proto::DataTypeVariant::Duration(duration) => {
                let time_unit = ir::TimeUnit::from_proto(duration.unit)?;
                ir::DataType::Duration(time_unit)
            }
            proto::DataTypeVariant::Interval(_) => ir::DataType::Interval,
            proto::DataTypeVariant::Binary(_) => ir::DataType::Binary,
            proto::DataTypeVariant::FixedSizeBinary(fixed_size_binary) => {
                ir::DataType::FixedSizeBinary(fixed_size_binary.size as usize)
            }
            proto::DataTypeVariant::Utf8(_) => ir::DataType::Utf8,
            proto::DataTypeVariant::FixedSizeList(fixed_size_list) => {
                todo!()
                // let element_type = from_proto(fixed_size_list.element_type)?.into();
                // ir::DataType::FixedSizeList(element_type, fixed_size_list.size as usize)
            }
            proto::DataTypeVariant::List(list) => {
                todo!()
                // let element_type: Box<ir::DataType> = from_proto(list.element_type.unwrap())?.into();
                // ir::DataType::List(element_type)
            }
            proto::DataTypeVariant::Struct(struct_) => {
                let fields = struct_
                    .fields
                    .into_iter()
                    .map(ir::Field::from_proto)
                    .collect::<ProtoResult<Vec<_>>>()?;
                ir::DataType::Struct(fields)
            }
            proto::DataTypeVariant::Map(map) => {
                todo!()
                // ir::DataType::Map {
                //     key: from_proto(map.key_type)?.into(),
                //     value: from_proto(map.value_type)?.into(),
                // }
            }
            proto::DataTypeVariant::Extension(_) => {
                return Err(crate::protos::ProtoError::FromProtoError(
                    "The extension type is not supported.".to_string(),
                ))
            }
            proto::DataTypeVariant::Embedding(embedding) => {
                todo!()
                // let element_type: Box<ir::DataType> = from_proto(embedding.element_type)?.into();
                // ir::DataType::Embedding(element_type, embedding.size as i32)
            }
            proto::DataTypeVariant::Image(image) => {
                let mode = image.mode.map(ir::ImageMode::from_proto).transpose()?;
                ir::DataType::Image(mode)
            }
            proto::DataTypeVariant::FixedShapeImage(fixed_shape_image) => {
                let mode = ir::ImageMode::from_proto(fixed_shape_image.mode)?;
                ir::DataType::FixedShapeImage(
                    mode,
                    fixed_shape_image.height,
                    fixed_shape_image.width,
                )
            }
            proto::DataTypeVariant::Tensor(tensor) => {
                todo!()
                // let element_type = from_proto(tensor.element_type)?.into();
                // ir::DataType::Tensor(element_type)
            }
            proto::DataTypeVariant::FixedShapeTensor(fixed_shape_tensor) => {
                todo!()
                // let element_type = from_proto(fixed_shape_tensor.element_type)?.into();
                // ir::DataType::FixedShapeTensor(element_type, fixed_shape_tensor.shape)
            }
            proto::DataTypeVariant::SparseTensor(sparse_tensor) => {
                todo!()
                // let element_type = from_proto(sparse_tensor.element_type)?.into();
                // ir::DataType::SparseTensor(element_type, sparse_tensor.indices_offset)
            }
            proto::DataTypeVariant::FixedShapeSparseTensor(fixed_shape_sparse_tensor) => {
                todo!()
                // let element_type = from_proto(fixed_shape_sparse_tensor.element_type)?.into();
                // ir::DataType::FixedShapeSparseTensor(
                //     element_type,
                //     fixed_shape_sparse_tensor.shape,
                //     fixed_shape_sparse_tensor.indices_offset,
                // )
            }
            proto::DataTypeVariant::Unknown(_) => ir::DataType::Unknown,
            #[cfg(feature = "python")]
            proto::DataTypeVariant::Python(_) => ir::DataType::Python,
            #[cfg(not(feature = "python"))]
            proto::DataTypeVariant::Python(_) => todo!("no python!"),
        })
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let variant = match self {
            ir::DataType::Null => proto::DataTypeVariant::Null(true),
            ir::DataType::Boolean => proto::DataTypeVariant::Boolean(true),
            ir::DataType::Int8 => proto::DataTypeVariant::Int8(true),
            ir::DataType::Int16 => proto::DataTypeVariant::Int16(true),
            ir::DataType::Int32 => proto::DataTypeVariant::Int32(true),
            ir::DataType::Int64 => proto::DataTypeVariant::Int64(true),
            ir::DataType::UInt8 => proto::DataTypeVariant::Uint8(true),
            ir::DataType::UInt32 => proto::DataTypeVariant::Uint32(true),
            ir::DataType::UInt64 => proto::DataTypeVariant::Uint64(true),
            ir::DataType::UInt16 => proto::DataTypeVariant::Uint16(true),
            ir::DataType::Float32 => proto::DataTypeVariant::Float32(true),
            ir::DataType::Float64 => proto::DataTypeVariant::Float64(true),
            ir::DataType::Decimal128(precision, scale) => {
                proto::DataTypeVariant::Decimal128(proto::Decimal128 {
                    precision: *precision as u64,
                    scale: *scale as u64,
                })
            }
            ir::DataType::Timestamp(time_unit, timezone) => {
                proto::DataTypeVariant::Timestamp(proto::Timestamp {
                    unit: time_unit.to_proto()?,
                    timezone: timezone.clone(),
                })
            }
            ir::DataType::Date => proto::DataTypeVariant::Date(true),
            ir::DataType::Time(time_unit) => proto::DataTypeVariant::Time(proto::Time {
                unit: time_unit.to_proto()?,
            }),
            ir::DataType::Duration(time_unit) => {
                proto::DataTypeVariant::Duration(proto::Duration {
                    unit: time_unit.to_proto()?,
                })
            }
            ir::DataType::Interval => proto::DataTypeVariant::Interval(true),
            ir::DataType::Binary => proto::DataTypeVariant::Binary(true),
            ir::DataType::FixedSizeBinary(size) => {
                proto::DataTypeVariant::FixedSizeBinary(proto::FixedSizeBinary {
                    size: *size as u64,
                })
            }
            ir::DataType::Utf8 => proto::DataTypeVariant::Utf8(true),
            ir::DataType::FixedSizeList(data_type, size) => proto::DataTypeVariant::FixedSizeList(
                proto::FixedSizeList {
                    element_type: Some(data_type.to_proto()?.into()),
                    size: *size as u64,
                }
                .into(),
            ),
            ir::DataType::List(data_type) => proto::DataTypeVariant::List(
                proto::List {
                    element_type: Some(data_type.to_proto()?.into()),
                }
                .into(),
            ),
            ir::DataType::Struct(fields) => proto::DataTypeVariant::Struct(proto::Struct {
                fields: fields
                    .iter()
                    .map(|f| f.to_proto())
                    .collect::<ProtoResult<Vec<_>>>()?,
            }),
            ir::DataType::Map { key, value } => proto::DataTypeVariant::Map(
                proto::Map {
                    key_type: Some(key.to_proto()?.into()),
                    value_type: Some(value.to_proto()?.into()),
                }
                .into(),
            ),
            ir::DataType::Extension(name, data_type, metadata) => {
                to_proto_err!("The extension type is not supported.")
            }
            ir::DataType::Embedding(data_type, size) => proto::DataTypeVariant::Embedding(
                proto::Embedding {
                    element_type: Some(data_type.to_proto()?.into()),
                    size: *size as u64,
                }
                .into(),
            ),
            ir::DataType::Image(image_mode) => {
                let image_mode = image_mode.map(|m| m.to_proto()).transpose()?;
                let image = proto::Image { mode: image_mode };
                proto::DataTypeVariant::Image(image)
            }
            ir::DataType::FixedShapeImage(image_mode, height, width) => {
                proto::DataTypeVariant::FixedShapeImage(proto::FixedShapeImage {
                    mode: image_mode.to_proto()?,
                    height: *height,
                    width: *width,
                })
            }
            ir::DataType::Tensor(data_type) => proto::DataTypeVariant::Tensor(
                proto::Tensor {
                    element_type: Some(data_type.to_proto()?.into()),
                }
                .into(),
            ),
            ir::DataType::FixedShapeTensor(data_type, shape) => {
                proto::DataTypeVariant::FixedShapeTensor(
                    proto::FixedShapeTensor {
                        element_type: Some(data_type.to_proto()?.into()),
                        shape: shape.clone(),
                    }
                    .into(),
                )
            }
            ir::DataType::SparseTensor(data_type, indices_offset) => {
                proto::DataTypeVariant::SparseTensor(
                    proto::SparseTensor {
                        element_type: Some(data_type.to_proto()?.into()),
                        indices_offset: *indices_offset,
                    }
                    .into(),
                )
            }
            ir::DataType::FixedShapeSparseTensor(data_type, shape, indices_offset) => {
                proto::DataTypeVariant::FixedShapeSparseTensor(
                    proto::FixedShapeSparseTensor {
                        element_type: Some(data_type.to_proto()?.into()),
                        shape: shape.clone(),
                        indices_offset: *indices_offset,
                    }
                    .into(),
                )
            }
            ir::DataType::Unknown => proto::DataTypeVariant::Unknown(true),
            #[cfg(feature = "python")]
            ir::DataType::Python => proto::DataTypeVariant::Python(true),
        };
        Ok(Self::Message {
            variant: Some(variant),
        })
    }
}

impl FromToProto for ir::Field {
    type Message = proto::Field;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let name = message.name;
        let data_type: DataType = from_proto(message.data_type)?;
        Ok(ir::Field::new(name, data_type))
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        Ok(Self::Message {
            name: self.name.clone(),
            data_type: self.dtype.to_proto()?.into(),
        })
    }
}

impl FromToProto for ir::TimeUnit {
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

impl FromToProto for ir::ImageMode {
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
