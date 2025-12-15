// Re-export arrow2::* modules for centralized access
pub use arrow_array::temporal_conversions;
pub use arrow_schema::ArrowError;
pub use arrow2::{array, chunk, compute, error, io, offset, scalar, trusted_len, types};

pub mod buffer {
    pub use arrow_buffer::{BooleanBufferBuilder, NullBuffer, NullBufferBuilder};
    pub use arrow2::buffer::*;

    /// Convert an arrow_buffer::buffer::NullBuffer to an arrow2::bitmap::Bitmap.
    ///
    /// This is a wrapper around arrow2::bitmap::Bitmap::from_null_buffer
    /// so we can easily replace it with a no-op in the future.
    pub fn from_null_buffer(value: arrow_buffer::buffer::NullBuffer) -> arrow2::bitmap::Bitmap {
        arrow2::bitmap::Bitmap::from_null_buffer(value)
    }

    /// Convert an Option<arrow_buffer::buffer::NullBuffer> to an Option<arrow2::bitmap::Bitmap>.
    ///
    /// This is a wrapper around arrow2::bitmap::Bitmap::from_null_buffer
    /// so we can easily replace it with a no-op in the future.
    pub fn wrap_null_buffer(
        value: Option<arrow_buffer::buffer::NullBuffer>,
    ) -> Option<arrow2::bitmap::Bitmap> {
        value.map(arrow2::bitmap::Bitmap::from_null_buffer)
    }
}

/// Explicitly isolate old arrow2::bitmap code to quickly remove in future
pub mod bitmap {
    // Uses come from arrow2::array::BooleanArray values buffer
    // And direct access to arrow2::array::Array objects
    pub use arrow2::bitmap::*;
}

pub mod datatypes {
    use std::{collections::HashMap, sync::Arc};

    pub use arrow2::datatypes::*;

    #[deprecated(note = "use arrow instead of arrow2")]
    pub fn arrow2_field_to_arrow(field: Field) -> arrow_schema::Field {
        use arrow_schema::{Field as ArrowField, UnionFields};

        let dtype = match field.data_type {
            DataType::Null => arrow_schema::DataType::Null,
            DataType::Boolean => arrow_schema::DataType::Boolean,
            DataType::Int8 => arrow_schema::DataType::Int8,
            DataType::Int16 => arrow_schema::DataType::Int16,
            DataType::Int32 => arrow_schema::DataType::Int32,
            DataType::Int64 => arrow_schema::DataType::Int64,
            DataType::UInt8 => arrow_schema::DataType::UInt8,
            DataType::UInt16 => arrow_schema::DataType::UInt16,
            DataType::UInt32 => arrow_schema::DataType::UInt32,
            DataType::UInt64 => arrow_schema::DataType::UInt64,
            DataType::Float16 => arrow_schema::DataType::Float16,
            DataType::Float32 => arrow_schema::DataType::Float32,
            DataType::Float64 => arrow_schema::DataType::Float64,
            DataType::Timestamp(unit, tz) => {
                arrow_schema::DataType::Timestamp(unit.into(), tz.map(Into::into))
            }
            DataType::Date32 => arrow_schema::DataType::Date32,
            DataType::Date64 => arrow_schema::DataType::Date64,
            DataType::Time32(unit) => arrow_schema::DataType::Time32(unit.into()),
            DataType::Time64(unit) => arrow_schema::DataType::Time64(unit.into()),
            DataType::Duration(unit) => arrow_schema::DataType::Duration(unit.into()),
            DataType::Interval(unit) => arrow_schema::DataType::Interval(unit.into()),
            DataType::Binary => arrow_schema::DataType::Binary,
            DataType::FixedSizeBinary(size) => arrow_schema::DataType::FixedSizeBinary(size as _),
            DataType::LargeBinary => arrow_schema::DataType::LargeBinary,
            DataType::Utf8 => arrow_schema::DataType::Utf8,
            DataType::LargeUtf8 => arrow_schema::DataType::LargeUtf8,
            DataType::List(f) => arrow_schema::DataType::List(Arc::new((*f).into())),
            DataType::FixedSizeList(f, size) => {
                arrow_schema::DataType::FixedSizeList(Arc::new((*f).into()), size as _)
            }
            DataType::LargeList(f) => arrow_schema::DataType::LargeList(Arc::new((*f).into())),
            DataType::Struct(f) => {
                arrow_schema::DataType::Struct(f.into_iter().map(ArrowField::from).collect())
            }
            DataType::Union(fields, Some(ids), mode) => {
                let ids = ids.into_iter().map(|x| x as _);
                let fields = fields.into_iter().map(ArrowField::from);
                arrow_schema::DataType::Union(UnionFields::new(ids, fields), mode.into())
            }
            DataType::Union(fields, None, mode) => {
                let ids = 0..fields.len() as i8;
                let fields = fields.into_iter().map(ArrowField::from);
                arrow_schema::DataType::Union(UnionFields::new(ids, fields), mode.into())
            }
            DataType::Map(f, ordered) => {
                arrow_schema::DataType::Map(Arc::new((*f).into()), ordered)
            }
            DataType::Dictionary(key, value, _) => arrow_schema::DataType::Dictionary(
                Box::new(DataType::from(key).into()),
                Box::new((*value).into()),
            ),
            DataType::Decimal(precision, scale) => {
                arrow_schema::DataType::Decimal128(precision as _, scale as _)
            }
            DataType::Decimal256(precision, scale) => {
                arrow_schema::DataType::Decimal256(precision as _, scale as _)
            }
            DataType::Extension(name, d, metadata) => {
                let mut metadata_map = HashMap::new();
                metadata_map.insert("ARROW:extension:name".to_string(), name);
                if let Some(metadata) = metadata {
                    metadata_map.insert("ARROW:extension:metadata".to_string(), metadata);
                }
                return arrow2_field_to_arrow(Field::new(field.name, *d, true))
                    .with_metadata(metadata_map);
            }
        };
        arrow_schema::Field::new(field.name, dtype, true)
    }
}
