// Re-export arrow2::* modules for centralized access
pub use arrow_array::{self, temporal_conversions};
// IPC module that exports arrow-ipc functionality
pub use arrow_schema::{self, ArrowError};
pub use arrow2::{array, chunk, compute, error, offset, scalar, trusted_len, types};

// Re-export io module but with our own IPC and Flight implementations
pub mod io {
    // Re-export arrow2::io modules except ipc and flight
    pub use arrow2::io::{csv, json, parquet};

    pub mod flight {
        use arrow2::{datatypes::Field, io::ipc::IpcField};

        /// Assigns every dictionary field a unique ID
        /// This is the same as arrow2::io::ipc::write::default_ipc_fields
        pub fn default_ipc_fields(fields: &[Field]) -> Vec<IpcField> {
            #[allow(deprecated, reason = "arrow2 migration")]
            use arrow2::datatypes::DataType;

            #[allow(deprecated, reason = "arrow2 migration")]
            fn default_ipc_field(data_type: &DataType, current_id: &mut i64) -> IpcField {
                #[allow(deprecated, reason = "arrow2 migration")]
                use DataType::*;
                match data_type.to_logical_type() {
                    Map(inner, ..) | FixedSizeList(inner, _) | LargeList(inner) | List(inner) => {
                        IpcField {
                            fields: vec![default_ipc_field(inner.data_type(), current_id)],
                            dictionary_id: None,
                        }
                    }
                    Union(fields, ..) | Struct(fields) => IpcField {
                        fields: fields
                            .iter()
                            .map(|f| default_ipc_field(f.data_type(), current_id))
                            .collect(),
                        dictionary_id: None,
                    },
                    Dictionary(_, data_type, _) => {
                        let dictionary_id = Some(*current_id);
                        *current_id += 1;
                        IpcField {
                            fields: vec![default_ipc_field(data_type, current_id)],
                            dictionary_id,
                        }
                    }
                    _ => IpcField {
                        fields: vec![],
                        dictionary_id: None,
                    },
                }
            }

            let mut dictionary_id = 0i64;
            fields
                .iter()
                .map(|field| {
                    default_ipc_field(field.data_type().to_logical_type(), &mut dictionary_id)
                })
                .collect()
        }
    }
}

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
    pub use arrow2::datatypes::*;

    #[deprecated(note = "use arrow instead of arrow2")]
    #[allow(deprecated, reason = "arrow2 migration")]
    pub fn arrow_datatype_to_arrow2(
        datatype: &arrow_schema::DataType,
    ) -> arrow2::datatypes::DataType {
        match datatype {
            arrow_schema::DataType::Null => arrow2::datatypes::DataType::Null,
            arrow_schema::DataType::Boolean => arrow2::datatypes::DataType::Boolean,
            arrow_schema::DataType::Int8 => arrow2::datatypes::DataType::Int8,
            arrow_schema::DataType::Int16 => arrow2::datatypes::DataType::Int16,
            arrow_schema::DataType::Int32 => arrow2::datatypes::DataType::Int32,
            arrow_schema::DataType::Int64 => arrow2::datatypes::DataType::Int64,
            arrow_schema::DataType::UInt8 => arrow2::datatypes::DataType::UInt8,
            arrow_schema::DataType::UInt16 => arrow2::datatypes::DataType::UInt16,
            arrow_schema::DataType::UInt32 => arrow2::datatypes::DataType::UInt32,
            arrow_schema::DataType::UInt64 => arrow2::datatypes::DataType::UInt64,
            arrow_schema::DataType::Float16 => arrow2::datatypes::DataType::Float16,
            arrow_schema::DataType::Float32 => arrow2::datatypes::DataType::Float32,
            arrow_schema::DataType::Float64 => arrow2::datatypes::DataType::Float64,
            arrow_schema::DataType::Timestamp(unit, tz) => arrow2::datatypes::DataType::Timestamp(
                (*unit).into(),
                tz.as_ref().map(|x| x.to_string()),
            ),
            arrow_schema::DataType::Date32 => arrow2::datatypes::DataType::Date32,
            arrow_schema::DataType::Date64 => arrow2::datatypes::DataType::Date64,
            arrow_schema::DataType::Time32(unit) => {
                arrow2::datatypes::DataType::Time32((*unit).into())
            }
            arrow_schema::DataType::Time64(unit) => {
                arrow2::datatypes::DataType::Time64((*unit).into())
            }
            arrow_schema::DataType::Duration(unit) => {
                arrow2::datatypes::DataType::Duration((*unit).into())
            }
            arrow_schema::DataType::Interval(unit) => {
                arrow2::datatypes::DataType::Interval((*unit).into())
            }
            arrow_schema::DataType::Binary => arrow2::datatypes::DataType::Binary,
            arrow_schema::DataType::FixedSizeBinary(size) => {
                arrow2::datatypes::DataType::FixedSizeBinary(*size as _)
            }
            arrow_schema::DataType::LargeBinary => arrow2::datatypes::DataType::LargeBinary,
            arrow_schema::DataType::Utf8 => arrow2::datatypes::DataType::Utf8,
            arrow_schema::DataType::LargeUtf8 => arrow2::datatypes::DataType::LargeUtf8,
            arrow_schema::DataType::List(f) => {
                arrow2::datatypes::DataType::List(Box::new(arrow_field_to_arrow2(f.as_ref())))
            }
            arrow_schema::DataType::FixedSizeList(f, size) => {
                arrow2::datatypes::DataType::FixedSizeList(
                    Box::new(arrow_field_to_arrow2(f.as_ref())),
                    *size as _,
                )
            }
            arrow_schema::DataType::LargeList(f) => {
                arrow2::datatypes::DataType::LargeList(Box::new(arrow_field_to_arrow2(f.as_ref())))
            }
            arrow_schema::DataType::Struct(f) => arrow2::datatypes::DataType::Struct(
                f.into_iter()
                    .map(|field| arrow_field_to_arrow2(field.as_ref()))
                    .collect(),
            ),
            arrow_schema::DataType::Union(fields, mode) => {
                let ids = fields.iter().map(|(id, _)| id as _).collect();
                let fields = fields
                    .iter()
                    .map(|(_, field)| arrow_field_to_arrow2(field))
                    .collect();
                arrow2::datatypes::DataType::Union(fields, Some(ids), (*mode).into())
            }
            arrow_schema::DataType::Map(f, ordered) => arrow2::datatypes::DataType::Map(
                Box::new(arrow_field_to_arrow2(f.as_ref())),
                *ordered,
            ),
            arrow_schema::DataType::Dictionary(key, value) => {
                let key_type = match key.as_ref() {
                    arrow_schema::DataType::Int8 => arrow2::datatypes::IntegerType::Int8,
                    arrow_schema::DataType::Int16 => arrow2::datatypes::IntegerType::Int16,
                    arrow_schema::DataType::Int32 => arrow2::datatypes::IntegerType::Int32,
                    arrow_schema::DataType::Int64 => arrow2::datatypes::IntegerType::Int64,
                    arrow_schema::DataType::UInt8 => arrow2::datatypes::IntegerType::UInt8,
                    arrow_schema::DataType::UInt16 => arrow2::datatypes::IntegerType::UInt16,
                    arrow_schema::DataType::UInt32 => arrow2::datatypes::IntegerType::UInt32,
                    arrow_schema::DataType::UInt64 => arrow2::datatypes::IntegerType::UInt64,
                    _ => panic!("Unsupported dictionary key type: {:?}", key.as_ref()),
                };

                arrow2::datatypes::DataType::Dictionary(
                    key_type,
                    Box::new(arrow_datatype_to_arrow2(value.as_ref())),
                    false,
                )
            }
            arrow_schema::DataType::Decimal128(precision, scale) => {
                arrow2::datatypes::DataType::Decimal(*precision as _, *scale as _)
            }
            arrow_schema::DataType::Decimal256(precision, scale) => {
                arrow2::datatypes::DataType::Decimal256(*precision as _, *scale as _)
            }
            other => panic!("Unsupported arrow datatype: {:?}", other),
        }
    }

    #[deprecated(note = "use arrow instead of arrow2")]
    #[allow(deprecated, reason = "arrow2 migration")]
    pub fn arrow_field_to_arrow2(field: &arrow_schema::Field) -> arrow2::datatypes::Field {
        let mut arrow2_dtype = arrow_datatype_to_arrow2(field.data_type());

        if let Some(extension_name) = field.extension_type_name() {
            let metadata = field.extension_type_metadata().map(|x| x.to_string());
            arrow2_dtype = arrow2::datatypes::DataType::Extension(
                extension_name.to_string(),
                Box::new(arrow2_dtype),
                metadata,
            );
        }

        let metadata = field
            .metadata()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        arrow2::datatypes::Field::new(field.name(), arrow2_dtype, true).with_metadata(metadata)
    }
}
