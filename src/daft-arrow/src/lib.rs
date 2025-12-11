// Re-export arrow2::* modules for centralized access
pub use arrow_array::{self, temporal_conversions};
pub use arrow_schema::{self, ArrowError};
pub use arrow2::{array, chunk, compute, error, offset, scalar, trusted_len, types};


// IPC module that exports arrow-ipc functionality
pub use arrow_ipc as ipc;

// Re-export io module but with our own IPC and Flight implementations
pub mod io {
    // Re-export arrow2::io modules except ipc and flight
    pub use arrow2::io::json;
    pub use arrow2::io::parquet;
    pub use arrow2::io::csv;
    
    // Our own IPC and Flight implementations using arrow-ipc and arrow-flight
    pub mod ipc {
        pub mod write {
            use arrow_format::ipc::planus::Builder;
            use arrow2::datatypes::{DataType, Field, IntegerType, IntervalUnit, Metadata, Schema, TimeUnit, UnionMode};
            
            use super::IpcField;
            
            // Re-export types from arrow2 that are needed
            // These are available even when io_ipc feature is disabled through the arrow feature
            pub use arrow2::io::ipc::write::{Compression, WriteOptions, StreamWriter};
            
            fn is_native_little_endian() -> bool {
                #[cfg(target_endian = "little")]
                {
                    true
                }
                #[cfg(target_endian = "big")]
                {
                    false
                }
            }
            
            /// Converts a [Schema] and [IpcField]s to a flatbuffers-encoded [arrow_format::ipc::Message].
            pub fn schema_to_bytes(schema: &Schema, ipc_fields: &[IpcField]) -> Vec<u8> {
                let schema = serialize_schema(schema, ipc_fields);
                
                let message = arrow_format::ipc::Message {
                    version: arrow_format::ipc::MetadataVersion::V5,
                    header: Some(arrow_format::ipc::MessageHeader::Schema(Box::new(schema))),
                    body_length: 0,
                    custom_metadata: None,
                };
                let mut builder = Builder::new();
                let footer_data = builder.finish(&message, None);
                footer_data.to_vec()
            }
            
            fn serialize_schema(schema: &Schema, ipc_fields: &[IpcField]) -> arrow_format::ipc::Schema {
                let endianness = if is_native_little_endian() {
                    arrow_format::ipc::Endianness::Little
                } else {
                    arrow_format::ipc::Endianness::Big
                };
                
                let fields = schema
                    .fields
                    .iter()
                    .zip(ipc_fields.iter())
                    .map(|(field, ipc_field)| serialize_field(field, ipc_field))
                    .collect::<Vec<_>>();
                
                let mut custom_metadata = vec![];
                for (key, value) in &schema.metadata {
                    custom_metadata.push(arrow_format::ipc::KeyValue {
                        key: Some(key.clone()),
                        value: Some(value.clone()),
                    });
                }
                let custom_metadata = if custom_metadata.is_empty() {
                    None
                } else {
                    Some(custom_metadata)
                };
                
                arrow_format::ipc::Schema {
                    endianness,
                    fields: Some(fields),
                    custom_metadata,
                    features: None,
                }
            }
            
            fn write_metadata(metadata: &Metadata, kv_vec: &mut Vec<arrow_format::ipc::KeyValue>) {
                for (k, v) in metadata {
                    if k != "ARROW:extension:name" && k != "ARROW:extension:metadata" {
                        let entry = arrow_format::ipc::KeyValue {
                            key: Some(k.clone()),
                            value: Some(v.clone()),
                        };
                        kv_vec.push(entry);
                    }
                }
            }
            
            fn write_extension(
                name: &str,
                metadata: &Option<String>,
                kv_vec: &mut Vec<arrow_format::ipc::KeyValue>,
            ) {
                if let Some(metadata) = metadata {
                    let entry = arrow_format::ipc::KeyValue {
                        key: Some("ARROW:extension:metadata".to_string()),
                        value: Some(metadata.clone()),
                    };
                    kv_vec.push(entry);
                }
                
                let entry = arrow_format::ipc::KeyValue {
                    key: Some("ARROW:extension:name".to_string()),
                    value: Some(name.to_string()),
                };
                kv_vec.push(entry);
            }
            
            fn serialize_field(field: &Field, ipc_field: &IpcField) -> arrow_format::ipc::Field {
                let mut kv_vec = vec![];
                if let DataType::Extension(name, _, metadata) = field.data_type() {
                    write_extension(name, metadata, &mut kv_vec);
                }
                
                let type_ = serialize_type(field.data_type());
                let children = serialize_children(field.data_type(), ipc_field);
                
                let dictionary = if let DataType::Dictionary(index_type, inner, is_ordered) = field.data_type() {
                    if let DataType::Extension(name, _, metadata) = inner.as_ref() {
                        write_extension(name, metadata, &mut kv_vec);
                    }
                    Some(serialize_dictionary(
                        index_type,
                        ipc_field
                            .dictionary_id
                            .expect("All Dictionary types have `dict_id`"),
                        *is_ordered,
                    ))
                } else {
                    None
                };
                
                write_metadata(&field.metadata, &mut kv_vec);
                
                let custom_metadata = if !kv_vec.is_empty() {
                    Some(kv_vec)
                } else {
                    None
                };
                
                arrow_format::ipc::Field {
                    name: Some(field.name.clone()),
                    nullable: field.is_nullable,
                    type_: Some(type_),
                    dictionary: dictionary.map(Box::new),
                    children: Some(children),
                    custom_metadata,
                }
            }
            
            fn serialize_time_unit(unit: &TimeUnit) -> arrow_format::ipc::TimeUnit {
                match unit {
                    TimeUnit::Second => arrow_format::ipc::TimeUnit::Second,
                    TimeUnit::Millisecond => arrow_format::ipc::TimeUnit::Millisecond,
                    TimeUnit::Microsecond => arrow_format::ipc::TimeUnit::Microsecond,
                    TimeUnit::Nanosecond => arrow_format::ipc::TimeUnit::Nanosecond,
                }
            }
            
            fn serialize_type(data_type: &DataType) -> arrow_format::ipc::Type {
                use arrow_format::ipc;
                use DataType::*;
                match data_type {
                    Null => ipc::Type::Null(Box::new(ipc::Null {})),
                    Boolean => ipc::Type::Bool(Box::new(ipc::Bool {})),
                    UInt8 => ipc::Type::Int(Box::new(ipc::Int {
                        bit_width: 8,
                        is_signed: false,
                    })),
                    UInt16 => ipc::Type::Int(Box::new(ipc::Int {
                        bit_width: 16,
                        is_signed: false,
                    })),
                    UInt32 => ipc::Type::Int(Box::new(ipc::Int {
                        bit_width: 32,
                        is_signed: false,
                    })),
                    UInt64 => ipc::Type::Int(Box::new(ipc::Int {
                        bit_width: 64,
                        is_signed: false,
                    })),
                    Int8 => ipc::Type::Int(Box::new(ipc::Int {
                        bit_width: 8,
                        is_signed: true,
                    })),
                    Int16 => ipc::Type::Int(Box::new(ipc::Int {
                        bit_width: 16,
                        is_signed: true,
                    })),
                    Int32 => ipc::Type::Int(Box::new(ipc::Int {
                        bit_width: 32,
                        is_signed: true,
                    })),
                    Int64 => ipc::Type::Int(Box::new(ipc::Int {
                        bit_width: 64,
                        is_signed: true,
                    })),
                    Float16 => ipc::Type::FloatingPoint(Box::new(ipc::FloatingPoint {
                        precision: ipc::Precision::Half,
                    })),
                    Float32 => ipc::Type::FloatingPoint(Box::new(ipc::FloatingPoint {
                        precision: ipc::Precision::Single,
                    })),
                    Float64 => ipc::Type::FloatingPoint(Box::new(ipc::FloatingPoint {
                        precision: ipc::Precision::Double,
                    })),
                    Decimal(precision, scale) => ipc::Type::Decimal(Box::new(ipc::Decimal {
                        precision: *precision as i32,
                        scale: *scale as i32,
                        bit_width: 128,
                    })),
                    Decimal256(precision, scale) => ipc::Type::Decimal(Box::new(ipc::Decimal {
                        precision: *precision as i32,
                        scale: *scale as i32,
                        bit_width: 256,
                    })),
                    Binary => ipc::Type::Binary(Box::new(ipc::Binary {})),
                    LargeBinary => ipc::Type::LargeBinary(Box::new(ipc::LargeBinary {})),
                    Utf8 => ipc::Type::Utf8(Box::new(ipc::Utf8 {})),
                    LargeUtf8 => ipc::Type::LargeUtf8(Box::new(ipc::LargeUtf8 {})),
                    FixedSizeBinary(size) => ipc::Type::FixedSizeBinary(Box::new(ipc::FixedSizeBinary {
                        byte_width: *size as i32,
                    })),
                    Date32 => ipc::Type::Date(Box::new(ipc::Date {
                        unit: ipc::DateUnit::Day,
                    })),
                    Date64 => ipc::Type::Date(Box::new(ipc::Date {
                        unit: ipc::DateUnit::Millisecond,
                    })),
                    Duration(unit) => ipc::Type::Duration(Box::new(ipc::Duration {
                        unit: serialize_time_unit(unit),
                    })),
                    Time32(unit) => ipc::Type::Time(Box::new(ipc::Time {
                        unit: serialize_time_unit(unit),
                        bit_width: 32,
                    })),
                    Time64(unit) => ipc::Type::Time(Box::new(ipc::Time {
                        unit: serialize_time_unit(unit),
                        bit_width: 64,
                    })),
                    Timestamp(unit, tz) => ipc::Type::Timestamp(Box::new(ipc::Timestamp {
                        unit: serialize_time_unit(unit),
                        timezone: tz.as_ref().cloned(),
                    })),
                    Interval(unit) => ipc::Type::Interval(Box::new(ipc::Interval {
                        unit: match unit {
                            IntervalUnit::YearMonth => ipc::IntervalUnit::YearMonth,
                            IntervalUnit::DayTime => ipc::IntervalUnit::DayTime,
                            IntervalUnit::MonthDayNano => ipc::IntervalUnit::MonthDayNano,
                        },
                    })),
                    List(_) => ipc::Type::List(Box::new(ipc::List {})),
                    LargeList(_) => ipc::Type::LargeList(Box::new(ipc::LargeList {})),
                    FixedSizeList(_, size) => ipc::Type::FixedSizeList(Box::new(ipc::FixedSizeList {
                        list_size: *size as i32,
                    })),
                    Union(_, type_ids, mode) => ipc::Type::Union(Box::new(ipc::Union {
                        mode: match mode {
                            UnionMode::Dense => ipc::UnionMode::Dense,
                            UnionMode::Sparse => ipc::UnionMode::Sparse,
                        },
                        type_ids: type_ids.clone(),
                    })),
                    Map(_, keys_sorted) => ipc::Type::Map(Box::new(ipc::Map {
                        keys_sorted: *keys_sorted,
                    })),
                    Struct(_) => ipc::Type::Struct(Box::new(ipc::Struct {})),
                    Dictionary(_, v, _) => serialize_type(v),
                    Extension(_, v, _) => serialize_type(v),
                }
            }
            
            fn serialize_children(data_type: &DataType, ipc_field: &IpcField) -> Vec<arrow_format::ipc::Field> {
                use DataType::*;
                match data_type {
                    Null
                    | Boolean
                    | Int8
                    | Int16
                    | Int32
                    | Int64
                    | UInt8
                    | UInt16
                    | UInt32
                    | UInt64
                    | Float16
                    | Float32
                    | Float64
                    | Timestamp(_, _)
                    | Date32
                    | Date64
                    | Time32(_)
                    | Time64(_)
                    | Duration(_)
                    | Interval(_)
                    | Binary
                    | FixedSizeBinary(_)
                    | LargeBinary
                    | Utf8
                    | LargeUtf8
                    | Decimal(_, _)
                    | Decimal256(_, _) => vec![],
                    FixedSizeList(inner, _) | LargeList(inner) | List(inner) | Map(inner, _) => {
                        vec![serialize_field(inner, &ipc_field.fields[0])]
                    }
                    Union(fields, _, _) | Struct(fields) => fields
                        .iter()
                        .zip(ipc_field.fields.iter())
                        .map(|(field, ipc)| serialize_field(field, ipc))
                        .collect(),
                    Dictionary(_, inner, _) => serialize_children(inner, ipc_field),
                    Extension(_, inner, _) => serialize_children(inner, ipc_field),
                }
            }
            
            fn serialize_dictionary(
                index_type: &IntegerType,
                dict_id: i64,
                dict_is_ordered: bool,
            ) -> arrow_format::ipc::DictionaryEncoding {
                use IntegerType::*;
                let is_signed = match index_type {
                    Int8 | Int16 | Int32 | Int64 => true,
                    UInt8 | UInt16 | UInt32 | UInt64 => false,
                };
                
                let bit_width = match index_type {
                    Int8 | UInt8 => 8,
                    Int16 | UInt16 => 16,
                    Int32 | UInt32 => 32,
                    Int64 | UInt64 => 64,
                };
                
                let index_type = arrow_format::ipc::Int {
                    bit_width,
                    is_signed,
                };
                
                arrow_format::ipc::DictionaryEncoding {
                    id: dict_id,
                    index_type: Some(Box::new(index_type)),
                    is_ordered: dict_is_ordered,
                    dictionary_kind: arrow_format::ipc::DictionaryKind::DenseArray,
                }
            }
        }
        
        pub mod read {
            pub use arrow2::io::ipc::read::*;
        }
        
        // Re-export IpcField from arrow2
        pub use arrow2::io::ipc::IpcField;
    }
    
    pub mod flight {
        use arrow2::datatypes::Field;
        use super::ipc::IpcField;
        
        /// Assigns every dictionary field a unique ID
        /// This is the same as arrow2::io::ipc::write::default_ipc_fields
        pub fn default_ipc_fields(fields: &[Field]) -> Vec<IpcField> {
            use arrow2::datatypes::DataType;
            
            fn default_ipc_field(data_type: &DataType, current_id: &mut i64) -> IpcField {
                use DataType::*;
                match data_type.to_logical_type() {
                    Map(inner, ..) | FixedSizeList(inner, _) | LargeList(inner) | List(inner) => IpcField {
                        fields: vec![default_ipc_field(inner.data_type(), current_id)],
                        dictionary_id: None,
                    },
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
                .map(|field| default_ipc_field(field.data_type().to_logical_type(), &mut dictionary_id))
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
