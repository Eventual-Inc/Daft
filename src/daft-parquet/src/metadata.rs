use std::{collections::BTreeMap, sync::Arc};

use bytes::{Bytes, BytesMut};
use common_runtime::get_io_runtime;
use daft_core::datatypes::Field;
use daft_io::{GetRange, IOClient, IOStatsRef};
use parquet::{
    basic::{ConvertedType, LogicalType, Repetition},
    schema::types::{BasicTypeInfo, Type, TypePtr},
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::{Error, ParquetMetadataSnafu, task_err};

const FOOTER_SIZE: usize = 8;

/// Parquet key-value metadata key for the embedded Arrow schema hint written by
/// pyarrow/arrow-cpp. See `parquet-format-thrift` KeyValue.
const ARROW_SCHEMA_KEY: &str = "ARROW:schema";

// ---------------------------------------------------------------------------
// Arrow-rs field ID mapping
// ---------------------------------------------------------------------------

/// Resolves parquet columns to table fields by parquet field ID.
///
/// Files written without field IDs (e.g. registered with Iceberg's `add_files`) are resolved
/// by column name through `name_mapping`, the table's `schema.name-mapping.default`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FieldIdMapping {
    pub fields: BTreeMap<i32, Field>,
    pub name_mapping: Option<Vec<MappedField>>,
}

/// An Iceberg name mapping entry, in the table spec's JSON form.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MappedField {
    #[serde(rename = "field-id", default)]
    pub field_id: Option<i32>,
    pub names: Vec<String>,
    #[serde(default)]
    pub fields: Option<Vec<Self>>,
}

/// Extract the optional field ID from a parquet `BasicTypeInfo`.
fn get_field_id(info: &BasicTypeInfo) -> Option<i32> {
    info.has_id().then(|| info.id())
}

fn is_list(info: &BasicTypeInfo) -> bool {
    matches!(info.logical_type_ref(), Some(LogicalType::List))
        || info.converted_type() == ConvertedType::LIST
}

fn is_map(info: &BasicTypeInfo) -> bool {
    matches!(info.logical_type_ref(), Some(LogicalType::Map))
        || matches!(
            info.converted_type(),
            ConvertedType::MAP | ConvertedType::MAP_KEY_VALUE
        )
}

/// Whether `repeated`, the repeated group of LIST `list`, is itself the element of a legacy
/// two-level list rather than a three-level wrapper, per parquet's backward-compatibility rules.
pub(crate) fn is_two_level_list_group(list: &Type, repeated: &Type) -> bool {
    match repeated.get_fields() {
        [child] => {
            !is_list(repeated.get_basic_info())
                && child.get_basic_info().repetition() != Repetition::REPEATED
                && (repeated.name() == "array"
                    || repeated.name() == format!("{}_tuple", list.name()))
        }
        _ => true,
    }
}

/// Rebuild a type with the given name, field ID and (for groups) children, keeping all other
/// attributes.
fn rebuild_type(tp: &Type, name: &str, id: Option<i32>, fields: Vec<TypePtr>) -> Type {
    let info = tp.get_basic_info();
    match tp {
        Type::PrimitiveType {
            physical_type,
            type_length,
            scale,
            precision,
            ..
        } => Type::primitive_type_builder(name, *physical_type)
            .with_repetition(info.repetition())
            .with_converted_type(info.converted_type())
            .with_logical_type(info.logical_type_ref().cloned())
            .with_length(*type_length)
            .with_precision(*precision)
            .with_scale(*scale)
            .with_id(id)
            .build(),
        Type::GroupType { .. } => Type::group_type_builder(name)
            .with_repetition(info.repetition())
            .with_converted_type(info.converted_type())
            .with_logical_type(info.logical_type_ref().cloned())
            .with_fields(fields)
            .with_id(id)
            .build(),
    }
    .expect("rebuilding a type with its own attributes should not fail")
}

/// Assign field IDs to the fields of a schema written without them, matching names against the
/// Iceberg name mapping. LIST elements and MAP keys/values match the mapping's `element`, `key`
/// and `value` entries whatever their physical names. Unmatched fields get no ID.
fn assign_field_ids(root: &Type, mapping: &[MappedField]) -> Type {
    fn find<'a>(mapping: &'a [MappedField], name: &str) -> Option<&'a MappedField> {
        mapping.iter().find(|f| f.names.iter().any(|n| n == name))
    }

    fn assign(tp: &Type, mapped: Option<&MappedField>) -> Type {
        let id = mapped.and_then(|m| m.field_id);
        let children = mapped.and_then(|m| m.fields.as_deref()).unwrap_or_default();
        let info = tp.get_basic_info();
        let fields = match tp {
            Type::PrimitiveType { .. } => vec![],
            Type::GroupType { fields, .. } if is_list(info) => fields
                .iter()
                .map(|repeated| match repeated.as_ref() {
                    // 3-level list: the repeated group wraps the element.
                    Type::GroupType { fields: inner, .. }
                        if !is_two_level_list_group(tp, repeated) =>
                    {
                        rebuild_type(
                            repeated,
                            repeated.name(),
                            None,
                            vec![Arc::new(assign(&inner[0], find(children, "element")))],
                        )
                    }
                    // 2-level list of groups: the repeated group is the element. It keeps no ID
                    // and is named `array`, so the rename by ID preserves the legacy marker.
                    Type::GroupType { .. } => {
                        let element = assign(repeated, find(children, "element"));
                        rebuild_type(&element, "array", None, element.get_fields().to_vec())
                    }
                    // 2-level list of primitives: the repeated field is the element.
                    Type::PrimitiveType { .. } => assign(repeated, find(children, "element")),
                })
                .map(Arc::new)
                .collect(),
            Type::GroupType { fields, .. } if is_map(info) => fields
                .iter()
                .map(|key_value| {
                    let entries = key_value.get_fields().iter().zip(["key", "value"]);
                    let entries =
                        entries.map(|(f, name)| Arc::new(assign(f, find(children, name))));
                    Arc::new(rebuild_type(
                        key_value,
                        key_value.name(),
                        None,
                        entries.collect(),
                    ))
                })
                .collect(),
            Type::GroupType { fields, .. } => fields
                .iter()
                .map(|f| Arc::new(assign(f, find(children, f.name()))))
                .collect(),
        };
        rebuild_type(tp, tp.name(), id, fields)
    }

    let fields = root.get_fields().iter();
    let fields = fields.map(|f| Arc::new(assign(f, find(mapping, f.name()))));
    Type::group_type_builder(root.name())
        .with_fields(fields.collect())
        .build()
        .expect("rebuilding the schema root should not fail")
}

/// Rewrite children of a group type per the field_id_mapping.
///
/// When the parent is a LIST/MAP (or has another logical type), intermediate children without
/// a field ID are preserved (with their own children recursed). Otherwise
/// (plain struct), unmapped children are dropped.
fn rewrite_children(
    parent: &BasicTypeInfo,
    fields: &[TypePtr],
    field_id_mapping: &BTreeMap<i32, Field>,
) -> Vec<TypePtr> {
    let is_container = parent.logical_type_ref().is_some() || is_list(parent) || is_map(parent);
    fields
        .iter()
        .filter_map(|child| {
            if is_container {
                // LIST/MAP intermediate nodes may lack field IDs but still
                // contain mapped descendants (e.g. struct fields inside a list).
                Some(Arc::new(
                    rewrite_arrowrs_type_with_field_ids(child, field_id_mapping)
                        .unwrap_or_else(|| recurse_children_only(child, field_id_mapping)),
                ))
            } else {
                // Plain struct: drop children not in mapping.
                rewrite_arrowrs_type_with_field_ids(child, field_id_mapping).map(Arc::new)
            }
        })
        .collect()
}

/// Recursively rewrite an arrow-rs `Type`, renaming fields per the mapping and
/// dropping unmapped struct children.  Returns `None` if this field should be
/// dropped entirely (no field ID, or ID not in the mapping).
fn rewrite_arrowrs_type_with_field_ids(
    tp: &Type,
    field_id_mapping: &BTreeMap<i32, Field>,
) -> Option<Type> {
    let info = tp.get_basic_info();
    let mapped_field = get_field_id(info).and_then(|fid| field_id_mapping.get(&fid))?;
    let children = match tp {
        Type::PrimitiveType { .. } => vec![],
        Type::GroupType { fields, .. } => rewrite_children(info, fields, field_id_mapping),
    };
    Some(rebuild_type(
        tp,
        &mapped_field.name,
        get_field_id(info),
        children,
    ))
}

/// Rebuild a group node preserving its original name/attributes, but still
/// recursing into its children to rename/filter them per the field_id_mapping.
/// For primitive nodes, returns an unchanged clone.
fn recurse_children_only(tp: &Type, field_id_mapping: &BTreeMap<i32, Field>) -> Type {
    let Type::GroupType { fields, .. } = tp else {
        return tp.clone();
    };
    let info = tp.get_basic_info();
    let children = rewrite_children(info, fields, field_id_mapping);
    rebuild_type(tp, info.name(), get_field_id(info), children)
}

/// Applies field_ids to an arrow-rs `ParquetMetaData`:
/// 1. Rename columns based on the `field_id_mapping`
/// 2. Drop columns without a field_id or without a corresponding mapping entry
pub(crate) fn apply_field_ids_to_arrowrs_parquet_metadata(
    metadata: Arc<parquet::file::metadata::ParquetMetaData>,
    field_id_mapping: &FieldIdMapping,
    path: &str,
) -> crate::Result<Arc<parquet::file::metadata::ParquetMetaData>> {
    use parquet::{
        file::metadata::{ColumnChunkMetaData, ParquetMetaData, RowGroupMetaData},
        schema::types::SchemaDescriptor,
    };

    let old_schema = metadata.file_metadata().schema_descr();
    let old_root = old_schema.root_schema();

    // A file with no field IDs at all can only be matched by name, through the name mapping.
    // Without one, every mapped column would silently read as null, so fail instead — a
    // partially-annotated file is still handled below, where unmapped children are dropped
    // on purpose.
    let named_schema;
    let source_schema = if !old_root.get_fields().is_empty()
        && !old_root
            .get_fields()
            .iter()
            .any(|field| get_field_id(field.get_basic_info()).is_some())
    {
        let Some(name_mapping) = &field_id_mapping.name_mapping else {
            return Err(Error::MissingParquetFieldIds {
                path: path.to_string(),
            });
        };
        named_schema = SchemaDescriptor::new(Arc::new(assign_field_ids(old_root, name_mapping)));
        &named_schema
    } else {
        old_schema
    };

    // 1. Rewrite the schema type tree: rename + filter by field_id_mapping
    let new_fields: Vec<_> = source_schema
        .root_schema()
        .get_fields()
        .iter()
        .filter_map(|field| {
            rewrite_arrowrs_type_with_field_ids(field, &field_id_mapping.fields).map(Arc::new)
        })
        .collect();

    let new_root = Type::group_type_builder(old_root.name())
        .with_fields(new_fields)
        .build()
        .with_context(|_| ParquetMetadataSnafu {
            path: path.to_string(),
        })?;
    let new_schema_descr = Arc::new(SchemaDescriptor::new(Arc::new(new_root)));

    // 2. Build field_id → new ColumnDescriptor mapping
    let field_id_to_col_descr: BTreeMap<i32, _> = new_schema_descr
        .columns()
        .iter()
        .filter_map(|col_descr| {
            let info = col_descr.self_type().get_basic_info();
            get_field_id(info).map(|fid| (fid, col_descr.clone()))
        })
        .collect();

    // 3. Rebuild row groups with filtered/renamed column descriptors
    let new_row_groups: Result<Vec<RowGroupMetaData>, _> = metadata
        .row_groups()
        .iter()
        .map(|rg| {
            debug_assert_eq!(rg.columns().len(), source_schema.num_columns());
            let new_columns: Vec<ColumnChunkMetaData> = rg
                .columns()
                .iter()
                .zip(source_schema.columns())
                .filter_map(|(col, source_descr)| {
                    let col_info = source_descr.self_type().get_basic_info();
                    let new_descr =
                        get_field_id(col_info).and_then(|fid| field_id_to_col_descr.get(&fid))?;

                    // Rebuild ColumnChunkMetaData with new descriptor.
                    // No set_column_descr on the builder, so we construct from scratch.
                    let mut builder = ColumnChunkMetaData::builder(new_descr.clone())
                        .set_encodings_mask(*col.encodings_mask())
                        .set_num_values(col.num_values())
                        .set_compression(col.compression())
                        .set_data_page_offset(col.data_page_offset())
                        .set_total_compressed_size(col.compressed_size())
                        .set_total_uncompressed_size(col.uncompressed_size())
                        .set_index_page_offset(col.index_page_offset())
                        .set_dictionary_page_offset(col.dictionary_page_offset())
                        .set_bloom_filter_offset(col.bloom_filter_offset())
                        .set_bloom_filter_length(col.bloom_filter_length())
                        .set_offset_index_offset(col.offset_index_offset())
                        .set_offset_index_length(col.offset_index_length())
                        .set_column_index_offset(col.column_index_offset())
                        .set_column_index_length(col.column_index_length())
                        .set_unencoded_byte_array_data_bytes(col.unencoded_byte_array_data_bytes());
                    if let Some(stats) = col.statistics() {
                        builder = builder.set_statistics(stats.clone());
                    }
                    if let Some(path) = col.file_path() {
                        builder = builder.set_file_path(path.to_string());
                    }
                    Some(
                        builder
                            .build()
                            .expect("column chunk rebuild should not fail"),
                    )
                })
                .collect();

            let total_byte_size: i64 = new_columns.iter().map(|c| c.uncompressed_size()).sum();
            RowGroupMetaData::builder(new_schema_descr.clone())
                .set_num_rows(rg.num_rows())
                .set_total_byte_size(total_byte_size)
                .set_column_metadata(new_columns)
                .build()
                .with_context(|_| ParquetMetadataSnafu {
                    path: path.to_string(),
                })
        })
        .collect();

    // 4. Rebuild FileMetaData and ParquetMetaData. Strip the embedded ARROW:schema
    // hint: it still carries the original (physical) field names and disagrees with
    // the renamed parquet type tree, causing arrow-rs to fail with
    // `incompatible arrow schema, expected field named <physical> got <logical>`.
    // Dropping it forces arrow-rs to infer from the renamed parquet types.
    Ok(Arc::new(ParquetMetaData::new(
        rebuild_file_metadata(metadata.file_metadata(), new_schema_descr, true),
        new_row_groups?,
    )))
}

/// Rebuild a `FileMetaData` with a new schema descriptor (preserving everything else).
///
/// When `strip_arrow_schema` is true the embedded `ARROW:schema` key-value entry is
/// removed; this is required after a field-id rename where the embedded arrow schema
/// would otherwise disagree with the renamed parquet types.
fn rebuild_file_metadata(
    fm: &parquet::file::metadata::FileMetaData,
    new_schema_descr: Arc<parquet::schema::types::SchemaDescriptor>,
    strip_arrow_schema: bool,
) -> parquet::file::metadata::FileMetaData {
    let key_value_metadata = fm.key_value_metadata().map(|kv| {
        kv.iter()
            .filter(|entry| !strip_arrow_schema || entry.key != ARROW_SCHEMA_KEY)
            .cloned()
            .collect::<Vec<_>>()
    });
    parquet::file::metadata::FileMetaData::new(
        fm.version(),
        fm.num_rows(),
        fm.created_by().map(str::to_string),
        key_value_metadata,
        new_schema_descr,
        fm.column_orders().cloned(),
    )
}

/// Strip STRING/UTF8 logical types from BYTE_ARRAY columns in parquet metadata.
///
/// This makes arrow-rs infer `Binary` instead of `Utf8` for string columns,
/// which avoids UTF-8 validation during decode. Used to implement
/// `StringEncoding::Raw` which reads string columns as raw bytes.
pub(crate) fn strip_string_types_from_parquet_metadata(
    metadata: Arc<parquet::file::metadata::ParquetMetaData>,
    path: &str,
) -> crate::Result<Arc<parquet::file::metadata::ParquetMetaData>> {
    use parquet::{
        file::metadata::{ParquetMetaData, RowGroupMetaData},
        schema::types::{SchemaDescriptor, Type},
    };

    let old_schema = metadata.file_metadata().schema_descr();
    let old_root = old_schema.root_schema();

    let new_fields: Vec<_> = old_root
        .get_fields()
        .iter()
        .map(|field| Arc::new(strip_string_type(field)))
        .collect();

    let new_root = Type::group_type_builder(old_root.name())
        .with_fields(new_fields)
        .build()
        .with_context(|_| ParquetMetadataSnafu {
            path: path.to_string(),
        })?;
    let new_schema_descr = Arc::new(SchemaDescriptor::new(Arc::new(new_root)));

    // Rebuild row groups with the new schema descriptor.
    let new_row_groups: Result<Vec<RowGroupMetaData>, _> = metadata
        .row_groups()
        .iter()
        .map(|rg| {
            RowGroupMetaData::builder(new_schema_descr.clone())
                .set_num_rows(rg.num_rows())
                .set_total_byte_size(rg.total_byte_size())
                .set_column_metadata(rg.columns().to_vec())
                .build()
                .with_context(|_| ParquetMetadataSnafu {
                    path: path.to_string(),
                })
        })
        .collect();

    Ok(Arc::new(ParquetMetaData::new(
        rebuild_file_metadata(metadata.file_metadata(), new_schema_descr, false),
        new_row_groups?,
    )))
}

/// Recursively strip STRING/UTF8 annotations from a parquet type.
fn strip_string_type(tp: &parquet::schema::types::Type) -> parquet::schema::types::Type {
    use parquet::{
        basic::{ConvertedType, LogicalType},
        schema::types::Type,
    };

    match tp {
        Type::PrimitiveType {
            physical_type,
            type_length,
            scale,
            precision,
            ..
        } => {
            let info = tp.get_basic_info();
            let is_string = matches!(info.logical_type_ref(), Some(LogicalType::String))
                || info.converted_type() == ConvertedType::UTF8;

            if is_string {
                // Rebuild without String/UTF8 annotations so arrow-rs infers Binary.
                Type::primitive_type_builder(info.name(), *physical_type)
                    .with_repetition(info.repetition())
                    .with_length(*type_length)
                    .with_precision(*precision)
                    .with_scale(*scale)
                    .with_id(get_field_id(info))
                    .build()
                    .expect("rebuilding primitive type should not fail")
            } else {
                tp.clone()
            }
        }
        Type::GroupType { fields, .. } => {
            let info = tp.get_basic_info();
            let new_children: Vec<_> = fields
                .iter()
                .map(|child| Arc::new(strip_string_type(child)))
                .collect();
            Type::group_type_builder(info.name())
                .with_repetition(info.repetition())
                .with_converted_type(info.converted_type())
                .with_logical_type(info.logical_type_ref().cloned())
                .with_fields(new_children)
                .with_id(get_field_id(info))
                .build()
                .expect("rebuilding group type should not fail")
        }
    }
}

fn validate_footer_magic(uri: &str, buffer: &[u8]) -> super::Result<()> {
    const PARQUET_MAGIC: [u8; 4] = *b"PAR1";

    if buffer.len() < FOOTER_SIZE {
        return Err(Error::FileTooSmall {
            path: uri.into(),
            file_size: buffer.len(),
        });
    }

    if buffer[buffer.len() - 4..] != PARQUET_MAGIC {
        return Err(Error::InvalidParquetFile {
            path: uri.into(),
            footer: buffer[buffer.len() - 4..].into(),
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared footer I/O
// ---------------------------------------------------------------------------

/// Fetches raw parquet footer bytes from a URI, handling suffix range fallback
/// and two-pass reads for large footers.
///
/// Returns `(footer_bytes, remaining_offset)` where `remaining_offset` is the
/// byte offset within `footer_bytes` where the thrift metadata starts.
async fn fetch_parquet_footer_bytes(
    uri: &str,
    file_size: Option<usize>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    default_footer_read_size: Option<usize>,
) -> super::Result<(Bytes, usize)> {
    async fn fetch_data(
        io_client: Arc<IOClient>,
        uri: &str,
        range: GetRange,
        io_stats: Option<IOStatsRef>,
    ) -> daft_io::Result<Bytes> {
        io_client
            .single_url_get(uri.into(), Some(range), io_stats)
            .await?
            .bytes()
            .await
    }

    let file_size_opt: Option<usize> = if file_size.is_none() && !io_client.support_suffix_range() {
        // suffix range unsupported, get object length for future I/O
        let size = io_client
            .single_url_get_size(uri.into(), io_stats.clone())
            .await?;
        Some(size)
    } else {
        file_size
    };

    // Check the minimum value of file size if provided
    if let Some(size) = file_size_opt
        && size < 12
    {
        return Err(Error::FileTooSmall {
            path: uri.into(),
            file_size: size,
        });
    }

    /// The number of bytes read at the end of the parquet file on first read
    const DEFAULT_FOOTER_READ_SIZE: usize = 128 * 1024;
    let footer_read_size = default_footer_read_size
        .unwrap_or(DEFAULT_FOOTER_READ_SIZE)
        .max(FOOTER_SIZE);
    let range = match file_size_opt {
        None => GetRange::Suffix(footer_read_size),
        Some(size) => {
            let default_end_len = std::cmp::min(footer_read_size, size);
            let start = size - default_end_len;
            (start..size).into()
        }
    };
    let mut data = fetch_data(io_client.clone(), uri, range, io_stats.clone()).await?;
    let buffer = data.as_ref();
    validate_footer_magic(uri, buffer)?;

    let metadata_size = i32::from_le_bytes(
        buffer[buffer.len() - 8..buffer.len() - 4]
            .try_into()
            .unwrap(),
    );
    let footer_len = FOOTER_SIZE + metadata_size as usize;
    if let Some(size) = file_size_opt
        && size < footer_len
    {
        return Err(Error::InvalidParquetFooterSize {
            path: uri.into(),
            footer_size: footer_len,
            file_size: size,
        });
    }

    let remaining = if footer_len <= buffer.len() {
        // the whole metadata is in the bytes we already read
        buffer.len() - footer_len
    } else {
        // the end of file read by default is not long enough, read more bytes of metadata.
        data = match file_size_opt {
            None => fetch_data(io_client, uri, GetRange::Suffix(footer_len), io_stats).await?,
            Some(size) => {
                let range =
                    (size.saturating_sub(footer_len)..size.saturating_sub(buffer.len())).into();
                let new_data = fetch_data(io_client, uri, range, io_stats).await?;

                let mut buffer = BytesMut::with_capacity(new_data.len() + data.len());
                buffer.extend_from_slice(&new_data);
                buffer.extend_from_slice(&data);
                buffer.freeze()
            }
        };
        0
    };

    let buffer = data.as_ref();
    validate_footer_magic(uri, buffer)?;

    Ok((data, remaining))
}

// ---------------------------------------------------------------------------
// Arrow-rs metadata deserialization
// ---------------------------------------------------------------------------

/// Read parquet metadata using arrow-rs deserialization.
///
/// Returns `Arc<parquet::file::metadata::ParquetMetaData>`.
pub(crate) async fn read_parquet_metadata(
    uri: &str,
    file_size: Option<usize>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    field_id_mapping: Option<Arc<FieldIdMapping>>,
    default_footer_read_size: Option<usize>,
) -> super::Result<Arc<parquet::file::metadata::ParquetMetaData>> {
    let (data, remaining) = fetch_parquet_footer_bytes(
        uri,
        file_size,
        io_client,
        io_stats,
        default_footer_read_size,
    )
    .await?;

    let metadata = get_io_runtime(true)
        .spawn_blocking(move || {
            let thrift_bytes = &data.as_ref()[remaining..data.len() - FOOTER_SIZE];
            parquet::file::metadata::ParquetMetaDataReader::decode_metadata(thrift_bytes)
        })
        .await
        .map_err(task_err(uri))?
        .map_err(|e| Error::ParquetMetadata {
            path: uri.to_string(),
            source: e,
        })?;

    let metadata = Arc::new(metadata);

    if let Some(field_id_mapping) = field_id_mapping {
        apply_field_ids_to_arrowrs_parquet_metadata(metadata, field_id_mapping.as_ref(), uri)
    } else {
        Ok(metadata)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType as ArrowDataType;
    use common_error::DaftResult;
    use daft_core::datatypes::{DataType, Field};
    use daft_io::{IOClient, IOConfig};
    use parquet::{
        basic::{ConvertedType, Repetition, Type as PhysicalType},
        file::metadata::{FileMetaData, ParquetMetaData},
        schema::{
            parser::parse_message_type,
            types::{SchemaDescriptor, Type},
        },
    };

    use super::{
        FieldIdMapping, MappedField, apply_field_ids_to_arrowrs_parquet_metadata,
        read_parquet_metadata,
    };
    use crate::{Error, schema_inference::infer_schema_from_parquet_metadata_arrowrs};

    fn mapped(field_id: i32, names: &[&str], fields: Vec<MappedField>) -> MappedField {
        MappedField {
            field_id: Some(field_id),
            names: names.iter().map(|n| (*n).to_string()).collect(),
            fields: Some(fields),
        }
    }

    #[test]
    fn test_apply_name_mapping_to_file_without_field_ids() {
        let parsed = parse_message_type(
            "message hive_schema {
                optional int64 a;
                optional binary b_old (STRING);
                optional group s { optional int32 x; }
                optional group l (LIST) { repeated group list { optional int64 element; } }
                optional group legacy (LIST) { repeated int64 array; }
                optional group m (MAP) {
                    repeated group key_value { required binary key (STRING); optional int64 value; }
                }
                optional int64 unmapped;
                optional group structs (LIST) { repeated group array { optional int64 x; } }
                optional group tuples_old (LIST) { repeated group tuples_old_tuple { optional int64 y; } }
            }",
        )
        .unwrap();
        // Older writers annotate lists with only the converted type.
        let element = Type::primitive_type_builder("array_element", PhysicalType::INT64)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();
        let bag = Type::group_type_builder("bag")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(element)])
            .build()
            .unwrap();
        let converted_only_list = Type::group_type_builder("hive_list")
            .with_repetition(Repetition::OPTIONAL)
            .with_converted_type(ConvertedType::LIST)
            .with_fields(vec![Arc::new(bag)])
            .build()
            .unwrap();
        let mut fields = parsed.get_fields().to_vec();
        fields.push(Arc::new(converted_only_list));
        let schema = Type::group_type_builder("hive_schema")
            .with_fields(fields)
            .build()
            .unwrap();
        let schema = Arc::new(SchemaDescriptor::new(Arc::new(schema)));
        let metadata = Arc::new(ParquetMetaData::new(
            FileMetaData::new(1, 0, None, None, schema, None),
            vec![],
        ));

        let names = [
            "a",
            "b",
            "s",
            "x",
            "l",
            "element",
            "legacy",
            "element",
            "m",
            "key",
            "value",
            "hive_list",
            "element",
            "structs",
            "element",
            "x",
            "tuples",
            "element",
            "y",
        ];
        let mapping = FieldIdMapping {
            fields: (1..)
                .zip(names)
                .map(|(id, name)| (id, Field::new(name, DataType::Null)))
                .collect(),
            name_mapping: Some(vec![
                mapped(1, &["a"], vec![]),
                mapped(2, &["b", "b_old"], vec![]),
                mapped(3, &["s"], vec![mapped(4, &["x"], vec![])]),
                mapped(5, &["l"], vec![mapped(6, &["element"], vec![])]),
                mapped(7, &["legacy"], vec![mapped(8, &["element"], vec![])]),
                mapped(
                    9,
                    &["m"],
                    vec![mapped(10, &["key"], vec![]), mapped(11, &["value"], vec![])],
                ),
                mapped(12, &["hive_list"], vec![mapped(13, &["element"], vec![])]),
                mapped(
                    14,
                    &["structs"],
                    vec![mapped(15, &["element"], vec![mapped(16, &["x"], vec![])])],
                ),
                mapped(
                    17,
                    &["tuples", "tuples_old"],
                    vec![mapped(18, &["element"], vec![mapped(19, &["y"], vec![])])],
                ),
            ]),
        };

        let metadata =
            apply_field_ids_to_arrowrs_parquet_metadata(metadata, &mapping, "test").unwrap();

        let columns: Vec<_> = metadata
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .map(|c| c.path().string())
            .collect();
        assert_eq!(
            columns,
            [
                "a",
                "b",
                "s.x",
                "l.list.element",
                "legacy.element",
                "m.key_value.key",
                "m.key_value.value",
                "structs.array.x",
                "tuples.array.y",
                "hive_list.bag.element",
            ]
        );
        // Two-level lists of groups must still read as lists of structs after the rename.
        let schema = infer_schema_from_parquet_metadata_arrowrs(&metadata, None, false).unwrap();
        for name in ["structs", "tuples"] {
            let ArrowDataType::List(element) = schema.field_with_name(name).unwrap().data_type()
            else {
                panic!("{name} is not a list");
            };
            assert!(matches!(element.data_type(), ArrowDataType::Struct(_)));
        }
    }

    #[tokio::test]
    async fn test_parquet_metadata_from_s3() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet";
        let size = 9882;

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        // Read metadata with actual file size.
        let metadata =
            read_parquet_metadata(file, Some(size), io_client.clone(), None, None, None).await?;
        assert_eq!(metadata.file_metadata().num_rows(), 100);

        // Read metadata without a file size.
        let metadata =
            read_parquet_metadata(file, None, io_client.clone(), None, None, None).await?;
        assert_eq!(metadata.file_metadata().num_rows(), 100);

        // Overwrite the default footer read size which less than footer length but without a file size.
        let metadata =
            read_parquet_metadata(file, None, io_client.clone(), None, None, Some(500)).await?;
        assert_eq!(metadata.file_metadata().num_rows(), 100);

        // Overwrite the default footer read size which less than footer length and a file size.
        let metadata =
            read_parquet_metadata(file, Some(size), io_client.clone(), None, None, Some(500))
                .await?;
        assert_eq!(metadata.file_metadata().num_rows(), 100);

        // Overwrite the default footer read size less than 8 bytes.
        let metadata =
            read_parquet_metadata(file, None, io_client.clone(), None, None, Some(5)).await?;
        assert_eq!(metadata.file_metadata().num_rows(), 100);

        // Test with invalid file size, assume file size is 10 bytes.
        let result =
            read_parquet_metadata(file, Some(10), io_client.clone(), None, None, None).await;
        assert!(matches!(result, Err(Error::FileTooSmall { .. })));

        // Test with invalid footer size, assume file size is 1260 bytes.
        let result = read_parquet_metadata(file, Some(1260), io_client, None, None, None).await;
        assert!(matches!(result, Err(Error::InvalidParquetFile { .. })));

        Ok(())
    }
}
