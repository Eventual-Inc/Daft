use std::{collections::BTreeMap, sync::Arc};

use bytes::{Bytes, BytesMut};
use common_error::DaftResult;
use daft_core::datatypes::Field;
use daft_io::{GetRange, IOClient, IOStatsRef};
use snafu::ResultExt;

use crate::{Error, JoinSnafu};

const FOOTER_SIZE: usize = 8;

fn metadata_len(buffer: &[u8], len: usize) -> i32 {
    i32::from_le_bytes(buffer[len - 8..len - 4].try_into().unwrap())
}

// ---------------------------------------------------------------------------
// Arrow-rs field ID mapping
// ---------------------------------------------------------------------------

/// Extract the optional field ID from a parquet `BasicTypeInfo`.
fn get_field_id(info: &parquet::schema::types::BasicTypeInfo) -> Option<i32> {
    info.has_id().then(|| info.id())
}

/// Rewrite children of a group type per the field_id_mapping.
///
/// When the parent has a logical type (LIST/MAP), intermediate children without
/// a field ID are preserved (with their own children recursed). Otherwise
/// (plain struct), unmapped children are dropped.
fn rewrite_children(
    fields: &[std::sync::Arc<parquet::schema::types::Type>],
    has_logical_type: bool,
    field_id_mapping: &BTreeMap<i32, Field>,
) -> Vec<std::sync::Arc<parquet::schema::types::Type>> {
    fields
        .iter()
        .filter_map(|child| {
            if has_logical_type {
                // LIST/MAP intermediate nodes may lack field IDs but still
                // contain mapped descendants (e.g. struct fields inside a list).
                Some(std::sync::Arc::new(
                    rewrite_arrowrs_type_with_field_ids(child, field_id_mapping)
                        .unwrap_or_else(|| recurse_children_only(child, field_id_mapping)),
                ))
            } else {
                // Plain struct: drop children not in mapping.
                rewrite_arrowrs_type_with_field_ids(child, field_id_mapping)
                    .map(std::sync::Arc::new)
            }
        })
        .collect()
}

/// Recursively rewrite an arrow-rs `Type`, renaming fields per the mapping and
/// dropping unmapped struct children.  Returns `None` if this field should be
/// dropped entirely (no field ID, or ID not in the mapping).
fn rewrite_arrowrs_type_with_field_ids(
    tp: &parquet::schema::types::Type,
    field_id_mapping: &BTreeMap<i32, Field>,
) -> Option<parquet::schema::types::Type> {
    use parquet::schema::types::Type;

    let info = tp.get_basic_info();
    let mapped_field = get_field_id(info).and_then(|fid| field_id_mapping.get(&fid))?;

    match tp {
        Type::PrimitiveType {
            physical_type,
            type_length,
            scale,
            precision,
            ..
        } => {
            let new_type = Type::primitive_type_builder(&mapped_field.name, *physical_type)
                .with_repetition(info.repetition())
                .with_converted_type(info.converted_type())
                .with_logical_type(info.logical_type_ref().cloned())
                .with_length(*type_length)
                .with_precision(*precision)
                .with_scale(*scale)
                .with_id(get_field_id(info))
                .build()
                .expect("rebuilding primitive type with same attributes should not fail");
            Some(new_type)
        }
        Type::GroupType { fields, .. } => {
            let new_children =
                rewrite_children(fields, info.logical_type_ref().is_some(), field_id_mapping);
            let new_type = Type::group_type_builder(&mapped_field.name)
                .with_repetition(info.repetition())
                .with_converted_type(info.converted_type())
                .with_logical_type(info.logical_type_ref().cloned())
                .with_fields(new_children)
                .with_id(get_field_id(info))
                .build()
                .expect("rebuilding group type with same attributes should not fail");
            Some(new_type)
        }
    }
}

/// Rebuild a group node preserving its original name/attributes, but still
/// recursing into its children to rename/filter them per the field_id_mapping.
/// For primitive nodes, returns an unchanged clone.
fn recurse_children_only(
    tp: &parquet::schema::types::Type,
    field_id_mapping: &BTreeMap<i32, Field>,
) -> parquet::schema::types::Type {
    use parquet::schema::types::Type;
    match tp {
        Type::PrimitiveType { .. } => tp.clone(),
        Type::GroupType { fields, .. } => {
            let info = tp.get_basic_info();
            let new_children =
                rewrite_children(fields, info.logical_type_ref().is_some(), field_id_mapping);
            Type::group_type_builder(info.name())
                .with_repetition(info.repetition())
                .with_converted_type(info.converted_type())
                .with_logical_type(info.logical_type_ref().cloned())
                .with_fields(new_children)
                .with_id(get_field_id(info))
                .build()
                .expect("rebuilding group type with same attributes should not fail")
        }
    }
}

/// Applies field_ids to an arrow-rs `ParquetMetaData`:
/// 1. Rename columns based on the `field_id_mapping`
/// 2. Drop columns without a field_id or without a corresponding mapping entry
pub(crate) fn apply_field_ids_to_arrowrs_parquet_metadata(
    metadata: Arc<parquet::file::metadata::ParquetMetaData>,
    field_id_mapping: &BTreeMap<i32, Field>,
) -> DaftResult<Arc<parquet::file::metadata::ParquetMetaData>> {
    use parquet::{
        file::metadata::{
            ColumnChunkMetaData, FileMetaData as ArrowrsFileMetaData, ParquetMetaData,
            RowGroupMetaData,
        },
        schema::types::{SchemaDescriptor, Type},
    };

    let old_schema = metadata.file_metadata().schema_descr();
    let old_root = old_schema.root_schema();

    // 1. Rewrite the schema type tree: rename + filter by field_id_mapping
    let new_fields: Vec<_> = old_root
        .get_fields()
        .iter()
        .filter_map(|field| {
            rewrite_arrowrs_type_with_field_ids(field, field_id_mapping).map(Arc::new)
        })
        .collect();

    let new_root = Type::group_type_builder(old_root.name())
        .with_fields(new_fields)
        .build()
        .map_err(|e| common_error::DaftError::External(e.into()))?;
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
            let new_columns: Vec<ColumnChunkMetaData> = rg
                .columns()
                .iter()
                .filter_map(|col| {
                    let col_info = col.column_descr().self_type().get_basic_info();
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
                .map_err(|e| common_error::DaftError::External(e.into()))
        })
        .collect();

    // 4. Rebuild FileMetaData and ParquetMetaData
    let fm = metadata.file_metadata();
    let new_file_metadata = ArrowrsFileMetaData::new(
        fm.version(),
        fm.num_rows(),
        fm.created_by().map(|s| s.to_string()),
        fm.key_value_metadata().cloned(),
        new_schema_descr,
        fm.column_orders().cloned(),
    );

    Ok(Arc::new(ParquetMetaData::new(
        new_file_metadata,
        new_row_groups?,
    )))
}

/// Strip STRING/UTF8 logical types from BYTE_ARRAY columns in parquet metadata.
///
/// This makes arrow-rs infer `Binary` instead of `Utf8` for string columns,
/// which avoids UTF-8 validation during decode. Used to implement
/// `StringEncoding::Raw` which reads string columns as raw bytes.
pub(crate) fn strip_string_types_from_parquet_metadata(
    metadata: Arc<parquet::file::metadata::ParquetMetaData>,
) -> DaftResult<Arc<parquet::file::metadata::ParquetMetaData>> {
    use parquet::{
        file::metadata::{FileMetaData as ArrowrsFileMetaData, ParquetMetaData, RowGroupMetaData},
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
        .map_err(|e| common_error::DaftError::External(e.into()))?;
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
                .map_err(|e| common_error::DaftError::External(e.into()))
        })
        .collect();

    let fm = metadata.file_metadata();
    let new_file_metadata = ArrowrsFileMetaData::new(
        fm.version(),
        fm.num_rows(),
        fm.created_by().map(|s| s.to_string()),
        fm.key_value_metadata().cloned(),
        new_schema_descr,
        fm.column_orders().cloned(),
    );

    Ok(Arc::new(ParquetMetaData::new(
        new_file_metadata,
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

pub(crate) fn validate_footer_magic(uri: &str, buffer: &[u8]) -> super::Result<()> {
    const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

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

    let metadata_size = metadata_len(buffer, buffer.len());
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
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
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

    let metadata = tokio::task::spawn_blocking(move || {
        let thrift_bytes = &data.as_ref()[remaining..data.len() - FOOTER_SIZE];
        parquet::file::metadata::ParquetMetaDataReader::decode_metadata(thrift_bytes)
    })
    .await
    .context(JoinSnafu {
        path: uri.to_string(),
    })?
    .map_err(|e| Error::UnableToParseMetadataArrowRs {
        path: uri.to_string(),
        source: e,
    })?;

    let metadata = Arc::new(metadata);

    if let Some(field_id_mapping) = field_id_mapping {
        apply_field_ids_to_arrowrs_parquet_metadata(metadata, field_id_mapping.as_ref()).map_err(
            |e| Error::UnableToParseMetadataArrowRs {
                path: uri.to_string(),
                source: parquet::errors::ParquetError::External(e.into()),
            },
        )
    } else {
        Ok(metadata)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_io::{IOClient, IOConfig};

    use super::read_parquet_metadata;
    use crate::Error;

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
