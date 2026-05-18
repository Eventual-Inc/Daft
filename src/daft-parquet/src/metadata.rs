use std::{collections::BTreeMap, sync::Arc};

use bytes::{Bytes, BytesMut};
use common_error::DaftResult;
use daft_core::datatypes::Field;
use daft_io::{GetRange, IOClient, IOStatsRef};
use parquet::{
    basic::{ConvertedType, LogicalType, Type as PhysicalType},
    file::metadata::{ColumnChunkMetaData, FileMetaData, ParquetMetaData, RowGroupMetaData},
    schema::types::{BasicTypeInfo, SchemaDescriptor, Type},
};
use snafu::ResultExt;

use crate::{Error, JoinSnafu};

const FOOTER_SIZE: usize = 8;

fn metadata_len(buffer: &[u8], len: usize) -> i32 {
    i32::from_le_bytes(buffer[len - 8..len - 4].try_into().unwrap())
}

// ─── parquet `Type` rebuilders ──────────────────────────────────────────────

fn field_id(info: &BasicTypeInfo) -> Option<i32> {
    info.has_id().then(|| info.id())
}

/// Rebuild a primitive `Type` with a (possibly new) name, preserving every
/// other attribute from `info`. Panics on builder error — we're echoing
/// validated data, not constructing from scratch.
fn rebuild_primitive(
    name: &str,
    info: &BasicTypeInfo,
    physical: PhysicalType,
    type_length: i32,
    scale: i32,
    precision: i32,
) -> Type {
    Type::primitive_type_builder(name, physical)
        .with_repetition(info.repetition())
        .with_converted_type(info.converted_type())
        .with_logical_type(info.logical_type_ref().cloned())
        .with_length(type_length)
        .with_precision(precision)
        .with_scale(scale)
        .with_id(field_id(info))
        .build()
        .expect("rebuilding primitive type from validated info should not fail")
}

/// Rebuild a group `Type` with a (possibly new) name + children, preserving
/// other attributes from `info`.
fn rebuild_group(name: &str, info: &BasicTypeInfo, fields: Vec<Arc<Type>>) -> Type {
    Type::group_type_builder(name)
        .with_repetition(info.repetition())
        .with_converted_type(info.converted_type())
        .with_logical_type(info.logical_type_ref().cloned())
        .with_fields(fields)
        .with_id(field_id(info))
        .build()
        .expect("rebuilding group type from validated info should not fail")
}

// ─── shared metadata-rebuild orchestrator ───────────────────────────────────

/// Rebuild a `ParquetMetaData` with a transformed schema root and a per-RG
/// transform. Preserves all file-level attributes (version, num_rows, kv
/// metadata, column orders).
fn rebuild_parquet_metadata(
    metadata: &ParquetMetaData,
    new_root: Type,
    rebuild_rg: impl Fn(&RowGroupMetaData, &Arc<SchemaDescriptor>) -> DaftResult<RowGroupMetaData>,
) -> DaftResult<Arc<ParquetMetaData>> {
    let new_schema_descr = Arc::new(SchemaDescriptor::new(Arc::new(new_root)));
    let new_row_groups: Vec<RowGroupMetaData> = metadata
        .row_groups()
        .iter()
        .map(|rg| rebuild_rg(rg, &new_schema_descr))
        .collect::<DaftResult<_>>()?;

    let fm = metadata.file_metadata();
    let new_file_metadata = FileMetaData::new(
        fm.version(),
        fm.num_rows(),
        fm.created_by().map(str::to_string),
        fm.key_value_metadata().cloned(),
        new_schema_descr,
        fm.column_orders().cloned(),
    );
    Ok(Arc::new(ParquetMetaData::new(
        new_file_metadata,
        new_row_groups,
    )))
}

fn builder_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> common_error::DaftError {
    common_error::DaftError::External(e.into())
}

// ─── field-id mapping ────────────────────────────────────────────────────────

/// Rewrite children of a group per the field_id_mapping.
///
/// Under a LIST/MAP parent, intermediate children without a field ID are
/// preserved (recursing into their own children). Under a plain struct,
/// unmapped children are dropped.
fn rewrite_children(
    fields: &[Arc<Type>],
    parent_has_logical_type: bool,
    field_id_mapping: &BTreeMap<i32, Field>,
) -> Vec<Arc<Type>> {
    fields
        .iter()
        .filter_map(|child| {
            if parent_has_logical_type {
                Some(Arc::new(
                    rewrite_type_with_field_ids(child, field_id_mapping)
                        .unwrap_or_else(|| recurse_children_only(child, field_id_mapping)),
                ))
            } else {
                rewrite_type_with_field_ids(child, field_id_mapping).map(Arc::new)
            }
        })
        .collect()
}

/// Recursively rewrite a `Type`, renaming fields per the mapping and dropping
/// unmapped struct children. Returns `None` if this field has no mapping entry.
fn rewrite_type_with_field_ids(tp: &Type, field_id_mapping: &BTreeMap<i32, Field>) -> Option<Type> {
    let info = tp.get_basic_info();
    let mapped = field_id(info).and_then(|fid| field_id_mapping.get(&fid))?;
    Some(match tp {
        Type::PrimitiveType {
            physical_type,
            type_length,
            scale,
            precision,
            ..
        } => rebuild_primitive(
            &mapped.name,
            info,
            *physical_type,
            *type_length,
            *scale,
            *precision,
        ),
        Type::GroupType { fields, .. } => rebuild_group(
            &mapped.name,
            info,
            rewrite_children(fields, info.logical_type_ref().is_some(), field_id_mapping),
        ),
    })
}

/// Preserve this group's name/attributes but recurse into its children
/// (used for LIST/MAP intermediate nodes that lack a field ID).
fn recurse_children_only(tp: &Type, field_id_mapping: &BTreeMap<i32, Field>) -> Type {
    match tp {
        Type::PrimitiveType { .. } => tp.clone(),
        Type::GroupType { fields, .. } => {
            let info = tp.get_basic_info();
            rebuild_group(
                info.name(),
                info,
                rewrite_children(fields, info.logical_type_ref().is_some(), field_id_mapping),
            )
        }
    }
}

/// Rename + filter parquet columns by `field_id_mapping`. Columns without a
/// field ID (or not in the mapping) are dropped.
pub(crate) fn apply_field_ids_to_arrowrs_parquet_metadata(
    metadata: Arc<ParquetMetaData>,
    field_id_mapping: &BTreeMap<i32, Field>,
) -> DaftResult<Arc<ParquetMetaData>> {
    let old_root = metadata.file_metadata().schema_descr().root_schema();
    let new_fields: Vec<Arc<Type>> = old_root
        .get_fields()
        .iter()
        .filter_map(|f| rewrite_type_with_field_ids(f, field_id_mapping).map(Arc::new))
        .collect();
    let new_root = Type::group_type_builder(old_root.name())
        .with_fields(new_fields)
        .build()
        .map_err(builder_err)?;

    // Need a fresh schema-descriptor first to build the field-id → new-descriptor lookup.
    let new_schema_descr = Arc::new(SchemaDescriptor::new(Arc::new(new_root.clone())));
    let field_id_to_descr: BTreeMap<i32, _> = new_schema_descr
        .columns()
        .iter()
        .filter_map(|d| field_id(d.self_type().get_basic_info()).map(|fid| (fid, d.clone())))
        .collect();

    rebuild_parquet_metadata(&metadata, new_root, |rg, new_schema_descr| {
        let new_columns: Vec<ColumnChunkMetaData> = rg
            .columns()
            .iter()
            .filter_map(|col| {
                let fid = field_id(col.column_descr().self_type().get_basic_info())?;
                let new_descr = field_id_to_descr.get(&fid)?;
                Some(rebuild_column_chunk(col, new_descr.clone()))
            })
            .collect();
        let total_byte_size: i64 = new_columns.iter().map(|c| c.uncompressed_size()).sum();
        RowGroupMetaData::builder(new_schema_descr.clone())
            .set_num_rows(rg.num_rows())
            .set_total_byte_size(total_byte_size)
            .set_column_metadata(new_columns)
            .build()
            .map_err(builder_err)
    })
}

/// Rebuild a `ColumnChunkMetaData` with a new descriptor. `ColumnChunkMetaData`
/// has no setter to swap the descriptor, so we re-fan the attributes.
fn rebuild_column_chunk(
    col: &ColumnChunkMetaData,
    new_descr: Arc<parquet::schema::types::ColumnDescriptor>,
) -> ColumnChunkMetaData {
    let mut b = ColumnChunkMetaData::builder(new_descr)
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
        b = b.set_statistics(stats.clone());
    }
    if let Some(path) = col.file_path() {
        b = b.set_file_path(path.to_string());
    }
    b.build().expect("column chunk rebuild should not fail")
}

// ─── string-as-binary stripping ──────────────────────────────────────────────

/// Strip STRING/UTF8 logical types from BYTE_ARRAY columns so arrow-rs infers
/// `Binary` instead of `Utf8`, skipping UTF-8 validation during decode.
pub(crate) fn strip_string_types_from_parquet_metadata(
    metadata: Arc<parquet::file::metadata::ParquetMetaData>,
) -> DaftResult<Arc<ParquetMetaData>> {
    let old_root = metadata.file_metadata().schema_descr().root_schema();
    let new_fields: Vec<Arc<Type>> = old_root
        .get_fields()
        .iter()
        .map(|f| Arc::new(strip_string_type(f)))
        .collect();
    let new_root = Type::group_type_builder(old_root.name())
        .with_fields(new_fields)
        .build()
        .map_err(builder_err)?;

    // Column-chunk byte layout is unchanged (just the logical-type annotation
    // shifts), so we can carry the existing `ColumnChunkMetaData` over.
    rebuild_parquet_metadata(&metadata, new_root, |rg, new_schema_descr| {
        RowGroupMetaData::builder(new_schema_descr.clone())
            .set_num_rows(rg.num_rows())
            .set_total_byte_size(rg.total_byte_size())
            .set_column_metadata(rg.columns().to_vec())
            .build()
            .map_err(builder_err)
    })
}

/// Recursively rebuild a parquet `Type` with STRING/UTF8 annotations stripped.
fn strip_string_type(tp: &Type) -> Type {
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
                // Drop String/UTF8 by rebuilding *without* the converted/logical type.
                Type::primitive_type_builder(info.name(), *physical_type)
                    .with_repetition(info.repetition())
                    .with_length(*type_length)
                    .with_precision(*precision)
                    .with_scale(*scale)
                    .with_id(field_id(info))
                    .build()
                    .expect("rebuilding primitive type should not fail")
            } else {
                tp.clone()
            }
        }
        Type::GroupType { fields, .. } => {
            let new_children: Vec<_> = fields
                .iter()
                .map(|c| Arc::new(strip_string_type(c)))
                .collect();
            rebuild_group(
                tp.get_basic_info().name(),
                tp.get_basic_info(),
                new_children,
            )
        }
    }
}

// ─── footer I/O + metadata deserialization ──────────────────────────────────

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

/// Fetch raw parquet footer bytes, handling suffix-range fallback and two-pass
/// reads for large footers. Returns `(footer_bytes, thrift_start_offset)`.
async fn fetch_parquet_footer_bytes(
    uri: &str,
    file_size: Option<usize>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    default_footer_read_size: Option<usize>,
) -> super::Result<(Bytes, usize)> {
    async fn fetch(
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
        Some(
            io_client
                .single_url_get_size(uri.into(), io_stats.clone())
                .await?,
        )
    } else {
        file_size
    };

    if let Some(size) = file_size_opt
        && size < 12
    {
        return Err(Error::FileTooSmall {
            path: uri.into(),
            file_size: size,
        });
    }

    const DEFAULT_FOOTER_READ_SIZE: usize = 128 * 1024;
    let footer_read_size = default_footer_read_size
        .unwrap_or(DEFAULT_FOOTER_READ_SIZE)
        .max(FOOTER_SIZE);
    let range = match file_size_opt {
        None => GetRange::Suffix(footer_read_size),
        Some(size) => (size - std::cmp::min(footer_read_size, size)..size).into(),
    };
    let mut data = fetch(io_client.clone(), uri, range, io_stats.clone()).await?;
    validate_footer_magic(uri, data.as_ref())?;

    let metadata_size = metadata_len(data.as_ref(), data.len());
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

    let remaining = if footer_len <= data.len() {
        data.len() - footer_len
    } else {
        // First read undershot the footer; fetch the rest.
        data = match file_size_opt {
            None => fetch(io_client, uri, GetRange::Suffix(footer_len), io_stats).await?,
            Some(size) => {
                let prefix_range =
                    (size.saturating_sub(footer_len)..size.saturating_sub(data.len())).into();
                let new_data = fetch(io_client, uri, prefix_range, io_stats).await?;
                let mut buf = BytesMut::with_capacity(new_data.len() + data.len());
                buf.extend_from_slice(&new_data);
                buf.extend_from_slice(&data);
                buf.freeze()
            }
        };
        0
    };

    validate_footer_magic(uri, data.as_ref())?;
    Ok((data, remaining))
}

/// Read parquet metadata via arrow-rs deserialization.
pub(crate) async fn fetch_parquet_metadata(
    uri: &str,
    file_size: Option<usize>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    default_footer_read_size: Option<usize>,
) -> super::Result<Arc<ParquetMetaData>> {
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
    if let Some(mapping) = field_id_mapping {
        apply_field_ids_to_arrowrs_parquet_metadata(metadata, mapping.as_ref()).map_err(|e| {
            Error::UnableToParseMetadataArrowRs {
                path: uri.to_string(),
                source: parquet::errors::ParquetError::External(e.into()),
            }
        })
    } else {
        Ok(metadata)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_io::{IOClient, IOConfig};

    use super::fetch_parquet_metadata;
    use crate::Error;

    #[tokio::test]
    async fn test_parquet_metadata_from_s3() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet";
        let size = 9882;

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let metadata =
            fetch_parquet_metadata(file, Some(size), io_client.clone(), None, None, None).await?;
        assert_eq!(metadata.file_metadata().num_rows(), 100);

        let metadata =
            fetch_parquet_metadata(file, None, io_client.clone(), None, None, None).await?;
        assert_eq!(metadata.file_metadata().num_rows(), 100);

        let metadata =
            fetch_parquet_metadata(file, None, io_client.clone(), None, None, Some(500)).await?;
        assert_eq!(metadata.file_metadata().num_rows(), 100);

        let metadata =
            fetch_parquet_metadata(file, Some(size), io_client.clone(), None, None, Some(500))
                .await?;
        assert_eq!(metadata.file_metadata().num_rows(), 100);

        let metadata =
            fetch_parquet_metadata(file, None, io_client.clone(), None, None, Some(5)).await?;
        assert_eq!(metadata.file_metadata().num_rows(), 100);

        let result =
            fetch_parquet_metadata(file, Some(10), io_client.clone(), None, None, None).await;
        assert!(matches!(result, Err(Error::FileTooSmall { .. })));

        let result = fetch_parquet_metadata(file, Some(1260), io_client, None, None, None).await;
        assert!(matches!(result, Err(Error::InvalidParquetFile { .. })));

        Ok(())
    }
}
