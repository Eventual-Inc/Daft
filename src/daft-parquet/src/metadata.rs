use std::{collections::BTreeMap, sync::Arc};

use bytes::{Bytes, BytesMut};
use common_error::DaftResult;
use daft_core::datatypes::Field;
use daft_dsl::common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_io::{GetRange, IOClient, IOStatsRef};
pub use parquet2::metadata::{FileMetaData, RowGroupMetaData};
use parquet2::{read::deserialize_metadata, schema::types::ParquetType};
use snafu::ResultExt;

use crate::{Error, JoinSnafu, UnableToParseMetadataSnafu};

const FOOTER_SIZE: usize = 8;

fn metadata_len(buffer: &[u8], len: usize) -> i32 {
    i32::from_le_bytes(buffer[len - 8..len - 4].try_into().unwrap())
}

struct ParquetTypeWrapper(ParquetType);

impl TreeNode for ParquetTypeWrapper {
    fn apply_children<F: FnMut(&Self) -> DaftResult<TreeNodeRecursion>>(
        &self,
        mut op: F,
    ) -> DaftResult<TreeNodeRecursion> {
        match &self.0 {
            ParquetType::PrimitiveType(..) => Ok(TreeNodeRecursion::Jump),
            ParquetType::GroupType { fields, .. } => {
                for child in fields {
                    // TODO: Expensive clone here because of ParquetTypeWrapper type, can we get rid of this?
                    match op(&Self(child.clone()))? {
                        TreeNodeRecursion::Continue => {}
                        TreeNodeRecursion::Jump => return Ok(TreeNodeRecursion::Continue),
                        TreeNodeRecursion::Stop => return Ok(TreeNodeRecursion::Stop),
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            }
        }
    }

    fn map_children<F: FnMut(Self) -> DaftResult<Transformed<Self>>>(
        self,
        transform: F,
    ) -> DaftResult<Transformed<Self>> {
        let mut transform = transform;

        match self.0 {
            ParquetType::PrimitiveType(..) => Ok(Transformed::no(self)),
            ParquetType::GroupType {
                field_info,
                logical_type,
                converted_type,
                fields,
            } => Ok(Transformed::yes(Self(ParquetType::GroupType {
                fields: fields
                    .into_iter()
                    .map(|child| transform(Self(child)).map(|wrapper| wrapper.data.0))
                    .collect::<DaftResult<Vec<_>>>()?,
                field_info,
                logical_type,
                converted_type,
            }))),
        }
    }
}

fn rewrite_parquet_type_with_field_id_mapping(
    mut pq_type: ParquetTypeWrapper,
    field_id_mapping: &BTreeMap<i32, Field>,
) -> DaftResult<Transformed<ParquetTypeWrapper>> {
    match pq_type.0 {
        ParquetType::PrimitiveType(ref mut primitive_type) => {
            let field_id = primitive_type.field_info.id;
            if let Some(mapped_field) =
                field_id.and_then(|field_id| field_id_mapping.get(&field_id))
            {
                // Fix the `pq_type`'s name
                primitive_type
                    .field_info
                    .name
                    .clone_from(&mapped_field.name);
                return Ok(Transformed::yes(pq_type));
            }
        }
        ParquetType::GroupType {
            ref mut field_info,
            ref mut fields,
            ref logical_type,
            ..
        } => {
            let field_id = field_info.id;
            if let Some(mapped_field) =
                field_id.and_then(|field_id| field_id_mapping.get(&field_id))
            {
                // Fix the `pq_type`'s name
                field_info.name.clone_from(&mapped_field.name);

                // Fix the `pq_type`'s fields, keeping only the fields that are correctly mapped to a field_id
                match logical_type {
                    // GroupTypes with logical types List/Map have intermediate child nested fields without field_ids,
                    // but we need to keep recursing on them to keep renaming on their children.
                    Some(_) => {}
                    // GroupTypes without logical_types are just structs, and we can go ahead with removing
                    // any fields that don't have field IDs with a corresponding match in the mapping
                    None => {
                        fields.retain(|f| {
                            f.get_field_info()
                                .id
                                .is_some_and(|field_id| field_id_mapping.contains_key(&field_id))
                        });
                    }
                }

                return Ok(Transformed::yes(pq_type));
            }
        }
    }
    Ok(Transformed::no(pq_type))
}

/// Applies field_ids to a ParquetType, returns `None` if the field ID is not found
/// in the `field_id_mapping`, or if the field has no field ID.
fn apply_field_ids_to_parquet_type(
    parquet_type: ParquetType,
    field_id_mapping: &BTreeMap<i32, Field>,
) -> Option<ParquetType> {
    let field_id = parquet_type.get_field_info().id;
    if field_id.is_some_and(|field_id| field_id_mapping.contains_key(&field_id)) {
        let rewritten_pq_type = ParquetTypeWrapper(parquet_type)
            .transform(&|pq_type| {
                rewrite_parquet_type_with_field_id_mapping(pq_type, field_id_mapping)
            })
            .unwrap()
            .data
            .0;
        Some(rewritten_pq_type)
    } else {
        None
    }
}

/// Applies field_ids to a parquet2 FileMetaData struct
/// 1. Rename columns based on the `field_id_mapping`
/// 2. Drop any columns that don't have a `field_id`, or that don't have a corresponding entry in the `field_id_mapping`
fn apply_field_ids_to_parquet_file_metadata(
    file_metadata: FileMetaData,
    field_id_mapping: &BTreeMap<i32, Field>,
) -> super::Result<FileMetaData> {
    use parquet2::metadata::{ColumnChunkMetaData, SchemaDescriptor};

    // Construct a new Parquet Schema after applying renaming using field_ids
    let new_schema_descr = SchemaDescriptor::new(
        file_metadata.schema_descr.name().to_string(),
        file_metadata
            .schema_descr
            .fields()
            .iter()
            .filter_map(|pq_type| {
                apply_field_ids_to_parquet_type(pq_type.clone(), field_id_mapping)
            })
            .collect(),
    );

    // Get a mapping of {field_id: ColumnDescriptor} to use in modifying the redundant ColumnDescriptors in our RowGroupMetadata
    let field_id_to_column_descriptor_mapping = new_schema_descr
        .columns()
        .iter()
        .filter_map(|col_descr| {
            col_descr
                .descriptor
                .primitive_type
                .field_info
                .id
                .map(|field_id| (field_id, col_descr))
        })
        .collect::<BTreeMap<_, _>>();

    let new_row_groups = file_metadata
        .row_groups
        .iter()
        .map(|(_, rg)| {
            let new_columns = rg
                .columns()
                .iter()
                .filter_map(|column| {
                    let field_id = column.descriptor().descriptor.primitive_type.field_info.id;
                    field_id
                        .and_then(|field_id| field_id_to_column_descriptor_mapping.get(&field_id))
                        .map(|col_descr| {
                            ColumnChunkMetaData::new(
                                column.column_chunk().clone(),
                                (*col_descr).clone(),
                            )
                        })
                })
                .collect::<Vec<_>>();
            let new_total_uncompressed_size = new_columns
                .iter()
                .map(|c| c.uncompressed_size() as usize)
                .sum::<usize>();
            parquet2::metadata::RowGroupMetaData::new(
                new_columns,
                rg.num_rows(),
                new_total_uncompressed_size,
            )
        })
        .enumerate()
        .collect();

    Ok(FileMetaData {
        row_groups: new_row_groups,
        schema_descr: new_schema_descr,
        ..file_metadata
    })
}

// ---------------------------------------------------------------------------
// Arrow-rs field ID mapping
// ---------------------------------------------------------------------------

/// Extract the optional field ID from a parquet `BasicTypeInfo`.
fn get_field_id(info: &parquet::schema::types::BasicTypeInfo) -> Option<i32> {
    info.has_id().then(|| info.id())
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

    // If this node has no field ID or its ID isn't in the mapping, drop it.
    let mapped_field = get_field_id(info).and_then(|fid| field_id_mapping.get(&fid))?;

    match tp {
        Type::PrimitiveType {
            basic_info: _,
            physical_type,
            type_length,
            scale,
            precision,
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
            let has_logical_type = info.logical_type_ref().is_some();
            let new_children: Vec<_> = fields
                .iter()
                .filter_map(|child| {
                    if has_logical_type {
                        // List/Map intermediate nodes: always recurse (they may lack field IDs)
                        Some(std::sync::Arc::new(
                            rewrite_arrowrs_type_with_field_ids(child, field_id_mapping)
                                .unwrap_or_else(|| child.as_ref().clone()),
                        ))
                    } else {
                        // Plain struct: drop children not in mapping
                        rewrite_arrowrs_type_with_field_ids(child, field_id_mapping)
                            .map(std::sync::Arc::new)
                    }
                })
                .collect();
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

pub(crate) async fn read_parquet_metadata(
    uri: &str,
    file_size: Option<usize>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    default_footer_read_size: Option<usize>,
) -> super::Result<FileMetaData> {
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

    // use rayon here
    let file_metadata = tokio::task::spawn_blocking(move || {
        let reader = &data.as_ref()[remaining..];
        let max_size = reader.len() * 2 + 1024;
        deserialize_metadata(reader, max_size)
    })
    .await
    .context(JoinSnafu {
        path: uri.to_string(),
    })?
    .context(UnableToParseMetadataSnafu { path: uri });

    if let Some(field_id_mapping) = field_id_mapping {
        apply_field_ids_to_parquet_file_metadata(file_metadata?, field_id_mapping.as_ref())
    } else {
        file_metadata
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
        assert_eq!(metadata.num_rows, 100);

        // Read metadata without a file size.
        let metadata =
            read_parquet_metadata(file, None, io_client.clone(), None, None, None).await?;
        assert_eq!(metadata.num_rows, 100);

        // Overwrite the default footer read size which less than footer length but without a file size.
        let metadata =
            read_parquet_metadata(file, None, io_client.clone(), None, None, Some(500)).await?;
        assert_eq!(metadata.num_rows, 100);

        // Overwrite the default footer read size which less than footer length and a file size.
        let metadata =
            read_parquet_metadata(file, Some(size), io_client.clone(), None, None, Some(500))
                .await?;
        assert_eq!(metadata.num_rows, 100);

        // Overwrite the default footer read size less than 8 bytes.
        let metadata =
            read_parquet_metadata(file, None, io_client.clone(), None, None, Some(5)).await?;
        assert_eq!(metadata.num_rows, 100);

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
