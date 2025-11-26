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
