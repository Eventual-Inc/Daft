use std::{collections::BTreeMap, sync::Arc};

use common_error::DaftResult;
use daft_core::datatypes::Field;
use daft_dsl::common_treenode::{TreeNode, TreeNodeRewriter, VisitRecursion};
use daft_io::{IOClient, IOStatsRef};

pub use parquet2::metadata::{FileMetaData, RowGroupMetaData};
use parquet2::read::deserialize_metadata;
use parquet2::schema::types::ParquetType;
use snafu::ResultExt;

use crate::{Error, JoinSnafu, UnableToParseMetadataSnafu};

fn metadata_len(buffer: &[u8], len: usize) -> i32 {
    i32::from_le_bytes(buffer[len - 8..len - 4].try_into().unwrap())
}

struct ParquetTypeWrapper(ParquetType);

impl TreeNode for ParquetTypeWrapper {
    fn apply_children<F>(&self, op: &mut F) -> DaftResult<VisitRecursion>
    where
        F: FnMut(&Self) -> DaftResult<VisitRecursion>,
    {
        match &self.0 {
            ParquetType::PrimitiveType(..) => Ok(VisitRecursion::Skip),
            ParquetType::GroupType { fields, .. } => {
                for child in fields.iter() {
                    // TODO: Expensive clone here because of ParquetTypeWrapper type, can we get rid of this?
                    match op(&ParquetTypeWrapper(child.clone()))? {
                        VisitRecursion::Continue => {}
                        VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                        VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
                    }
                }
                Ok(VisitRecursion::Continue)
            }
        }
    }

    fn map_children<F>(self, transform: F) -> DaftResult<Self>
    where
        F: FnMut(Self) -> DaftResult<Self>,
    {
        let mut transform = transform;

        match self.0 {
            ParquetType::PrimitiveType(..) => Ok(self),
            ParquetType::GroupType {
                field_info,
                logical_type,
                converted_type,
                fields,
            } => Ok(ParquetTypeWrapper(ParquetType::GroupType {
                fields: fields
                    .into_iter()
                    .map(|child| transform(ParquetTypeWrapper(child)).map(|wrapper| wrapper.0))
                    .collect::<DaftResult<Vec<_>>>()?,
                field_info,
                logical_type,
                converted_type,
            })),
        }
    }
}

struct ParquetTypeFieldIdRewriter<'a> {
    field_id_mapping: &'a BTreeMap<i32, Field>,
}

impl<'a> TreeNodeRewriter for ParquetTypeFieldIdRewriter<'a> {
    type N = ParquetTypeWrapper;

    fn mutate(&mut self, node: Self::N) -> DaftResult<Self::N> {
        match node.0 {
            ParquetType::PrimitiveType(mut primitive_type) => {
                // Fix the `node`'s name
                let field_id = primitive_type.field_info.id;
                if let Some(mapped_field) =
                    field_id.and_then(|field_id| self.field_id_mapping.get(&field_id))
                {
                    primitive_type.field_info.name = mapped_field.name.clone();
                }
                Ok(ParquetTypeWrapper(ParquetType::PrimitiveType(
                    primitive_type,
                )))
            }
            ParquetType::GroupType {
                mut field_info,
                fields,
                logical_type,
                converted_type,
            } => {
                // Fix the `node`'s name
                let field_id = field_info.id;
                if let Some(mapped_field) =
                    field_id.and_then(|field_id| self.field_id_mapping.get(&field_id))
                {
                    field_info.name = mapped_field.name.clone();
                }

                let fields = match logical_type {
                    // GroupTypes with logical types List/Map have intermediate child nested fields without field_ids,
                    // but we need to keep recursing on them to keep renaming on their children.
                    Some(_) => fields,
                    // GroupTypes without logical_types are just structs, and we can go ahead with removing
                    // any fields that don't have field IDs with a corresponding match in the mapping
                    None => fields
                        .into_iter()
                        .filter(|f| {
                            f.get_field_info()
                                .id
                                .and_then(|field_id| self.field_id_mapping.get(&field_id))
                                .is_some()
                        })
                        .collect(),
                };

                Ok(ParquetTypeWrapper(ParquetType::GroupType {
                    field_info,
                    logical_type,
                    converted_type,
                    fields,
                }))
            }
        }
    }
}

/// Applies field_ids to a ParquetType, returns `None` if the field ID is not found
/// in the `field_id_mapping`, or if the field has no field ID.
fn apply_field_ids_to_parquet_type(
    parquet_type: ParquetType,
    field_id_mapping: &BTreeMap<i32, Field>,
) -> Option<ParquetType> {
    let field_id = parquet_type.get_field_info().id;
    if field_id
        .and_then(|field_id| field_id_mapping.get(&field_id))
        .is_some()
    {
        let mut pq_type_rewriter = ParquetTypeFieldIdRewriter { field_id_mapping };
        let rewritten_pq_type = ParquetTypeWrapper(parquet_type)
            .rewrite(&mut pq_type_rewriter)
            .unwrap()
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
        .map(|rg| {
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
        .collect();

    Ok(FileMetaData {
        row_groups: new_row_groups,
        schema_descr: new_schema_descr,
        ..file_metadata
    })
}

pub(crate) async fn read_parquet_metadata(
    uri: &str,
    size: usize,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
) -> super::Result<FileMetaData> {
    const FOOTER_SIZE: usize = 8;
    const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];
    if size < 12 {
        return Err(Error::FileTooSmall {
            path: uri.into(),
            file_size: size,
        });
    }

    /// The number of bytes read at the end of the parquet file on first read
    const DEFAULT_FOOTER_READ_SIZE: usize = 128 * 1024;
    let default_end_len = std::cmp::min(DEFAULT_FOOTER_READ_SIZE, size);

    let start = size.saturating_sub(default_end_len);
    let mut data = io_client
        .single_url_get(uri.into(), Some(start..size), io_stats.clone())
        .await?
        .bytes()
        .await?;

    let buffer = data.as_ref();
    if buffer[buffer.len() - 4..] != PARQUET_MAGIC {
        return Err(Error::InvalidParquetFile {
            path: uri.into(),
            footer: buffer[buffer.len() - 4..].into(),
        });
    }
    let metadata_size = metadata_len(buffer, default_end_len);
    let footer_len = FOOTER_SIZE + metadata_size as usize;

    if footer_len > size {
        return Err(Error::InvalidParquetFooterSize {
            path: uri.into(),
            footer_size: footer_len,
            file_size: size,
        });
    }

    let remaining;
    if footer_len < buffer.len() {
        // the whole metadata is in the bytes we already read
        remaining = buffer.len() - footer_len;
    } else {
        // the end of file read by default is not long enough, read again including the metadata.

        let start = size.saturating_sub(footer_len);
        data = io_client
            .single_url_get(uri.into(), Some(start..size), io_stats)
            .await?
            .bytes()
            .await?;
        remaining = data.len() - footer_len;
    };

    let buffer = data.as_ref();

    if buffer[buffer.len() - 4..] != PARQUET_MAGIC {
        return Err(Error::InvalidParquetFile {
            path: uri.into(),
            footer: buffer[buffer.len() - 4..].into(),
        });
    }
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

    #[tokio::test]
    async fn test_parquet_metadata_from_s3() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet";
        let size = 9882;

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let metadata = read_parquet_metadata(file, size, io_client.clone(), None, None).await?;
        assert_eq!(metadata.num_rows, 100);

        Ok(())
    }
}
