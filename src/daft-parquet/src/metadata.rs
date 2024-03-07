use std::{collections::BTreeMap, sync::Arc};

use daft_core::datatypes::Field;
use daft_io::{IOClient, IOStatsRef};

pub use parquet2::metadata::{FileMetaData, RowGroupMetaData};
use parquet2::read::deserialize_metadata;
use parquet2::schema::types::{FieldInfo, ParquetType, PrimitiveType};
use snafu::ResultExt;

use crate::{Error, JoinSnafu, UnableToParseMetadataSnafu};

fn metadata_len(buffer: &[u8], len: usize) -> i32 {
    i32::from_le_bytes(buffer[len - 8..len - 4].try_into().unwrap())
}

fn rename_parquet_type(
    parquet_type: &ParquetType,
    field_id_mapping: &BTreeMap<i32, Field>,
) -> ParquetType {
    match parquet_type {
        ParquetType::PrimitiveType(primitive_type) => {
            let field_id = primitive_type.field_info.id;
            let new_name = field_id
                .and_then(|field_id| field_id_mapping.get(&field_id))
                .map(|matched_field| matched_field.name.clone())
                .unwrap_or_else(|| primitive_type.field_info.name.clone());
            let new_field_info = FieldInfo {
                name: new_name.clone(),
                ..primitive_type.field_info
            };
            let new_primitive_type = PrimitiveType {
                field_info: new_field_info,
                ..primitive_type.clone()
            };
            ParquetType::PrimitiveType(new_primitive_type)
        }
        ParquetType::GroupType {
            field_info,
            fields,
            logical_type,
            converted_type,
        } => {
            let field_id = field_info.id;
            let new_name = field_id
                .and_then(|field_id| field_id_mapping.get(&field_id))
                .map(|matched_field| matched_field.name.clone())
                .unwrap_or_else(|| field_info.name.clone());
            let new_field_info = FieldInfo {
                name: new_name.clone(),
                ..field_info.clone()
            };
            let new_fields = fields
                .iter()
                .map(|parquet_type| rename_parquet_type(parquet_type, field_id_mapping))
                .collect();
            ParquetType::GroupType {
                field_info: new_field_info,
                fields: new_fields,
                logical_type: *logical_type,
                converted_type: *converted_type,
            }
        }
    }
}

fn rename_parquet_metadata_with_field_ids(
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
            .map(|pq_type| rename_parquet_type(pq_type, field_id_mapping))
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
            let columns = rg
                .columns()
                .iter()
                .map(|column| {
                    let field_id = column.descriptor().descriptor.primitive_type.field_info.id;
                    let col_descr = field_id_to_column_descriptor_mapping
                        .get(
                            &field_id
                                .expect("TODO: Need a field ID, figure out what to do without one"),
                        )
                        .expect("Should have a corresponding entry in the schema");
                    ColumnChunkMetaData::new(column.column_chunk().clone(), (*col_descr).clone())
                })
                .collect();
            parquet2::metadata::RowGroupMetaData::new(columns, rg.num_rows(), rg.total_byte_size())
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
        rename_parquet_metadata_with_field_ids(file_metadata?, field_id_mapping.as_ref())
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
