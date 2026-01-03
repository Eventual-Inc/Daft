//! Daft integration with Apache Hudi via hudi-rs.
//!
//! This crate provides a native Rust implementation of the `ScanOperator` trait
//! for reading Apache Hudi tables.

use std::{collections::HashMap, sync::Arc};

// Use arrow types from hudi_core to ensure type compatibility with the schema returned by HudiTable
use hudi_core::arrow::datatypes::Schema as ArrowSchema;
use common_error::{DaftError, DaftResult};
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};
use common_scan_info::{PartitionField, Pushdowns, ScanOperator, ScanTaskLike, ScanTaskLikeRef};
use daft_core::prelude::Utf8Array;
use daft_core::series::IntoSeries;
use daft_recordbatch::RecordBatch;
use daft_scan::storage_config::StorageConfig;
use daft_scan::{DataSource, ScanTask};
use daft_schema::dtype::DataType;
use daft_schema::field::Field;
use daft_schema::schema::{Schema, SchemaRef};
use daft_stats::{PartitionSpec, TableMetadata};
use hudi_core::config::util::empty_options;
use hudi_core::table::Table as HudiTable;
use snafu::Snafu;
use url::Url;

#[cfg(feature = "python")]
pub mod python;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Hudi error: {}", message))]
    Hudi { message: String },

    #[snafu(display("Schema conversion error: {}", message))]
    SchemaConversion { message: String },

    #[snafu(display("Invalid table URI: {}", uri))]
    InvalidUri { uri: String },
}

impl From<Error> for DaftError {
    fn from(value: Error) -> Self {
        Self::External(value.into())
    }
}

impl From<hudi_core::error::CoreError> for Error {
    fn from(err: hudi_core::error::CoreError) -> Self {
        Self::Hudi {
            message: err.to_string(),
        }
    }
}

/// Hudi table scan operator for Daft.
///
/// This implements the `ScanOperator` trait for reading Apache Hudi tables
/// using the native Rust hudi-rs library.
#[derive(Debug)]
pub struct HudiScanOperator {
    /// The underlying Hudi table
    table: HudiTable,
    /// Table schema converted to Daft schema
    schema: SchemaRef,
    /// Storage configuration
    storage_config: Arc<StorageConfig>,
    /// Partition fields
    partition_keys: Vec<PartitionField>,
}

impl HudiScanOperator {
    /// Create a new HudiScanOperator from a table URI.
    ///
    /// # Arguments
    /// * `table_uri` - The URI of the Hudi table (supports local paths and cloud storage URIs)
    /// * `storage_config` - Storage configuration for IO operations
    pub async fn try_new(
        table_uri: &str,
        storage_config: Arc<StorageConfig>,
    ) -> DaftResult<Self> {
        Self::try_new_with_options(table_uri, storage_config, empty_options()).await
    }

    /// Create a new HudiScanOperator with additional Hudi options.
    ///
    /// # Arguments
    /// * `table_uri` - The URI of the Hudi table
    /// * `storage_config` - Storage configuration for IO operations
    /// * `options` - Additional Hudi configuration options
    pub async fn try_new_with_options<I, K, V>(
        table_uri: &str,
        storage_config: Arc<StorageConfig>,
        options: I,
    ) -> DaftResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        // Build storage options from IO config
        let storage_options = build_storage_options(&storage_config);

        // Merge with user-provided options
        let all_options: Vec<(String, String)> = storage_options
            .into_iter()
            .chain(
                options
                    .into_iter()
                    .map(|(k, v)| (k.as_ref().to_string(), v.into())),
            )
            .collect();

        // Create the Hudi table
        let table = HudiTable::new_with_options(table_uri, all_options)
            .await
            .map_err(|e| Error::Hudi {
                message: e.to_string(),
            })?;

        // Get the schema
        let arrow_schema = table.get_schema().await.map_err(|e| Error::Hudi {
            message: format!("Failed to get table schema: {}", e),
        })?;

        let schema = arrow_schema_to_daft_schema(&arrow_schema)?;

        // Get partition fields
        let partition_fields: Vec<String> = table
            .hudi_configs
            .get_or_default(hudi_core::config::table::HudiTableConfig::PartitionFields)
            .into();

        let partition_keys = partition_fields
            .iter()
            .filter_map(|pf| {
                schema.get_field(pf).ok().map(|field| {
                    PartitionField::new(field.clone(), None, None).unwrap_or_else(|_| {
                        // Fallback: create a simple partition field
                        PartitionField::new(field.clone(), None, None).unwrap()
                    })
                })
            })
            .collect();

        Ok(Self {
            table,
            schema: Arc::new(schema),
            storage_config,
            partition_keys,
        })
    }

    /// Get the base URL of the Hudi table
    pub fn base_url(&self) -> Url {
        self.table.base_url()
    }

    /// Get the table name
    pub fn table_name(&self) -> String {
        self.table.table_name()
    }
}

impl ScanOperator for HudiScanOperator {
    fn name(&self) -> &str {
        "HudiScanOperator"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitioning_keys(&self) -> &[PartitionField] {
        &self.partition_keys
    }

    fn file_path_column(&self) -> Option<&str> {
        None
    }

    fn generated_fields(&self) -> Option<SchemaRef> {
        None
    }

    fn can_absorb_filter(&self) -> bool {
        false
    }

    fn can_absorb_select(&self) -> bool {
        true
    }

    fn can_absorb_limit(&self) -> bool {
        false
    }

    fn can_absorb_shard(&self) -> bool {
        false
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            format!("HudiScanOperator({})", self.table_name()),
            format!("Schema = {}", self.schema),
            format!("Partitioning keys = {:?}", self.partitioning_keys()),
        ]
    }

    fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskLikeRef>> {
        // Use the IO runtime to run async operations
        let runtime = common_runtime::get_io_runtime(true);

        // Clone necessary data for the async block
        let table = self.table.clone();
        let schema = self.schema.clone();
        let storage_config = self.storage_config.clone();
        let base_url = self.base_url();
        let partition_keys = self.partition_keys.clone();

        // Execute async file slice fetching
        runtime.block_on_current_thread(async move {
            // Get file slices from the Hudi table
            let file_slices = table
                .get_file_slices(hudi_core::config::util::empty_filters())
                .await
                .map_err(|e| Error::Hudi {
                    message: format!("Failed to get file slices: {}", e),
                })?;

            if file_slices.is_empty() {
                return Ok(Vec::new());
            }

            let limit_files = pushdowns.limit.is_some()
                && pushdowns.filters.is_none()
                && pushdowns.partition_filters.is_none();
            let mut rows_left = pushdowns.limit.unwrap_or(0);

            let file_format_config = Arc::new(FileFormatConfig::Parquet(ParquetSourceConfig {
                coerce_int96_timestamp_unit: daft_schema::time_unit::TimeUnit::Microseconds,
                field_id_mapping: None,
                row_groups: None,
                chunk_size: None,
            }));

            let mut scan_tasks: Vec<ScanTaskLikeRef> = Vec::with_capacity(file_slices.len());

            for file_slice in file_slices {
                if limit_files && rows_left <= 0 {
                    break;
                }

                // Construct the full file path
                let relative_path = file_slice.base_file_relative_path().map_err(|e| {
                    Error::Hudi {
                        message: format!("Failed to get base file path: {}", e),
                    }
                })?;

                let file_path = hudi_core::storage::util::join_url_segments(&base_url, &[&relative_path])
                    .map_err(|e| Error::Hudi {
                        message: format!("Failed to construct file URL: {}", e),
                    })?
                    .to_string();

                // Get file metadata if available
                let (num_rows, size_bytes) = if let Some(ref metadata) =
                    file_slice.base_file.file_metadata
                {
                    // Use byte_size from FileMetadata (i64) and convert to u64
                    let size = if metadata.byte_size > 0 {
                        Some(metadata.byte_size as u64)
                    } else {
                        None
                    };
                    (Some(metadata.num_records as usize), size)
                } else {
                    (None, None)
                };

                // Create partition values from partition path
                let partition_spec = if !file_slice.partition_path.is_empty()
                    && !partition_keys.is_empty()
                {
                    // Create a simple partition spec with the partition path
                    let partition_path_series = Utf8Array::from_iter(
                        "_hoodie_partition_path",
                        std::iter::once(Some(file_slice.partition_path.as_str())),
                    )
                    .into_series();

                    let partition_rb =
                        RecordBatch::from_nonempty_columns(vec![partition_path_series])?;
                    Some(PartitionSpec {
                        keys: partition_rb,
                    })
                } else {
                    None
                };

                // Create table metadata
                let metadata = num_rows.map(|length| TableMetadata { length });

                // Create the data source
                let data_source = DataSource::File {
                    path: file_path,
                    chunk_spec: None,
                    size_bytes,
                    iceberg_delete_files: None,
                    metadata,
                    partition_spec,
                    statistics: None,
                    parquet_metadata: None,
                };

                // Create the scan task
                let scan_task = ScanTask::new(
                    vec![data_source],
                    file_format_config.clone(),
                    schema.clone(),
                    storage_config.clone(),
                    pushdowns.clone(),
                    None,
                );

                if let Some(num) = num_rows {
                    rows_left = rows_left.saturating_sub(num);
                }

                scan_tasks.push(Arc::new(scan_task) as Arc<dyn ScanTaskLike>);
            }

            Ok(scan_tasks)
        })
    }
}

/// Convert Arrow schema from hudi-core to Daft schema.
///
/// Note: hudi-core with `arrow-54` feature re-exports `arrow_v54` as `hudi_core::arrow`.
/// This is a different crate from Daft's `arrow` dependency, so we need to manually
/// convert types rather than using `TryFrom` which only works within the same crate.
fn arrow_schema_to_daft_schema(arrow_schema: &ArrowSchema) -> DaftResult<Schema> {
    use hudi_core::arrow::datatypes::DataType as HudiArrowType;
    use hudi_core::arrow::datatypes::TimeUnit as HudiTimeUnit;

    fn convert_time_unit(tu: &HudiTimeUnit) -> daft_schema::time_unit::TimeUnit {
        match tu {
            HudiTimeUnit::Second => daft_schema::time_unit::TimeUnit::Seconds,
            HudiTimeUnit::Millisecond => daft_schema::time_unit::TimeUnit::Milliseconds,
            HudiTimeUnit::Microsecond => daft_schema::time_unit::TimeUnit::Microseconds,
            HudiTimeUnit::Nanosecond => daft_schema::time_unit::TimeUnit::Nanoseconds,
        }
    }

    fn convert_arrow_type(arrow_type: &HudiArrowType) -> DaftResult<DataType> {
        match arrow_type {
            HudiArrowType::Null => Ok(DataType::Null),
            HudiArrowType::Boolean => Ok(DataType::Boolean),
            HudiArrowType::Int8 => Ok(DataType::Int8),
            HudiArrowType::Int16 => Ok(DataType::Int16),
            HudiArrowType::Int32 => Ok(DataType::Int32),
            HudiArrowType::Int64 => Ok(DataType::Int64),
            HudiArrowType::UInt8 => Ok(DataType::UInt8),
            HudiArrowType::UInt16 => Ok(DataType::UInt16),
            HudiArrowType::UInt32 => Ok(DataType::UInt32),
            HudiArrowType::UInt64 => Ok(DataType::UInt64),
            HudiArrowType::Float16 => Err(DaftError::ValueError(
                "Float16 is not supported".to_string(),
            )),
            HudiArrowType::Float32 => Ok(DataType::Float32),
            HudiArrowType::Float64 => Ok(DataType::Float64),
            HudiArrowType::Utf8 | HudiArrowType::LargeUtf8 => Ok(DataType::Utf8),
            HudiArrowType::Binary | HudiArrowType::LargeBinary => Ok(DataType::Binary),
            HudiArrowType::FixedSizeBinary(size) => Ok(DataType::FixedSizeBinary(*size as usize)),
            HudiArrowType::Date32 | HudiArrowType::Date64 => Ok(DataType::Date),
            HudiArrowType::Timestamp(tu, tz) => {
                Ok(DataType::Timestamp(convert_time_unit(tu), tz.clone().map(|s| s.to_string())))
            }
            HudiArrowType::Time32(tu) | HudiArrowType::Time64(tu) => {
                Ok(DataType::Time(convert_time_unit(tu)))
            }
            HudiArrowType::Duration(tu) => Ok(DataType::Duration(convert_time_unit(tu))),
            HudiArrowType::Interval(_) => Ok(DataType::Interval),
            HudiArrowType::Decimal128(precision, scale) => {
                Ok(DataType::Decimal128(*precision as usize, *scale as usize))
            }
            HudiArrowType::Decimal256(_, _) => Err(DaftError::ValueError(
                "Decimal256 is not supported".to_string(),
            )),
            HudiArrowType::List(field) => {
                let inner_type = convert_arrow_type(field.data_type())?;
                Ok(DataType::List(Box::new(inner_type)))
            }
            HudiArrowType::LargeList(field) => {
                let inner_type = convert_arrow_type(field.data_type())?;
                Ok(DataType::List(Box::new(inner_type)))
            }
            HudiArrowType::FixedSizeList(field, size) => {
                let inner_type = convert_arrow_type(field.data_type())?;
                Ok(DataType::FixedSizeList(Box::new(inner_type), *size as usize))
            }
            HudiArrowType::Struct(fields) => {
                let daft_fields: DaftResult<Vec<Field>> = fields
                    .iter()
                    .map(|f| {
                        let dtype = convert_arrow_type(f.data_type())?;
                        Ok(Field::new(f.name(), dtype))
                    })
                    .collect();
                Ok(DataType::Struct(daft_fields?))
            }
            HudiArrowType::Map(field, _) => {
                // Map is stored as List<Struct<key, value>>
                if let HudiArrowType::Struct(fields) = field.data_type() {
                    if fields.len() == 2 {
                        let key_type = convert_arrow_type(fields[0].data_type())?;
                        let value_type = convert_arrow_type(fields[1].data_type())?;
                        return Ok(DataType::Map {
                            key: Box::new(key_type),
                            value: Box::new(value_type),
                        });
                    }
                }
                Err(DaftError::ValueError(format!(
                    "Invalid Map type structure: {:?}",
                    arrow_type
                )))
            }
            HudiArrowType::Utf8View | HudiArrowType::BinaryView => {
                // View types are string/binary views - convert to regular string/binary
                if matches!(arrow_type, HudiArrowType::Utf8View) {
                    Ok(DataType::Utf8)
                } else {
                    Ok(DataType::Binary)
                }
            }
            HudiArrowType::ListView(_) | HudiArrowType::LargeListView(_) => {
                Err(DaftError::ValueError(format!(
                    "ListView types are not supported: {:?}",
                    arrow_type
                )))
            }
            HudiArrowType::Dictionary(_, value_type) => {
                // For dictionary types, use the value type
                convert_arrow_type(value_type.as_ref())
            }
            HudiArrowType::Union(_, _) | HudiArrowType::RunEndEncoded(_, _) => {
                Err(DaftError::ValueError(format!(
                    "Unsupported Arrow type: {:?}",
                    arrow_type
                )))
            }
        }
    }

    let fields: DaftResult<Vec<Field>> = arrow_schema
        .fields()
        .iter()
        .map(|f| {
            let dtype = convert_arrow_type(f.data_type())?;
            Ok(Field::new(f.name(), dtype))
        })
        .collect();

    Ok(Schema::new(fields?))
}

/// Build storage options from StorageConfig for hudi-rs
fn build_storage_options(storage_config: &StorageConfig) -> HashMap<String, String> {
    let mut options = HashMap::new();

    if let Some(ref io_config) = storage_config.io_config {
        // AWS S3 configuration
        let s3_config = &io_config.s3;
        if let Some(region) = &s3_config.region_name {
            options.insert("aws_region".to_string(), region.clone());
        }
        if let Some(key_id) = &s3_config.key_id {
            options.insert("aws_access_key_id".to_string(), key_id.clone());
        }
        if let Some(access_key) = &s3_config.access_key {
            options.insert("aws_secret_access_key".to_string(), access_key.to_string());
        }
        if let Some(session_token) = &s3_config.session_token {
            options.insert("aws_session_token".to_string(), session_token.to_string());
        }
        if let Some(endpoint) = &s3_config.endpoint_url {
            options.insert("aws_endpoint".to_string(), endpoint.clone());
        }
        if s3_config.anonymous {
            options.insert("aws_skip_signature".to_string(), "true".to_string());
        }

        // Azure configuration
        let azure_config = &io_config.azure;
        if let Some(account_name) = &azure_config.storage_account {
            options.insert("azure_storage_account_name".to_string(), account_name.clone());
        }
        if let Some(account_key) = &azure_config.access_key {
            options.insert("azure_storage_account_key".to_string(), account_key.to_string());
        }
        if let Some(sas_token) = &azure_config.sas_token {
            options.insert("azure_storage_sas_token".to_string(), sas_token.to_string());
        }
        if azure_config.anonymous {
            options.insert("azure_skip_signature".to_string(), "true".to_string());
        }

        // GCS configuration
        let gcs_config = &io_config.gcs;
        if let Some(project_id) = &gcs_config.project_id {
            options.insert("google_project_id".to_string(), project_id.clone());
        }
        if let Some(credentials) = &gcs_config.credentials {
            options.insert("google_application_credentials".to_string(), credentials.to_string());
        }
        if gcs_config.anonymous {
            options.insert("google_skip_signature".to_string(), "true".to_string());
        }
    }

    options
}

#[cfg(test)]
mod tests {
    use super::*;
    use hudi_core::arrow::datatypes::DataType as HudiArrowDataType;
    use hudi_core::arrow::datatypes::Field as HudiArrowField;

    #[test]
    fn test_arrow_schema_to_daft_schema() {
        let arrow_schema = ArrowSchema::new(vec![
            HudiArrowField::new("id", HudiArrowDataType::Int64, false),
            HudiArrowField::new("name", HudiArrowDataType::Utf8, true),
            HudiArrowField::new("active", HudiArrowDataType::Boolean, false),
        ]);

        let daft_schema = arrow_schema_to_daft_schema(&arrow_schema).unwrap();

        assert_eq!(daft_schema.names().len(), 3);
        assert_eq!(daft_schema.get_field("id").unwrap().dtype, DataType::Int64);
        assert_eq!(daft_schema.get_field("name").unwrap().dtype, DataType::Utf8);
        assert_eq!(
            daft_schema.get_field("active").unwrap().dtype,
            DataType::Boolean
        );
    }
}
