//! Daft integration with Apache Hudi via hudi-rs.
//!
//! This crate provides a native Rust implementation of the `ScanOperator` trait
//! for reading Apache Hudi tables.
//!
//! # Current Limitations
//!
//! ## Table Type Support
//! Only **Copy-on-Write (COW)** tables are fully supported. Merge-on-Read (MOR) tables
//! will be read in "read-optimized" mode, meaning only base Parquet files are read and
//! delta log files are ignored. This may result in stale or incomplete data for MOR tables
//! that have pending compactions.
//!
//! ## Column Statistics
//! Per-file column-level min/max statistics are not currently extracted. The hudi-core
//! `FileMetadata` struct needs to be extended to include column statistics from Parquet
//! file metadata to enable effective predicate pushdown based on column stats.

use std::{collections::HashMap, sync::Arc};

// Use arrow types from hudi_core to ensure type compatibility with the schema returned by HudiTable
use common_error::{DaftError, DaftResult};
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};
use common_scan_info::{PartitionField, Pushdowns, ScanOperator, ScanTaskLike, ScanTaskLikeRef};
use daft_core::{lit::Literal, prelude::Utf8Array, series::IntoSeries};
use daft_dsl::{Expr, ExprRef, Operator};
use daft_recordbatch::RecordBatch;
use daft_scan::{DataSource, ScanTask, storage_config::StorageConfig};
use daft_schema::{
    dtype::DataType,
    field::Field,
    schema::{Schema, SchemaRef},
};
use daft_stats::{PartitionSpec, TableMetadata};
use hudi_core::{
    arrow::datatypes::Schema as ArrowSchema, config::util::empty_options, table::Table as HudiTable,
};
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

/// Convert Daft partition filter expressions to hudi-core filter format.
///
/// hudi-core's `get_file_slices()` accepts filters as `(field_name, operator, value)` string tuples.
/// This function extracts simple comparison expressions from Daft's `ExprRef` and converts them.
///
/// Only simple comparisons of the form `Column op Literal` are supported.
/// Complex expressions (functions, nested comparisons, etc.) are ignored.
fn convert_partition_filters(expr: &ExprRef) -> Vec<(String, String, String)> {
    let mut filters = Vec::new();
    collect_filters(expr, &mut filters);
    filters
}

/// Recursively collect simple comparison filters from an expression tree.
fn collect_filters(expr: &ExprRef, filters: &mut Vec<(String, String, String)>) {
    match expr.as_ref() {
        // Handle AND expressions - collect filters from both sides
        Expr::BinaryOp {
            op: Operator::And,
            left,
            right,
        } => {
            collect_filters(left, filters);
            collect_filters(right, filters);
        }
        // Handle comparison expressions: Column op Literal or Literal op Column
        Expr::BinaryOp { op, left, right } => {
            if let Some(filter) = try_extract_comparison(op, left, right) {
                filters.push(filter);
            }
        }
        // Ignore other expression types (functions, NOT, IS NULL, etc.)
        _ => {}
    }
}

/// Try to extract a simple comparison filter from a binary operation.
/// Returns Some((field_name, operator_str, value_str)) if successful.
fn try_extract_comparison(
    op: &Operator,
    left: &ExprRef,
    right: &ExprRef,
) -> Option<(String, String, String)> {
    // Convert operator to hudi-core string format
    let op_str = match op {
        Operator::Eq => "=",
        Operator::NotEq => "!=",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        // Other operators (arithmetic, bitwise, etc.) are not supported
        _ => return None,
    };

    // Try Column op Literal
    if let (Some(col_name), Some(lit_value)) = (try_get_column_name(left), try_get_literal(right)) {
        return Some((col_name, op_str.to_string(), lit_value));
    }

    // Try Literal op Column (flip the operator)
    if let (Some(lit_value), Some(col_name)) = (try_get_literal(left), try_get_column_name(right)) {
        // Flip comparison operators when literal is on the left
        let flipped_op = match op {
            Operator::Lt => ">",
            Operator::LtEq => ">=",
            Operator::Gt => "<",
            Operator::GtEq => "<=",
            _ => op_str, // Eq and NotEq are symmetric
        };
        return Some((col_name, flipped_op.to_string(), lit_value));
    }

    None
}

/// Try to extract a column name from an expression.
fn try_get_column_name(expr: &ExprRef) -> Option<String> {
    match expr.as_ref() {
        Expr::Column(col) =>
        {
            #[allow(deprecated)]
            Some(col.name())
        }
        // Handle aliased columns
        Expr::Alias(inner, _) => try_get_column_name(inner),
        _ => None,
    }
}

/// Try to convert a literal expression to a string value for hudi-core filters.
fn try_get_literal(expr: &ExprRef) -> Option<String> {
    match expr.as_ref() {
        Expr::Literal(lit) => literal_to_string(lit),
        _ => None,
    }
}

/// Convert a Daft Literal to a string representation for hudi-core filters.
fn literal_to_string(lit: &Literal) -> Option<String> {
    match lit {
        Literal::Null => None, // NULL comparisons don't work in hudi-core filters
        Literal::Boolean(b) => Some(b.to_string()),
        Literal::Utf8(s) => Some(s.clone()),
        Literal::Int8(v) => Some(v.to_string()),
        Literal::UInt8(v) => Some(v.to_string()),
        Literal::Int16(v) => Some(v.to_string()),
        Literal::UInt16(v) => Some(v.to_string()),
        Literal::Int32(v) => Some(v.to_string()),
        Literal::UInt32(v) => Some(v.to_string()),
        Literal::Int64(v) => Some(v.to_string()),
        Literal::UInt64(v) => Some(v.to_string()),
        Literal::Float32(v) => Some(v.to_string()),
        Literal::Float64(v) => Some(v.to_string()),
        Literal::Date(days) => Some(days.to_string()),
        Literal::Timestamp(ts, _, _) => Some(ts.to_string()),
        Literal::Decimal(v, _, _) => Some(v.to_string()),
        // Complex types (List, Struct, Binary, etc.) are not supported
        _ => None,
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
    pub async fn try_new(table_uri: &str, storage_config: Arc<StorageConfig>) -> DaftResult<Self> {
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
                    // PartitionField::new with (None, None) for source_field and transform
                    // cannot fail per the implementation
                    PartitionField::new(field.clone(), None, None)
                        .expect("PartitionField::new with no transform should never fail")
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
    fn name(&self) -> &'static str {
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
        let runtime = common_runtime::get_io_runtime(self.storage_config.multithreaded_io);

        // Clone necessary data for the async block
        let table = self.table.clone();
        let schema = self.schema.clone();
        let storage_config = self.storage_config.clone();
        let base_url = self.base_url();
        let partition_keys = self.partition_keys.clone();

        // Execute async file slice fetching
        runtime.block_on_current_thread(async move {
            // Get file slices from the Hudi table.

            // Convert partition filters to hudi-core format for pushdown.
            // Only simple comparisons (Column op Literal) are supported.
            // Complex expressions and unsupported operators are silently ignored.
            let partition_filters: Vec<(String, String, String)> = pushdowns
                .partition_filters
                .as_ref()
                .map(convert_partition_filters)
                .unwrap_or_default();

            // Pass filters to get_file_slices() for partition pruning
            let file_slices = table
                .get_file_slices(
                    partition_filters
                        .iter()
                        .map(|(f, o, v)| (f.as_str(), o.as_str(), v.as_str())),
                )
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
                if limit_files && rows_left == 0 {
                    break;
                }

                // Construct the full file path
                // LIMITATION: MOR table support are read-optimized only.
                // We only read base Parquet files here. For MOR tables, this means Hudi log
                // files are ignored, resulting in "read-optimized" semantics. To properly
                // support MOR, we would need to use hudi-core's `FileGroupReader` to perform
                // merging at execution time.
                let relative_path =
                    file_slice
                        .base_file_relative_path()
                        .map_err(|e| Error::Hudi {
                            message: format!("Failed to get base file path: {}", e),
                        })?;

                let file_path =
                    hudi_core::storage::util::join_url_segments(&base_url, &[&relative_path])
                        .map_err(|e| Error::Hudi {
                            message: format!("Failed to construct file URL: {}", e),
                        })?
                        .to_string();

                // Get file metadata if available, with safe numeric conversions
                let (num_rows, size_bytes) =
                    if let Some(ref metadata) = file_slice.base_file.file_metadata {
                        // Safely convert byte_size (i64) to u64, treating negative/zero as None
                        let size = u64::try_from(metadata.byte_size).ok().filter(|&s| s > 0);
                        // Safely convert num_records (i64) to usize, treating negative as None
                        let rows = usize::try_from(metadata.num_records).ok();
                        (rows, size)
                    } else {
                        (None, None)
                    };

                // Create partition values from partition path.
                // Partition pruning is already handled by get_file_slices() above.
                // This is to for additional runtime pruning effect.
                let partition_spec =
                    if !file_slice.partition_path.is_empty() && !partition_keys.is_empty() {
                        let partition_path_series = Utf8Array::from_iter(
                            "_hoodie_partition_path",
                            std::iter::once(Some(file_slice.partition_path.as_str())),
                        )
                        .into_series();

                        let partition_rb =
                            RecordBatch::from_nonempty_columns(vec![partition_path_series])?;
                        Some(PartitionSpec { keys: partition_rb })
                    } else {
                        None
                    };

                // Create table metadata
                let metadata = num_rows.map(|length| TableMetadata { length });

                // Create the data source.
                //
                // LIMITATION: Column statistics
                // Per-column min/max statistics are not currently extracted from Parquet metadata.
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
    use hudi_core::arrow::datatypes::{DataType as HudiArrowType, TimeUnit as HudiTimeUnit};

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
            HudiArrowType::Timestamp(tu, tz) => Ok(DataType::Timestamp(
                convert_time_unit(tu),
                tz.clone().map(|s| s.to_string()),
            )),
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
                Ok(DataType::FixedSizeList(
                    Box::new(inner_type),
                    *size as usize,
                ))
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
                if let HudiArrowType::Struct(fields) = field.data_type()
                    && fields.len() == 2
                {
                    let key_type = convert_arrow_type(fields[0].data_type())?;
                    let value_type = convert_arrow_type(fields[1].data_type())?;
                    return Ok(DataType::Map {
                        key: Box::new(key_type),
                        value: Box::new(value_type),
                    });
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
            HudiArrowType::Union(_, _) | HudiArrowType::RunEndEncoded(_, _) => Err(
                DaftError::ValueError(format!("Unsupported Arrow type: {:?}", arrow_type)),
            ),
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
            options.insert(
                "azure_storage_account_name".to_string(),
                account_name.clone(),
            );
        }
        if let Some(account_key) = &azure_config.access_key {
            options.insert(
                "azure_storage_account_key".to_string(),
                account_key.to_string(),
            );
        }
        if let Some(sas_token) = &azure_config.sas_token {
            options.insert("azure_storage_sas_token".to_string(), sas_token.clone());
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
            options.insert(
                "google_application_credentials".to_string(),
                credentials.to_string(),
            );
        }
        if gcs_config.anonymous {
            options.insert("google_skip_signature".to_string(), "true".to_string());
        }
    }

    options
}

#[cfg(test)]
mod tests {
    use hudi_core::arrow::datatypes::{DataType as HudiArrowDataType, Field as HudiArrowField};

    use super::*;

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
