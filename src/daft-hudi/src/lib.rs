use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields, Schema as ArrowSchema,
};
use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::Utf8Array,
    series::{IntoSeries, Series},
};
use daft_recordbatch::RecordBatch;
use daft_scan::{
    FileFormatConfig, ParquetSourceConfig, PartitionField, Precision, Pushdowns, ScanOperator,
    ScanSource, ScanSourceKind, ScanTask, ScanTaskRef, SourceConfig, Statistics,
    storage_config::StorageConfig,
};
use daft_schema::{
    field::Field,
    schema::{Schema, SchemaRef},
};
use daft_stats::{ColumnRangeStatistics, PartitionSpec, TableStatistics};
use hudi_core::{
    config::{ReadOptions, read::HudiReadConfig, table::HudiTableConfig},
    statistics::StatisticsContainer,
    table::Table as HudiTable,
};
use snafu::Snafu;
use url::Url;

#[cfg(feature = "python")]
pub mod python;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Hudi error: {message}"))]
    Hudi { message: String },

    #[snafu(display("Schema conversion error: {message}"))]
    SchemaConversion { message: String },

    #[snafu(display("Invalid table URI: {uri}"))]
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

fn convert_partition_filters(expr: &daft_dsl::ExprRef) -> Vec<(String, String, String)> {
    let mut filters = Vec::new();
    collect_filters(expr, &mut filters);
    filters
}

fn collect_filters(expr: &daft_dsl::ExprRef, filters: &mut Vec<(String, String, String)>) {
    use daft_core::prelude::Operator;
    use daft_dsl::Expr;
    match expr.as_ref() {
        Expr::BinaryOp {
            op: Operator::And,
            left,
            right,
        } => {
            collect_filters(left, filters);
            collect_filters(right, filters);
        }
        Expr::BinaryOp { op, left, right } => {
            if let Some(filter) = try_extract_comparison(op, left, right) {
                filters.push(filter);
            }
        }
        _ => {}
    }
}

fn try_extract_comparison(
    op: &daft_core::prelude::Operator,
    left: &daft_dsl::ExprRef,
    right: &daft_dsl::ExprRef,
) -> Option<(String, String, String)> {
    use daft_core::prelude::Operator;

    let op_str = match op {
        Operator::Eq => "=",
        Operator::NotEq => "!=",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        _ => return None,
    };

    if let (Some(col_name), Some(lit_value)) = (try_get_column_name(left), try_get_literal(right)) {
        return Some((col_name, op_str.to_string(), lit_value));
    }

    if let (Some(lit_value), Some(col_name)) = (try_get_literal(left), try_get_column_name(right)) {
        let flipped_op = match op {
            Operator::Lt => ">",
            Operator::LtEq => ">=",
            Operator::Gt => "<",
            Operator::GtEq => "<=",
            _ => op_str,
        };
        return Some((col_name, flipped_op.to_string(), lit_value));
    }

    None
}

fn try_get_column_name(expr: &daft_dsl::ExprRef) -> Option<String> {
    use daft_dsl::Expr;
    match expr.as_ref() {
        Expr::Column(col) =>
        {
            #[allow(deprecated)]
            Some(col.name())
        }
        Expr::Alias(inner, _) => try_get_column_name(inner),
        _ => None,
    }
}

fn try_get_literal(expr: &daft_dsl::ExprRef) -> Option<String> {
    use daft_dsl::Expr;
    match expr.as_ref() {
        Expr::Literal(lit) => literal_to_string(lit),
        _ => None,
    }
}

fn literal_to_string(lit: &daft_core::lit::Literal) -> Option<String> {
    use daft_core::lit::Literal;
    match lit {
        Literal::Null => None,
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
        _ => None,
    }
}

#[derive(Debug)]
pub struct HudiScanOperator {
    table: HudiTable,
    schema: SchemaRef,
    storage_config: Arc<StorageConfig>,
    partition_keys: Vec<PartitionField>,
    is_hive_style: bool,
}

impl HudiScanOperator {
    pub async fn try_new(table_uri: &str, storage_config: Arc<StorageConfig>) -> DaftResult<Self> {
        Self::try_new_with_options(
            table_uri,
            storage_config,
            std::iter::empty::<(&str, &str)>(),
        )
        .await
    }

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
        let storage_options = build_storage_options(&storage_config);

        let all_options: Vec<(String, String)> = storage_options
            .into_iter()
            .chain(
                options
                    .into_iter()
                    .map(|(k, v)| (k.as_ref().to_string(), v.into())),
            )
            .collect();

        let table = HudiTable::new_with_options(table_uri, all_options)
            .await
            .map_err(|e| Error::Hudi {
                message: e.to_string(),
            })?;

        let arrow_schema = table
            .get_schema_with_meta_fields()
            .await
            .map_err(|e| Error::Hudi {
                message: format!("Failed to get table schema: {e}"),
            })?;
        let schema = arrow_schema_to_daft_schema(&arrow_schema)?;

        let partition_schema = table
            .get_partition_schema()
            .await
            .map_err(|e| Error::Hudi {
                message: format!("Failed to get partition schema: {e}"),
            })?;
        let partition_keys = partition_schema
            .fields()
            .iter()
            .filter_map(|af| {
                Field::try_from(af.as_ref()).ok().map(|field| {
                    PartitionField::new(field, None, None)
                        .expect("PartitionField::new with no transform should never fail")
                })
            })
            .collect();

        let is_hive_style: bool = table
            .hudi_options()
            .get(HudiTableConfig::IsHiveStylePartitioning.as_ref())
            .and_then(|v| v.parse().ok())
            .unwrap_or(false);

        Ok(Self {
            table,
            schema: Arc::new(schema),
            storage_config,
            partition_keys,
            is_hive_style,
        })
    }

    pub fn base_url(&self) -> Url {
        self.table.base_url()
    }

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

    fn statistics(&self) -> Option<Statistics> {
        let runtime = common_runtime::get_io_runtime(self.storage_config.multithreaded_io);
        let table = self.table.clone();
        runtime
            .block_on_current_thread(async move { table.compute_table_stats(None).await })
            .map(|(num_records, total_bytes)| Statistics {
                num_rows: Precision::Inexact(num_records),
                size_bytes: Precision::Inexact(total_bytes),
                column_statistics: Vec::new(),
            })
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            format!("HudiScanOperator({})", self.table_name()),
            format!("Table type = {}", self.table.table_type()),
            format!("Schema = {}", self.schema),
            format!("Partitioning keys = {:?}", self.partitioning_keys()),
        ]
    }

    fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskRef>> {
        let runtime = common_runtime::get_io_runtime(self.storage_config.multithreaded_io);

        let table = self.table.clone();
        let schema = self.schema.clone();
        let storage_config = self.storage_config.clone();
        let base_url = self.base_url();
        let partition_keys = self.partition_keys.clone();
        let is_hive_style = self.is_hive_style;

        runtime.block_on_current_thread(async move {
            let partition_filters: Vec<(String, String, String)> = pushdowns
                .partition_filters
                .as_ref()
                .map(convert_partition_filters)
                .unwrap_or_default();

            let mut read_options = ReadOptions::new()
                .with_hudi_option(HudiReadConfig::UseReadOptimizedMode.as_ref(), "true");
            if !partition_filters.is_empty() {
                read_options = read_options
                    .with_filters(
                        partition_filters
                            .iter()
                            .map(|(f, o, v)| (f.as_str(), o.as_str(), v.as_str())),
                    )
                    .map_err(|e| Error::Hudi {
                        message: format!("Failed to build read options with filters: {e}"),
                    })?;
            }
            if let Some(ref columns) = pushdowns.columns {
                read_options = read_options.with_projection(columns.iter().map(|c| c.as_str()));
            }

            let file_slices =
                table
                    .get_file_slices(&read_options)
                    .await
                    .map_err(|e| Error::Hudi {
                        message: format!("Failed to get file slices: {e}"),
                    })?;

            if file_slices.is_empty() {
                return Ok(Vec::new());
            }

            let limit_files = pushdowns.limit.is_some()
                && pushdowns.filters.is_none()
                && pushdowns.partition_filters.is_none();
            let mut rows_left = pushdowns.limit.unwrap_or(0);

            let source_config = Arc::new(SourceConfig::File(FileFormatConfig::Parquet(
                ParquetSourceConfig {
                    coerce_int96_timestamp_unit: daft_schema::time_unit::TimeUnit::Microseconds,
                    field_id_mapping: None,
                    row_groups: None,
                    chunk_size: None,
                },
            )));

            let partition_fields: Vec<Field> =
                partition_keys.iter().map(|pk| pk.clone_field()).collect();
            let partition_schema = Schema::new(partition_fields);

            let mut scan_tasks: Vec<ScanTaskRef> = Vec::with_capacity(file_slices.len());

            for file_slice in file_slices {
                if limit_files && rows_left == 0 {
                    break;
                }

                let relative_path =
                    file_slice
                        .base_file_relative_path()
                        .map_err(|e| Error::Hudi {
                            message: format!("Failed to get base file path: {e}"),
                        })?;

                let file_path =
                    hudi_core::storage::util::join_url_segments(&base_url, &[&relative_path])
                        .map_err(|e| Error::Hudi {
                            message: format!("Failed to construct file URL: {e}"),
                        })?
                        .to_string();

                let size_bytes = {
                    let total = file_slice.total_size_bytes();
                    if total > 0 { Some(total) } else { None }
                };

                let num_rows = file_slice
                    .base_file
                    .file_metadata
                    .as_ref()
                    .and_then(|m| usize::try_from(m.num_records).ok())
                    .filter(|&r| r > 0);

                let metadata = num_rows.map(|length| daft_stats::TableMetadata { length });

                let partition_spec = parse_partition_spec(
                    &file_slice.partition_path,
                    &partition_schema,
                    is_hive_style,
                );

                let statistics = file_slice
                    .base_file_column_stats
                    .as_ref()
                    .and_then(|cs| convert_column_stats(cs, &schema));

                let scan_source = ScanSource {
                    size_bytes,
                    metadata,
                    statistics,
                    partition_spec,
                    kind: ScanSourceKind::File {
                        path: file_path,
                        chunk_spec: None,
                        iceberg_delete_files: None,
                        parquet_metadata: None,
                    },
                };

                let scan_task = ScanTask::new(
                    vec![scan_source],
                    source_config.clone(),
                    schema.clone(),
                    storage_config.clone(),
                    pushdowns.clone(),
                    None,
                );

                if let Some(num) = num_rows {
                    rows_left = rows_left.saturating_sub(num);
                }

                scan_tasks.push(Arc::new(scan_task));
            }

            Ok(scan_tasks)
        })
    }
}

const HOODIE_PARTITION_PATH_FIELD: &str = "_hoodie_partition_path";

fn parse_partition_spec(
    partition_path: &str,
    partition_schema: &Schema,
    is_hive_style: bool,
) -> Option<PartitionSpec> {
    if partition_path.is_empty() || partition_schema.is_empty() {
        return None;
    }

    let fields = partition_schema.fields();

    // For timestamp-based keygen, hudi-rs returns a single `_hoodie_partition_path`
    // field. The raw partition path (e.g. "2023/04/01/12") must be kept as-is.
    if fields.len() == 1 && fields[0].name.as_ref() == HOODIE_PARTITION_PATH_FIELD {
        let series = Utf8Array::from_iter(&fields[0].name, std::iter::once(Some(partition_path)))
            .into_series();
        return RecordBatch::from_nonempty_columns(vec![series])
            .ok()
            .map(|keys| PartitionSpec { keys });
    }

    let segments: Vec<&str> = partition_path.split('/').collect();

    let partition_values: Vec<_> = if is_hive_style {
        segments
            .iter()
            .filter_map(|segment| {
                let (key, value) = segment.split_once('=')?;
                let field = partition_schema.get_field(key).ok()?;
                Some(Utf8Array::from_iter(&field.name, std::iter::once(Some(value))).into_series())
            })
            .collect()
    } else {
        if segments.len() != fields.len() {
            return None;
        }
        segments
            .iter()
            .zip(fields)
            .map(|(value, field)| {
                Utf8Array::from_iter(&field.name, std::iter::once(Some(*value))).into_series()
            })
            .collect()
    };

    if partition_values.is_empty() {
        return None;
    }

    RecordBatch::from_nonempty_columns(partition_values)
        .ok()
        .map(|keys| PartitionSpec { keys })
}

fn convert_column_stats(
    stats_container: &StatisticsContainer,
    schema: &Schema,
) -> Option<TableStatistics> {
    let mut col_stats = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let col_stat = stats_container
            .columns
            .get(field.name.as_ref())
            .and_then(|cs| {
                let min_series = cs
                    .min_value
                    .as_ref()
                    .and_then(|arr| Series::from_arrow(field.clone(), arr.clone()).ok());
                let max_series = cs
                    .max_value
                    .as_ref()
                    .and_then(|arr| Series::from_arrow(field.clone(), arr.clone()).ok());
                ColumnRangeStatistics::new(min_series, max_series).ok()
            })
            .unwrap_or(ColumnRangeStatistics::Missing);
        col_stats.push(col_stat);
    }
    Some(TableStatistics::new(col_stats, Arc::new(schema.clone())))
}

fn arrow_schema_to_daft_schema(arrow_schema: &ArrowSchema) -> DaftResult<Schema> {
    let corrected = fix_hudi_arrow_schema(arrow_schema);
    let fields: DaftResult<Vec<Field>> = corrected
        .fields()
        .iter()
        .map(|f| Field::try_from(f.as_ref()))
        .collect();
    Ok(Schema::new(fields?))
}

/// hudi-rs converts Avro Map types to Arrow `Dictionary(Utf8, V)`.
/// Daft expects Arrow `Map(Struct(key, value), false)`. This function
/// rewrites Dictionary fields into the Map representation Daft understands.
fn fix_hudi_arrow_schema(schema: &ArrowSchema) -> ArrowSchema {
    let fields: Vec<ArrowField> = schema
        .fields()
        .iter()
        .map(|f| fix_hudi_arrow_field(f))
        .collect();
    ArrowSchema::new(fields)
}

fn fix_hudi_arrow_field(field: &ArrowField) -> ArrowField {
    let new_dt = fix_hudi_arrow_dtype(field.data_type());
    ArrowField::new(field.name(), new_dt, field.is_nullable())
}

fn fix_hudi_arrow_dtype(dt: &ArrowDataType) -> ArrowDataType {
    match dt {
        ArrowDataType::Dictionary(key_type, value_type)
            if matches!(key_type.as_ref(), ArrowDataType::Utf8) =>
        {
            let value_dt = fix_hudi_arrow_dtype(value_type);
            let entries = ArrowField::new(
                "entries",
                ArrowDataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("key", ArrowDataType::Utf8, false),
                    ArrowField::new("value", value_dt, true),
                ])),
                false,
            );
            ArrowDataType::Map(Arc::new(entries), false)
        }
        ArrowDataType::List(inner) => ArrowDataType::List(Arc::new(fix_hudi_arrow_field(inner))),
        ArrowDataType::LargeList(inner) => {
            ArrowDataType::LargeList(Arc::new(fix_hudi_arrow_field(inner)))
        }
        ArrowDataType::FixedSizeList(inner, size) => {
            ArrowDataType::FixedSizeList(Arc::new(fix_hudi_arrow_field(inner)), *size)
        }
        ArrowDataType::Struct(fields) => {
            let new_fields: Vec<ArrowField> =
                fields.iter().map(|f| fix_hudi_arrow_field(f)).collect();
            ArrowDataType::Struct(ArrowFields::from(new_fields))
        }
        ArrowDataType::Map(entries_field, sorted) => {
            ArrowDataType::Map(Arc::new(fix_hudi_arrow_field(entries_field)), *sorted)
        }
        other => other.clone(),
    }
}

fn build_storage_options(storage_config: &StorageConfig) -> HashMap<String, String> {
    let mut options = HashMap::new();

    if let Some(ref io_config) = storage_config.io_config {
        let s3_config = &io_config.s3;

        if let Some(ref provider) = s3_config.credentials_provider
            && let Ok(creds) = provider.get_cached_credentials()
        {
            options.insert("aws_access_key_id".to_string(), creds.key_id.clone());
            options.insert(
                "aws_secret_access_key".to_string(),
                creds.access_key.clone(),
            );
            if let Some(ref token) = creds.session_token {
                options.insert("aws_session_token".to_string(), token.clone());
            }
        }

        if let Some(region) = &s3_config.region_name {
            options.insert("aws_region".to_string(), region.clone());
        }
        if !options.contains_key("aws_access_key_id")
            && let Some(key_id) = &s3_config.key_id
        {
            options.insert("aws_access_key_id".to_string(), key_id.clone());
        }
        if !options.contains_key("aws_secret_access_key")
            && let Some(access_key) = &s3_config.access_key
        {
            options.insert("aws_secret_access_key".to_string(), access_key.to_string());
        }
        if !options.contains_key("aws_session_token")
            && let Some(session_token) = &s3_config.session_token
        {
            options.insert("aws_session_token".to_string(), session_token.to_string());
        }
        if let Some(endpoint) = &s3_config.endpoint_url {
            options.insert("aws_endpoint".to_string(), endpoint.clone());
        }
        if s3_config.anonymous {
            options.insert("aws_skip_signature".to_string(), "true".to_string());
        }

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
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields};
    use daft_schema::dtype::DataType;

    use super::*;

    #[test]
    fn test_arrow_schema_to_daft_schema() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("name", ArrowDataType::LargeUtf8, true),
            ArrowField::new("active", ArrowDataType::Boolean, false),
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

    #[test]
    fn test_dictionary_to_map_conversion() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new(
                "map_field",
                ArrowDataType::Dictionary(
                    Box::new(ArrowDataType::Utf8),
                    Box::new(ArrowDataType::Struct(ArrowFields::from(vec![
                        ArrowField::new("f1", ArrowDataType::Float64, true),
                        ArrowField::new("f2", ArrowDataType::Boolean, true),
                    ]))),
                ),
                true,
            ),
        ]);

        let daft_schema = arrow_schema_to_daft_schema(&arrow_schema).unwrap();

        assert_eq!(daft_schema.names().len(), 2);
        let map_dtype = &daft_schema.get_field("map_field").unwrap().dtype;
        assert!(matches!(map_dtype, DataType::Map { .. }));
        assert_eq!(*map_dtype.key_type().unwrap(), DataType::Utf8);
        assert!(matches!(
            map_dtype.value_type().unwrap(),
            DataType::Struct(..)
        ));
    }

    #[test]
    fn test_nested_dictionary_to_map_conversion() {
        let arrow_schema = ArrowSchema::new(vec![ArrowField::new(
            "list_of_maps",
            ArrowDataType::List(Arc::new(ArrowField::new(
                "element",
                ArrowDataType::Dictionary(
                    Box::new(ArrowDataType::Utf8),
                    Box::new(ArrowDataType::Int32),
                ),
                true,
            ))),
            true,
        )]);

        let daft_schema = arrow_schema_to_daft_schema(&arrow_schema).unwrap();

        let list_dtype = &daft_schema.get_field("list_of_maps").unwrap().dtype;
        assert!(matches!(list_dtype, DataType::List(..)));
        if let DataType::List(inner) = list_dtype {
            assert!(matches!(inner.as_ref(), DataType::Map { .. }));
        }
    }

    #[test]
    fn test_partition_spec_hoodie_partition_path() {
        let schema = Schema::new(vec![Field::new(
            HOODIE_PARTITION_PATH_FIELD,
            DataType::Utf8,
        )]);
        let spec = parse_partition_spec("2023/04/01/12", &schema, false);
        assert!(spec.is_some());
        let spec = spec.unwrap();
        let col = spec.keys.get_column(0);
        assert_eq!(col.name(), HOODIE_PARTITION_PATH_FIELD);
        assert_eq!(col.utf8().unwrap().get(0).unwrap(), "2023/04/01/12");
    }

    #[test]
    fn test_partition_spec_segment_count_mismatch_returns_none() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8),
            Field::new("b", DataType::Utf8),
        ]);
        let spec = parse_partition_spec("only_one_segment", &schema, false);
        assert!(spec.is_none());
    }
}
