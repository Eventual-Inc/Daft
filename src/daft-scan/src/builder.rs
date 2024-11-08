use std::{collections::BTreeMap, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};
use common_io_config::IOConfig;
use common_scan_info::{PhysicalScanInfo, Pushdowns, ScanOperatorRef};
use daft_core::prelude::TimeUnit;
use daft_logical_plan::{
    builder::IntoGlobPath, logical_plan::LogicalPlan, ops, source_info::SourceInfo,
    LogicalPlanBuilder,
};
use daft_schema::{
    field::Field,
    schema::{Schema, SchemaRef},
};
#[cfg(feature = "python")]
use {crate::python::pylib::ScanOperatorHandle, pyo3::prelude::*};

use crate::{
    glob::GlobScanOperator,
    storage_config::{NativeStorageConfig, StorageConfig},
};

pub fn table_scan(
    scan_operator: ScanOperatorRef,
    pushdowns: Option<Pushdowns>,
) -> DaftResult<LogicalPlanBuilder> {
    let schema = scan_operator.0.schema();
    let partitioning_keys = scan_operator.0.partitioning_keys();
    let source_info = SourceInfo::Physical(PhysicalScanInfo::new(
        scan_operator.clone(),
        schema.clone(),
        partitioning_keys.into(),
        pushdowns.clone().unwrap_or_default(),
    ));
    // If file path column is specified, check that it doesn't conflict with any column names in the schema.
    if let Some(file_path_column) = &scan_operator.0.file_path_column() {
        if schema.names().contains(&(*file_path_column).to_string()) {
            return Err(DaftError::ValueError(format!(
                "Attempting to make a Schema with a file path column name that already exists: {}",
                file_path_column
            )));
        }
    }
    // Add generated fields to the schema.
    let schema_with_generated_fields = {
        if let Some(generated_fields) = scan_operator.0.generated_fields() {
            // We use the non-distinct union here because some scan operators have table schema information that
            // already contain partitioned fields. For example,the deltalake scan operator takes the table schema.
            Arc::new(schema.non_distinct_union(&generated_fields))
        } else {
            schema
        }
    };
    // If column selection (projection) pushdown is specified, prune unselected columns from the schema.
    let output_schema = if let Some(Pushdowns {
        columns: Some(columns),
        ..
    }) = &pushdowns
        && columns.len() < schema_with_generated_fields.fields.len()
    {
        let pruned_upstream_schema = schema_with_generated_fields
            .fields
            .iter()
            .filter(|&(name, _)| columns.contains(name))
            .map(|(_, field)| field.clone())
            .collect::<Vec<_>>();
        Arc::new(Schema::new(pruned_upstream_schema)?)
    } else {
        schema_with_generated_fields
    };
    let logical_plan: LogicalPlan = ops::Source::new(output_schema, source_info.into()).into();
    Ok(LogicalPlanBuilder::new(logical_plan.into(), None))
}

pub struct ParquetScanBuilder {
    pub glob_paths: Vec<String>,
    pub infer_schema: bool,
    pub coerce_int96_timestamp_unit: TimeUnit,
    pub field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    pub row_groups: Option<Vec<Option<Vec<i64>>>>,
    pub chunk_size: Option<usize>,
    pub io_config: Option<IOConfig>,
    pub multithreaded: bool,
    pub schema: Option<SchemaRef>,
    pub file_path_column: Option<String>,
    pub hive_partitioning: bool,
}

impl ParquetScanBuilder {
    pub fn new<T: IntoGlobPath>(glob_paths: T) -> Self {
        let glob_paths = glob_paths.into_glob_path();
        Self::new_impl(glob_paths)
    }

    // concrete implementation to reduce LLVM code duplication
    fn new_impl(glob_paths: Vec<String>) -> Self {
        Self {
            glob_paths,
            infer_schema: true,
            coerce_int96_timestamp_unit: TimeUnit::Nanoseconds,
            field_id_mapping: None,
            row_groups: None,
            chunk_size: None,
            multithreaded: true,
            schema: None,
            io_config: None,
            file_path_column: None,
            hive_partitioning: false,
        }
    }
    pub fn infer_schema(mut self, infer_schema: bool) -> Self {
        self.infer_schema = infer_schema;
        self
    }
    pub fn coerce_int96_timestamp_unit(mut self, unit: TimeUnit) -> Self {
        self.coerce_int96_timestamp_unit = unit;
        self
    }
    pub fn field_id_mapping(mut self, field_id_mapping: Arc<BTreeMap<i32, Field>>) -> Self {
        self.field_id_mapping = Some(field_id_mapping);
        self
    }
    pub fn row_groups(mut self, row_groups: Vec<Option<Vec<i64>>>) -> Self {
        self.row_groups = Some(row_groups);
        self
    }
    pub fn chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = Some(chunk_size);
        self
    }

    pub fn io_config(mut self, io_config: IOConfig) -> Self {
        self.io_config = Some(io_config);
        self
    }

    pub fn multithreaded(mut self, multithreaded: bool) -> Self {
        self.multithreaded = multithreaded;
        self
    }

    pub fn schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn file_path_column(mut self, file_path_column: String) -> Self {
        self.file_path_column = Some(file_path_column);
        self
    }

    pub fn hive_partitioning(mut self, hive_partitioning: bool) -> Self {
        self.hive_partitioning = hive_partitioning;
        self
    }

    pub fn finish(self) -> DaftResult<LogicalPlanBuilder> {
        let cfg = ParquetSourceConfig {
            coerce_int96_timestamp_unit: self.coerce_int96_timestamp_unit,
            field_id_mapping: self.field_id_mapping,
            row_groups: self.row_groups,
            chunk_size: self.chunk_size,
        };

        let operator = Arc::new(GlobScanOperator::try_new(
            self.glob_paths,
            Arc::new(FileFormatConfig::Parquet(cfg)),
            Arc::new(StorageConfig::Native(Arc::new(
                NativeStorageConfig::new_internal(self.multithreaded, self.io_config),
            ))),
            self.infer_schema,
            self.schema,
            self.file_path_column,
            self.hive_partitioning,
        )?);

        table_scan(ScanOperatorRef(operator), None)
    }
}

pub fn parquet_scan<T: IntoGlobPath>(glob_path: T) -> ParquetScanBuilder {
    ParquetScanBuilder::new(glob_path)
}

#[cfg(feature = "python")]
pub fn delta_scan<T: AsRef<str>>(
    glob_path: T,
    io_config: Option<IOConfig>,
    multithreaded_io: bool,
) -> DaftResult<LogicalPlanBuilder> {
    use crate::storage_config::{NativeStorageConfig, PyStorageConfig, StorageConfig};

    Python::with_gil(|py| {
        let io_config = io_config.unwrap_or_default();

        let native_storage_config = NativeStorageConfig {
            io_config: Some(io_config),
            multithreaded_io,
        };

        let py_storage_config: PyStorageConfig =
            Arc::new(StorageConfig::Native(Arc::new(native_storage_config))).into();

        // let py_io_config = PyIOConfig { config: io_config };
        let delta_lake_scan = PyModule::import_bound(py, "daft.delta_lake.delta_lake_scan")?;
        let delta_lake_scan_operator =
            delta_lake_scan.getattr(pyo3::intern!(py, "DeltaLakeScanOperator"))?;
        let delta_lake_operator = delta_lake_scan_operator
            .call1((glob_path.as_ref(), py_storage_config))?
            .to_object(py);
        let scan_operator_handle =
            ScanOperatorHandle::from_python_scan_operator(delta_lake_operator, py)?;
        table_scan(scan_operator_handle.into(), None)
    })
}

#[cfg(not(feature = "python"))]
pub fn delta_scan<T: IntoGlobPath>(
    glob_path: T,
    io_config: Option<IOConfig>,
    multithreaded_io: bool,
) -> DaftResult<LogicalPlanBuilder> {
    panic!("Delta Lake scan requires the 'python' feature to be enabled.")
}
