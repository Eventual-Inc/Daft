use std::{collections::BTreeMap, sync::Arc};

use common_error::DaftResult;
use common_file_formats::{CsvSourceConfig, FileFormatConfig, ParquetSourceConfig};
use common_io_config::IOConfig;
use common_scan_info::ScanOperatorRef;
use daft_core::prelude::TimeUnit;
use daft_logical_plan::{builder::IntoGlobPath, LogicalPlanBuilder};
use daft_schema::{field::Field, schema::SchemaRef};
#[cfg(feature = "python")]
use {crate::python::pylib::ScanOperatorHandle, pyo3::prelude::*};

use crate::{
    glob::GlobScanOperator,
    storage_config::{NativeStorageConfig, StorageConfig},
};

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

        LogicalPlanBuilder::table_scan(ScanOperatorRef(operator), None)
    }
}

pub fn parquet_scan<T: IntoGlobPath>(glob_path: T) -> ParquetScanBuilder {
    ParquetScanBuilder::new(glob_path)
}

pub struct CsvScanBuilder {
    pub glob_paths: Vec<String>,
    pub infer_schema: bool,
    pub io_config: Option<IOConfig>,
    pub schema: Option<SchemaRef>,
    pub file_path_column: Option<String>,
    pub hive_partitioning: bool,
    pub delimiter: Option<char>,
    pub has_headers: bool,
    pub double_quote: bool,
    pub quote: Option<char>,
    pub escape_char: Option<char>,
    pub comment: Option<char>,
    pub allow_variable_columns: bool,
    pub buffer_size: Option<usize>,
    pub chunk_size: Option<usize>,
    pub use_native_downloader: bool,
    pub schema_hints: Option<SchemaRef>,
}

impl CsvScanBuilder {
    pub fn new<T: IntoGlobPath>(glob_paths: T) -> Self {
        let glob_paths = glob_paths.into_glob_path();
        Self::new_impl(glob_paths)
    }

    // concrete implementation to reduce LLVM code duplication
    fn new_impl(glob_paths: Vec<String>) -> Self {
        Self {
            glob_paths,
            infer_schema: true,
            schema: None,
            io_config: None,
            file_path_column: None,
            hive_partitioning: false,
            delimiter: None,
            has_headers: true,
            double_quote: true,
            quote: None,
            escape_char: None,
            comment: None,
            allow_variable_columns: false,
            buffer_size: None,
            chunk_size: None,
            use_native_downloader: true,
            schema_hints: None,
        }
    }
    pub fn infer_schema(mut self, infer_schema: bool) -> Self {
        self.infer_schema = infer_schema;
        self
    }
    pub fn io_config(mut self, io_config: IOConfig) -> Self {
        self.io_config = Some(io_config);
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
    pub fn delimiter(mut self, delimiter: char) -> Self {
        self.delimiter = Some(delimiter);
        self
    }
    pub fn has_headers(mut self, has_headers: bool) -> Self {
        self.has_headers = has_headers;
        self
    }
    pub fn double_quote(mut self, double_quote: bool) -> Self {
        self.double_quote = double_quote;
        self
    }
    pub fn quote(mut self, quote: char) -> Self {
        self.quote = Some(quote);
        self
    }
    pub fn escape_char(mut self, escape_char: char) -> Self {
        self.escape_char = Some(escape_char);
        self
    }
    pub fn comment(mut self, comment: char) -> Self {
        self.comment = Some(comment);
        self
    }
    pub fn allow_variable_columns(mut self, allow_variable_columns: bool) -> Self {
        self.allow_variable_columns = allow_variable_columns;
        self
    }
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = Some(buffer_size);
        self
    }
    pub fn chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = Some(chunk_size);
        self
    }
    pub fn schema_hints(mut self, schema_hints: SchemaRef) -> Self {
        self.schema_hints = Some(schema_hints);
        self
    }
    pub fn use_native_downloader(mut self, use_native_downloader: bool) -> Self {
        self.use_native_downloader = use_native_downloader;
        self
    }

    pub fn finish(self) -> DaftResult<LogicalPlanBuilder> {
        let cfg = CsvSourceConfig {
            delimiter: self.delimiter,
            has_headers: self.has_headers,
            double_quote: self.double_quote,
            quote: self.quote,
            escape_char: self.escape_char,
            comment: self.comment,
            allow_variable_columns: self.allow_variable_columns,
            buffer_size: self.buffer_size,
            chunk_size: self.chunk_size,
        };

        let operator = Arc::new(GlobScanOperator::try_new(
            self.glob_paths,
            Arc::new(FileFormatConfig::Csv(cfg)),
            Arc::new(StorageConfig::Native(Arc::new(
                NativeStorageConfig::new_internal(false, self.io_config),
            ))),
            self.infer_schema,
            self.schema,
            self.file_path_column,
            self.hive_partitioning,
        )?);

        LogicalPlanBuilder::table_scan(ScanOperatorRef(operator), None)
    }
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
        LogicalPlanBuilder::table_scan(scan_operator_handle.into(), None)
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
