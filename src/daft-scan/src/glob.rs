use std::{fmt::Display, sync::Arc};

use common_error::DaftResult;
use daft_core::schema::SchemaRef;
use daft_io::{get_io_client, get_runtime, IOStatsContext};

use crate::{DataFileSource, FileType, PartitionField, ScanOperator, ScanOperatorRef, ScanTask};
#[derive(Debug)]
pub struct GlobScanOperator {
    glob_path: String,
    file_type: FileType,
    columns_to_select: Option<Vec<String>>,
    limit: Option<usize>,
    schema: SchemaRef,
    io_config: Arc<daft_io::IOConfig>,
}

fn run_glob(
    glob_path: &str,
    io_config: Arc<daft_io::IOConfig>,
    limit: Option<usize>,
) -> DaftResult<Vec<String>> {
    // Use single-threaded runtime which should be safe here as it is not shared across async contexts
    let runtime = get_runtime(false)?;
    let io_client = get_io_client(false, io_config)?;

    let _rt_guard = runtime.enter();
    runtime.block_on(async {
        Ok(io_client
            .as_ref()
            .glob(glob_path, None, None, limit, None)
            .await?
            .into_iter()
            .map(|fm| fm.filepath)
            .collect())
    })
}

impl GlobScanOperator {
    pub fn _try_new(
        glob_path: &str,
        file_type: FileType,
        io_config: Arc<daft_io::IOConfig>,
    ) -> DaftResult<Self> {
        let paths = run_glob(glob_path, io_config.clone(), Some(1))?;
        let first_filepath = paths[0].as_str();

        let schema = match file_type {
            FileType::Parquet => {
                let io_client = get_io_client(true, io_config.clone())?; // it appears that read_parquet_schema is hardcoded to use multithreaded_io
                let io_stats = IOStatsContext::new(format!(
                    "GlobScanOperator constructor read_parquet_schema: for uri {first_filepath}"
                ));
                daft_parquet::read::read_parquet_schema(
                    first_filepath,
                    io_client,
                    Some(io_stats),
                    Default::default(), // TODO: pass-through schema inference options
                )?
            }
            FileType::Csv => {
                let io_client = get_io_client(true, io_config.clone())?; // it appears that read_parquet_schema is hardcoded to use multithreaded_io
                let io_stats = IOStatsContext::new(format!(
                    "GlobScanOperator constructor read_csv_schema: for uri {first_filepath}"
                ));
                let (schema, _, _, _, _) = daft_csv::metadata::read_csv_schema(
                    first_filepath,
                    true, // TODO: pass-through schema inference options
                    None, // TODO: pass-through schema inference options
                    None, // TODO: pass-through schema inference options
                    io_client,
                    Some(io_stats),
                )?;
                schema
            }
            FileType::Avro => todo!("Schema inference for Avro not implemented"),
            FileType::Orc => todo!("Schema inference for Orc not implemented"),
        };

        Ok(Self {
            glob_path: glob_path.to_string(),
            file_type,
            columns_to_select: None,
            limit: None,
            schema: Arc::new(schema),
            io_config,
        })
    }
}

impl Display for GlobScanOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

impl ScanOperator for GlobScanOperator {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitioning_keys(&self) -> &[PartitionField] {
        &[]
    }

    fn num_partitions(&self) -> common_error::DaftResult<usize> {
        unimplemented!("Cannot get number of partitions -- this will not be implemented.");
    }

    fn select(self: Box<Self>, columns: &[&str]) -> common_error::DaftResult<ScanOperatorRef> {
        for c in columns {
            if self.schema.get_field(c).is_err() {
                return Err(common_error::DaftError::FieldNotFound(format!(
                    "{c} not found in {:?}",
                    self.columns_to_select
                )));
            }
        }
        let mut to_rtn = self;
        to_rtn.columns_to_select = Some(columns.iter().map(|s| s.to_string()).collect());
        Ok(to_rtn)
    }

    fn limit(self: Box<Self>, num: usize) -> DaftResult<ScanOperatorRef> {
        let mut to_rtn = self;
        to_rtn.limit = Some(num);
        Ok(to_rtn)
    }

    fn filter(self: Box<Self>, _predicate: &daft_dsl::Expr) -> DaftResult<(bool, ScanOperatorRef)> {
        Ok((false, self))
    }

    fn to_scan_tasks(
        self: Box<Self>,
    ) -> DaftResult<Box<dyn Iterator<Item = DaftResult<crate::ScanTask>>>> {
        let files = run_glob(self.glob_path.as_str(), self.io_config.clone(), None)?;
        let iter = files.into_iter().map(move |f| {
            let source = DataFileSource::AnonymousDataFile {
                file_type: self.file_type,
                path: f,
                metadata: None,
                partition_spec: None,
                statistics: None,
            };
            Ok(ScanTask {
                source,
                columns: self.columns_to_select.clone(),
                limit: self.limit,
            })
        });
        Ok(Box::new(iter))
    }
}
