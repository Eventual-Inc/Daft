use std::{
    fmt::{Debug, Display},
    str::FromStr,
};

use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::Field, schema::SchemaRef};
use daft_dsl::Expr;
use daft_stats::{PartitionSpec, TableMetadata, TableStatistics};
use serde::{Deserialize, Serialize};

mod anonymous;
#[cfg(feature = "python")]
pub mod python;

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub enum FileType {
    Parquet,
    Avro,
    Orc,
    Csv,
}

impl FromStr for FileType {
    type Err = DaftError;

    fn from_str(file_type: &str) -> DaftResult<Self> {
        use FileType::*;
        if file_type.trim().eq_ignore_ascii_case("parquet") {
            return Ok(Parquet);
        } else if file_type.trim().eq_ignore_ascii_case("avro") {
            return Ok(Avro);
        } else if file_type.trim().eq_ignore_ascii_case("orc") {
            return Ok(Orc);
        } else if file_type.trim().eq_ignore_ascii_case("csv") {
            return Ok(Csv);
        } else {
            return Err(DaftError::TypeError(format!(
                "FileType {} not supported!",
                file_type
            )));
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum DataFileSource {
    AnonymousDataFile {
        file_type: FileType,
        path: String,
        metadata: Option<TableMetadata>,
        partition_spec: Option<PartitionSpec>,
        statistics: Option<TableStatistics>,
    },
    CatalogDataFile {
        file_type: FileType,
        path: String,
        metadata: TableMetadata,
        partition_spec: PartitionSpec,
        statistics: Option<TableStatistics>,
    },
}

#[derive(Serialize, Deserialize)]
pub struct ScanTask {
    // Micropartition will take this in as an input
    source: DataFileSource,
    columns: Option<Vec<String>>,
    limit: Option<usize>,
}

pub trait ScanOperator: Send + Display {
    fn schema(&self) -> SchemaRef;
    fn partitioning_keys(&self) -> &[Field];
    fn num_partitions(&self) -> DaftResult<usize>;

    // also returns a bool to indicate if the scan operator can "absorb" the predicate
    fn filter(self: Box<Self>, predicate: &Expr) -> DaftResult<(bool, ScanOperatorRef)>;
    fn select(self: Box<Self>, columns: &[&str]) -> DaftResult<ScanOperatorRef>;
    fn limit(self: Box<Self>, num: usize) -> DaftResult<ScanOperatorRef>;
    fn to_scan_tasks(self: Box<Self>)
        -> DaftResult<Box<dyn Iterator<Item = DaftResult<ScanTask>>>>;
}

pub type ScanOperatorRef = Box<dyn ScanOperator>;
