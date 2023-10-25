use common_error::DaftResult;
use daft_core::{datatypes::Field, schema::SchemaRef};
use daft_dsl::Expr;
use daft_stats::{PartitionSpec, TableMetadata, TableStatistics};
use serde::{Deserialize, Serialize};

mod anonymous;

#[derive(Serialize, Deserialize, Clone, Copy)]
pub enum FileType {
    Parquet,
    Avro,
    Orc,
    Csv,
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

pub trait ScanOperator {
    fn schema(&self) -> SchemaRef;
    fn partitioning_keys(&self) -> &[Field];
    fn num_partitions(&self) -> DaftResult<usize>;
    fn filter(self: Box<Self>, predicate: &Expr) -> DaftResult<(bool, Box<Self>)>;
    fn select(self: Box<Self>, columns: &[&str]) -> DaftResult<Box<Self>>;
    fn limit(self: Box<Self>, num: usize) -> DaftResult<Box<Self>>;
    fn to_scan_tasks(self: Box<Self>)
        -> DaftResult<Box<dyn Iterator<Item = DaftResult<ScanTask>>>>;
}

pub type ScanOperatorRef = Box<dyn ScanOperator>;
