use common_error::DaftResult;
use daft_core::{datatypes::Field, schema::SchemaRef};
use daft_dsl::Expr;
use daft_stats::{PartitionSpec, TableStatistics};
use serde::{Deserialize, Serialize};
mod table_metadata;

pub use table_metadata::TableMetadata;

#[derive(Serialize, Deserialize)]
pub enum FileType {
    Parquet,
    Avro,
    Orc,
    Csv,
}

#[derive(Serialize, Deserialize)]
pub enum DataFileSource {
    AnonymousDataFile {
        path: String,
        metadata: Option<TableMetadata>,
        partition_spec: Option<PartitionSpec>,
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
    filter: Option<Expr>,
}

pub trait ScanOperator {
    fn schema(&self) -> SchemaRef;
    fn partitioning_keys(&self) -> &[Field];
    fn partition_spec(&self) -> Option<&PartitionSpec>;
    fn num_partitions(&self) -> DaftResult<usize>;
    fn filter(self: Box<Self>, predicate: &Expr) -> DaftResult<Box<Self>>;
    fn select(self: Box<Self>, columns: &[&str]) -> DaftResult<Box<Self>>;
    fn limit(self: Box<Self>, num: usize) -> DaftResult<Box<Self>>;
    fn to_scan_tasks(self: Box<Self>)
        -> DaftResult<Box<dyn Iterator<Item = DaftResult<ScanTask>>>>;
}

pub type ScanOperatorRef = Box<dyn ScanOperator>;
