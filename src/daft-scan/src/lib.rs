use common_error::DaftResult;
use daft_core::{datatypes::Field, schema::SchemaRef};
use daft_dsl::Expr;
use serde::{Deserialize, Serialize};
mod table_metadata;
pub use table_metadata::TableMetadata;

#[derive(Serialize, Deserialize)]
enum FileType {
    Parquet,
    Avro,
    Orc,
    Csv,
}

#[derive(Serialize, Deserialize)]
enum ScanTask {
    AnonymousDataFile {
        path: String,
    },
    CatalogDataFile {
        file_type: FileType,
        path: String,
        metadata: TableMetadata,
        //statistics: Option<TableStatistics> // Need to figure out better serialization story for series
    },
}

trait ScanOperator {
    fn schema(&self) -> SchemaRef;
    fn partitioning_keys(&self) -> &[Field];
    fn num_partitions(&self) -> DaftResult<usize>;
    fn filter(self: Box<Self>, predicate: &Expr) -> DaftResult<Box<Self>>;
    fn limit(self: Box<Self>, num: usize) -> DaftResult<Box<Self>>;
    fn to_scan_tasks(self: Box<Self>)
        -> DaftResult<Box<dyn Iterator<Item = DaftResult<ScanTask>>>>;
}
