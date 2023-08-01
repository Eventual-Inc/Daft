use daft_core::schema::SchemaRef;

pub enum SourceInfo {
    ParquetFilesInfo(ParquetFilesInfo),
}

impl SourceInfo {
    pub fn schema(&self) -> SchemaRef {
        use SourceInfo::*;
        match self {
            ParquetFilesInfo(pq_info) => pq_info.schema.clone(),
        }
    }
}

pub struct ParquetFilesInfo {
    pub filepaths: Vec<String>, // TODO: pull in some sort of URL crate for this
    pub schema: SchemaRef,
}

impl ParquetFilesInfo {
    pub(crate) fn new(filepaths: Vec<String>, schema: SchemaRef) -> Self {
        Self { filepaths, schema }
    }
}
