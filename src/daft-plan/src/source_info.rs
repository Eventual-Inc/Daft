use daft_core::schema::SchemaRef;

#[derive(Debug)]
pub enum SourceInfo {
    FilesInfo(FilesInfo),
}

impl SourceInfo {
    pub fn schema(&self) -> SchemaRef {
        use SourceInfo::*;
        match self {
            FilesInfo(files_info) => files_info.schema.clone(),
        }
    }
}

#[derive(Debug)]
pub enum FileFormat {
    Parquet,
    Csv,
    Json,
}

#[derive(Debug)]
pub struct FilesInfo {
    pub file_format: FileFormat,
    pub filepaths: Vec<String>, // TODO: pull in some sort of URL crate for this
    pub schema: SchemaRef,
}

impl FilesInfo {
    pub(crate) fn new(file_format: FileFormat, filepaths: Vec<String>, schema: SchemaRef) -> Self {
        Self {
            file_format,
            filepaths,
            schema,
        }
    }
}
