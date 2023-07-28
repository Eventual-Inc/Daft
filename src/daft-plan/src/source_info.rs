use daft_core::schema::SchemaRef;

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

pub struct FilesInfo {
    pub filepaths: Vec<String>, // TODO: pull in some sort of URL crate for this
    pub schema: SchemaRef,
}
