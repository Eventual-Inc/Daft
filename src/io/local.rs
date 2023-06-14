use crate::error::DaftResult;

use super::object_io::{GetResult, ObjectSource};
use async_trait::async_trait;

pub struct LocalSource {}

impl LocalSource {
    pub async fn new() -> Self {
        LocalSource {}
    }
}

#[async_trait]
impl ObjectSource for LocalSource {
    async fn get(&self, uri: &str) -> DaftResult<GetResult> {
        let path = url::Url::parse(uri)?;
        let file_path = path.to_file_path().unwrap();
        let file = tokio::fs::File::open(file_path).await;
        match file {
            Ok(file) => Ok(GetResult::File(file)),
            Err(err) => {
                log::warn!("Failed opening local path: {uri}");
                Err(err.into())
            }
        }
    }
}
