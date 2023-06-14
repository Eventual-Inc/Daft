use crate::error::{DaftError, DaftResult};

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
        const EXPECTED_START: &str = "file://";
        let is_valid = uri.starts_with(EXPECTED_START);
        if !is_valid {
            return Err(DaftError::ValueError(
                "Local Path does not start with `file://`: is: {uri}".into(),
            ));
        }
        let file_path = &uri[EXPECTED_START.len()..];
        let file = tokio::fs::File::open(file_path).await;
        match file {
            Ok(file) => Ok(GetResult::File(file)),
            Err(err) => Err(err.into()),
        }
    }
}
