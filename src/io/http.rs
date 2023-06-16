use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};

use super::object_io::{GetResult, ObjectSource};

pub struct HttpSource {}

impl From<reqwest::Error> for super::Error {
    fn from(error: reqwest::Error) -> Self {
        super::Error::Generic {
            store: "http",
            source: error.into(),
        }
    }
}

impl HttpSource {
    pub async fn new() -> Self {
        HttpSource {}
    }
}

#[async_trait]
impl ObjectSource for HttpSource {
    async fn get(&self, uri: &str) -> super::Result<GetResult> {
        let response = reqwest::get(uri).await?;
        let response = response.error_for_status()?;
        let size_bytes = response.content_length().map(|s| s as usize);
        let stream = response.bytes_stream();
        let stream = stream.map_err(|e| e.into());
        Ok(GetResult::Stream(stream.boxed(), size_bytes))
    }
}
