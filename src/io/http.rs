use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};

use crate::error::DaftResult;

use super::object_io::{GetResult, ObjectSource};

pub struct HttpSource {}

#[async_trait]
impl ObjectSource for HttpSource {
    async fn get(&self, uri: String) -> DaftResult<GetResult> {
        let response = reqwest::get(uri).await?;
        let response = response.error_for_status()?;
        let stream = response.bytes_stream();
        let stream = stream.map_err(|e| e.into());
        Ok(GetResult::Stream(stream.boxed()))
    }
}
