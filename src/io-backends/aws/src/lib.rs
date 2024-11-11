#![feature(async_closure)]

use std::sync::Arc;

use daft_io::{ObjectSource, ObjectSourceFactory, SourceType};

mod s3_like;
use daft_io::ObjectSourceFactoryEntry;

struct S3Factory {}

#[async_trait::async_trait]
impl ObjectSourceFactory for S3Factory {
    async fn get_source(
        &self,
        config: &common_io_config::IOConfig,
    ) -> daft_io::Result<Arc<dyn ObjectSource>> {
        Ok(s3_like::S3LikeSource::get_client(&config.s3).await? as Arc<dyn ObjectSource>)
    }
}

inventory::submit! {
    ObjectSourceFactoryEntry::new(SourceType::S3, &S3Factory {})
}
