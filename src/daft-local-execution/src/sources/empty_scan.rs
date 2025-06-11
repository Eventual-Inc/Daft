use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::source::Source;
use crate::channel::{create_channel, Receiver, Sender};

pub struct EmptyScanSource {
    schema: SchemaRef,
}

impl EmptyScanSource {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
    pub fn arced(self) -> Arc<dyn Source> {
        Arc::new(self) as Arc<dyn Source>
    }
}

#[async_trait]
impl Source for EmptyScanSource {
    #[instrument(name = "EmptyScanSource::get_data", level = "info", skip_all)]
    async fn get_data(
        &self,
        _maintain_order: bool,
        _io_stats: IOStatsRef,
        tx: Sender<(usize, Receiver<Arc<MicroPartition>>)>,
    ) -> DaftResult<()> {
        let (empty_sender, empty_receiver) = create_channel(0);
        if tx.send((0, empty_receiver)).await.is_err() {
            return Ok(());
        }
        let empty = Arc::new(MicroPartition::empty(Some(self.schema.clone())));
        if empty_sender.send(empty).await.is_err() {
            return Ok(());
        }
        Ok(())
    }
    fn name(&self) -> &'static str {
        "EmptyScan"
    }
    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("EmptyScan:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res
    }
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}
