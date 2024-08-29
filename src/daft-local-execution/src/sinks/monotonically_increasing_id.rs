use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::streaming_sink::{StreamSinkOutput, StreamingSink};

pub struct MonotonicallyIncreasingIdSink {
    column_name: String,
    next_partition_id: u64,
}

impl MonotonicallyIncreasingIdSink {
    pub fn new(column_name: String) -> Self {
        Self {
            column_name,
            next_partition_id: 0,
        }
    }
    pub fn boxed(self) -> Box<dyn StreamingSink> {
        Box::new(self)
    }
}

impl StreamingSink for MonotonicallyIncreasingIdSink {
    #[instrument(skip_all, name = "MonotonicallyIncreasingIdSink::sink")]
    fn execute(
        &mut self,
        index: usize,
        input: &Arc<MicroPartition>,
    ) -> DaftResult<StreamSinkOutput> {
        assert_eq!(index, 0);

        let added =
            input.add_monotonically_increasing_id(self.next_partition_id, &self.column_name)?;
        self.next_partition_id += 1;
        Ok(StreamSinkOutput::NeedMoreInput(Some(Arc::new(added))))
    }

    fn name(&self) -> &'static str {
        "MonotonicallyIncreasingId"
    }
}
