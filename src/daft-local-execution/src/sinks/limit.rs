use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::streaming_sink::{StreamSinkOutput, StreamingSink};

#[derive(Clone)]
pub struct LimitSink {
    limit: usize,
    remaining: usize,
}

impl LimitSink {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            remaining: limit,
        }
    }
    pub fn boxed(self) -> Box<dyn StreamingSink> {
        Box::new(self)
    }
}

impl StreamingSink for LimitSink {
    #[instrument(skip_all, name = "LimitSink::sink")]
    fn execute(
        &mut self,
        index: usize,
        input: &Arc<MicroPartition>,
    ) -> DaftResult<StreamSinkOutput> {
        assert_eq!(index, 0);

        let input_num_rows = input.len();

        if input_num_rows == self.remaining {
            self.remaining = 0;
            Ok(StreamSinkOutput::Finished(Some(input.clone())))
        } else if input_num_rows > self.remaining {
            let taken = input.head(self.remaining)?;
            self.remaining -= taken.len();
            Ok(StreamSinkOutput::Finished(Some(Arc::new(taken))))
        } else {
            self.remaining -= input_num_rows;
            Ok(StreamSinkOutput::NeedMoreInput(Some(input.clone())))
        }
    }

    fn name(&self) -> &'static str {
        "Limit"
    }
}
