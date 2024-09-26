use std::sync::Arc;

use common_error::DaftResult;
use daft_table::Table;
use tracing::instrument;

use super::streaming_sink::{StreamSinkOutput, StreamingSink};

pub struct LimitSink {
    #[allow(dead_code)]
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
    fn execute(&mut self, index: usize, input: &Arc<Table>) -> DaftResult<StreamSinkOutput> {
        assert_eq!(index, 0);

        let input_num_rows = input.len();

        use std::cmp::Ordering::*;
        match input_num_rows.cmp(&self.remaining) {
            Less => {
                self.remaining -= input_num_rows;
                Ok(StreamSinkOutput::NeedMoreInput(Some(input.clone())))
            }
            Equal => {
                self.remaining = 0;
                Ok(StreamSinkOutput::Finished(Some(input.clone())))
            }
            Greater => {
                let taken = input.head(self.remaining)?;
                self.remaining -= taken.len();
                Ok(StreamSinkOutput::Finished(Some(taken.into())))
            }
        }
    }

    fn name(&self) -> &'static str {
        "Limit"
    }
}
