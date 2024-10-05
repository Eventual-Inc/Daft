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
    fn execute(&mut self, index: usize, input: &[Table]) -> DaftResult<StreamSinkOutput> {
        assert_eq!(index, 0);
        use std::cmp::Ordering::*;

        let mut result = vec![];
        for t in input {
            let input_num_rows = t.len();

            match input_num_rows.cmp(&self.remaining) {
                Less => {
                    result.push(t.clone());
                    self.remaining -= input_num_rows;
                }
                Equal => {
                    self.remaining = 0;
                    result.push(t.clone());
                    return Ok(StreamSinkOutput::Finished(Some(result.into())));
                }
                Greater => {
                    let taken = t.head(self.remaining)?;
                    self.remaining -= taken.len();
                    result.push(taken.clone());
                    return Ok(StreamSinkOutput::Finished(Some(result.into())));
                }
            }
        }
        Ok(StreamSinkOutput::NeedMoreInput(Some(result.into())))
    }

    fn name(&self) -> &'static str {
        "Limit"
    }
}
