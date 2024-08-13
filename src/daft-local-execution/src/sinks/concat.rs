// use std::sync::Arc;

// use common_error::DaftResult;
// use daft_micropartition::MicroPartition;
// use tracing::instrument;

// use super::sink::{Sink, SinkResultType};

// #[derive(Clone)]
// pub struct ConcatSink {
//     result_left: Vec<Arc<MicroPartition>>,
//     result_right: Vec<Arc<MicroPartition>>,
// }

// impl ConcatSink {
//     pub fn new() -> Self {
//         Self {
//             result_left: Vec::new(),
//             result_right: Vec::new(),
//         }
//     }

//     #[instrument(skip_all, name = "ConcatSink::sink")]
//     fn sink_left(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
//         self.result_left.push(input.clone());
//         Ok(SinkResultType::NeedMoreInput)
//     }

//     #[instrument(skip_all, name = "ConcatSink::sink")]
//     fn sink_right(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
//         self.result_right.push(input.clone());
//         Ok(SinkResultType::NeedMoreInput)
//     }
// }

// impl Sink for ConcatSink {
//     fn sink(&mut self, index: usize, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
//         match index {
//             0 => self.sink_left(input),
//             1 => self.sink_right(input),
//             _ => panic!("concat only supports 2 inputs, got {index}"),
//         }
//     }

//     fn in_order(&self) -> bool {
//         true
//     }

//     fn num_inputs(&self) -> usize {
//         2
//     }

//     #[instrument(skip_all, name = "ConcatSink::finalize")]
//     fn finalize(self: Box<Self>) -> DaftResult<Vec<Arc<MicroPartition>>> {
//         Ok(self
//             .result_left
//             .into_iter()
//             .chain(self.result_right.into_iter())
//             .collect())
//     }
// }
