use common_error::{DaftError, DaftResult};
use daft_local_plan::{FlightShuffleReadInput, Input, InputId};
use daft_micropartition::MicroPartitionRef;
use daft_scan::ScanTaskRef;

use crate::{
    channel::UnboundedSender,
    sources::streaming::{StreamingInputSender, StreamingPushError, StreamingPushResult},
};

/// Enum for input senders of different types
#[derive(Clone)]
pub(crate) enum InputSender {
    ScanTasks(UnboundedSender<(InputId, Vec<ScanTaskRef>)>),
    InMemory(UnboundedSender<(InputId, Vec<MicroPartitionRef>)>),
    GlobPaths(UnboundedSender<(InputId, Vec<String>)>),
    FlightShuffle(UnboundedSender<(InputId, Vec<FlightShuffleReadInput>)>),
    Streaming(StreamingInputSender),
}

impl InputSender {
    pub fn send(&self, input_id: InputId, input: Input) -> DaftResult<()> {
        match (self, input) {
            (Self::ScanTasks(sender), Input::ScanTasks(tasks)) => sender
                .send((input_id, tasks))
                .map_err(|e| DaftError::ValueError(e.to_string())),
            (Self::InMemory(sender), Input::InMemory(partitions)) => sender
                .send((input_id, partitions))
                .map_err(|e| DaftError::ValueError(e.to_string())),
            (Self::GlobPaths(sender), Input::GlobPaths(glob_paths)) => sender
                .send((input_id, glob_paths))
                .map_err(|e| DaftError::ValueError(e.to_string())),
            (Self::FlightShuffle(sender), Input::FlightShuffle(inputs)) => sender
                .send((input_id, inputs))
                .map_err(|e| DaftError::ValueError(e.to_string())),
            (Self::Streaming(sender), Input::InMemory(partitions)) => {
                for partition in partitions {
                    match sender.try_push_batch_owned(0, input_id, partition) {
                        Ok(()) => {}
                        Err(StreamingPushError::Again(_)) => {
                            return Err(DaftError::ValueError(
                                "streaming input queue is full".to_string(),
                            ));
                        }
                        Err(StreamingPushError::End(_)) => {
                            return Err(DaftError::ValueError(
                                "streaming input is closed".to_string(),
                            ));
                        }
                        Err(StreamingPushError::InvalidSource(_)) => unreachable!(),
                    }
                }
                match sender.try_finish_for_input(0, input_id) {
                    StreamingPushResult::Ok => Ok(()),
                    StreamingPushResult::Again => Err(DaftError::ValueError(
                        "streaming input queue is full before Finish".to_string(),
                    )),
                    StreamingPushResult::End => Err(DaftError::ValueError(
                        "streaming input is closed before Finish".to_string(),
                    )),
                    StreamingPushResult::Error => unreachable!(),
                }
            }
            _ => unreachable!("Invalid input sender for input type"),
        }
    }
}
