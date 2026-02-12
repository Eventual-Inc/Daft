use common_error::{DaftError, DaftResult};
use common_scan_info::ScanTaskLikeRef;
use daft_local_plan::{Input, InputId};
use daft_micropartition::MicroPartitionRef;

use crate::channel::UnboundedSender;

/// Enum for input senders of different types
#[derive(Clone)]
pub(crate) enum InputSender {
    ScanTasks(UnboundedSender<(InputId, Vec<ScanTaskLikeRef>)>),
    InMemory(UnboundedSender<(InputId, Vec<MicroPartitionRef>)>),
    GlobPaths(UnboundedSender<(InputId, Vec<String>)>),
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
            _ => unreachable!("Invalid input sender for input type"),
        }
    }

    pub fn send_empty(&self, input_id: InputId) -> DaftResult<()> {
        match self {
            Self::ScanTasks(sender) => sender
                .send((input_id, vec![]))
                .map_err(|e| DaftError::ValueError(e.to_string())),
            Self::InMemory(sender) => sender
                .send((input_id, vec![]))
                .map_err(|e| DaftError::ValueError(e.to_string())),
            Self::GlobPaths(sender) => sender
                .send((input_id, vec![]))
                .map_err(|e| DaftError::ValueError(e.to_string())),
        }
    }
}
