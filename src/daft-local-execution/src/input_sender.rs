use common_error::{DaftError, DaftResult};
use common_scan_info::ScanTaskLikeRef;
use daft_local_plan::{Input, InputId};
use daft_micropartition::MicroPartitionRef;

use crate::channel::Sender;

/// Enum for input senders of different types
#[derive(Clone)]
pub(crate) enum InputSender {
    ScanTasks(Sender<(InputId, Vec<ScanTaskLikeRef>)>),
    InMemory(Sender<(InputId, Vec<MicroPartitionRef>)>),
    GlobPaths(Sender<(InputId, Vec<String>)>),
}

impl InputSender {
    pub async fn send(&self, input_id: InputId, input: Input) -> DaftResult<()> {
        match (self, input) {
            (Self::ScanTasks(sender), Input::ScanTasks(tasks)) => sender
                .send((input_id, tasks))
                .await
                .map_err(|e| DaftError::ValueError(e.to_string())),
            (Self::InMemory(sender), Input::InMemory(partitions)) => sender
                .send((input_id, partitions))
                .await
                .map_err(|e| DaftError::ValueError(e.to_string())),
            (Self::GlobPaths(sender), Input::GlobPaths(glob_paths)) => sender
                .send((input_id, glob_paths))
                .await
                .map_err(|e| DaftError::ValueError(e.to_string())),
            _ => unreachable!("Invalid input sender for input type"),
        }
    }

    pub async fn send_empty(&self, input_id: InputId) -> DaftResult<()> {
        match self {
            Self::ScanTasks(sender) => sender
                .send((input_id, vec![]))
                .await
                .map_err(|e| DaftError::ValueError(e.to_string())),
            Self::InMemory(sender) => sender
                .send((input_id, vec![]))
                .await
                .map_err(|e| DaftError::ValueError(e.to_string())),
            Self::GlobPaths(sender) => sender
                .send((input_id, vec![]))
                .await
                .map_err(|e| DaftError::ValueError(e.to_string())),
        }
    }
}
