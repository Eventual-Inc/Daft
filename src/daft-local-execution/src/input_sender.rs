use common_error::{DaftError, DaftResult};
use daft_local_plan::{Input, InputId};
use daft_micropartition::MicroPartitionRef;
use daft_scan::ScanTaskRef;

use crate::channel::Sender;

/// Enum for input senders of different types
#[derive(Clone)]
pub(crate) enum InputSender {
    ScanTasks(Sender<(InputId, Vec<ScanTaskRef>)>),
    InMemory(Sender<(InputId, Vec<MicroPartitionRef>)>),
    GlobPaths(Sender<(InputId, Vec<String>)>),
}

impl InputSender {
    pub async fn send(&self, input_id: InputId, input: Input) -> DaftResult<()> {
        match (self, input) {
            (Self::ScanTasks(sender), Input::ScanTasks(tasks)) => {
                // Convert ScanTaskLikeRef to ScanTaskRef
                let tasks: Vec<ScanTaskRef> = tasks
                    .into_iter()
                    .map(|task| {
                        task.as_any_arc().downcast().map_err(|_| {
                            DaftError::ValueError(
                                "Failed to downcast ScanTaskLikeRef to ScanTaskRef".to_string(),
                            )
                        })
                    })
                    .collect::<DaftResult<Vec<_>>>()?;
                sender
                    .send((input_id, tasks))
                    .await
                    .map_err(|e| DaftError::ValueError(e.to_string()))
            }
            (Self::InMemory(sender), Input::InMemory(partitions)) => sender
                .send((input_id, partitions))
                .await
                .map_err(|e| DaftError::ValueError(e.to_string())),
            (Self::GlobPaths(sender), Input::GlobPaths(glob_paths)) => sender
                .send((input_id, glob_paths))
                .await
                .map_err(|e| DaftError::ValueError(e.to_string())),
            _ => Err(DaftError::ValueError("Invalid input sender".to_string())),
        }
    }
}
