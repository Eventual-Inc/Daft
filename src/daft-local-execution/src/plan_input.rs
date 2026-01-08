use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_scan_info::ScanTaskLikeRef;
use daft_micropartition::{MicroPartition, MicroPartitionRef, partitioning::PartitionSetRef};
use daft_scan::ScanTaskRef;

use crate::channel::Sender;

/// Input ID type, matching TaskID from distributed scheduling
pub type InputId = u32;

/// Message that can flow through the pipeline - either data (Morsel) or a flush signal
#[derive(Debug, Clone)]
pub enum PipelineMessage {
    /// Data morsel with input_id and partition
    Morsel {
        input_id: InputId,
        partition: Arc<MicroPartition>,
    },
    /// Flush signal for a specific input_id - indicates that input is finished
    Flush(InputId),
}

/// Enum representing different types of inputs to a plan
#[derive(Debug, Clone)]
pub enum PlanInput {
    ScanTasks(Vec<ScanTaskLikeRef>),
    InMemoryPartitions(PartitionSetRef<MicroPartitionRef>),
    GlobPaths(Vec<String>),
}

/// Enum for input senders of different types
#[derive(Clone)]
pub(crate) enum InputSender {
    ScanTasks(Sender<(InputId, Vec<ScanTaskRef>)>),
    InMemory(Sender<(InputId, PartitionSetRef<MicroPartitionRef>)>),
    GlobPaths(Sender<(InputId, Vec<String>)>),
}

impl InputSender {
    pub async fn send(&self, input_id: InputId, input: PlanInput) -> DaftResult<()> {
        match (self, input) {
            (InputSender::ScanTasks(sender), PlanInput::ScanTasks(tasks)) => {
                // Convert ScanTaskLikeRef to ScanTaskRef
                let tasks: Vec<ScanTaskRef> = tasks
                    .into_iter()
                    .map(|task| task.as_any_arc().downcast().unwrap())
                    .collect();
                sender
                    .send((input_id, tasks))
                    .await
                    .map_err(|e| DaftError::ValueError(e.to_string()))
            }
            (InputSender::InMemory(sender), PlanInput::InMemoryPartitions(partitions)) => sender
                .send((input_id, partitions))
                .await
                .map_err(|e| DaftError::ValueError(e.to_string())),
            (InputSender::GlobPaths(sender), PlanInput::GlobPaths(glob_paths)) => sender
                .send((input_id, glob_paths))
                .await
                .map_err(|e| DaftError::ValueError(e.to_string())),
            _ => Err(DaftError::ValueError("Invalid input sender".to_string())),
        }
    }
}
