use std::sync::Arc;

use daft_micropartition::MicroPartition;

/// Input ID type, used to track which input batch a partition originated from
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
