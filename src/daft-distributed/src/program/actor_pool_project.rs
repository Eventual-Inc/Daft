use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;

use crate::{channel::Receiver, runtime::JoinSet, scheduling::dispatcher::TaskDispatcherHandle};

pub struct ActorPoolProjectProgram {}

impl ActorPoolProjectProgram {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {}
    }

    #[allow(dead_code)]
    pub fn spawn_program(
        self,
        _task_dispatcher_handle: TaskDispatcherHandle,
        _config: Arc<DaftExecutionConfig>,
        _psets: HashMap<String, Vec<PartitionRef>>,
        _input_rx: Option<Receiver<PartitionRef>>,
        _joinset: &mut JoinSet<DaftResult<()>>,
    ) -> Receiver<PartitionRef> {
        todo!()
    }
}
