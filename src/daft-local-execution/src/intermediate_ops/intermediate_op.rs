use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

pub trait IntermediateOperator: dyn_clone::DynClone + Send + Sync {
    fn execute(&self, input: &Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>>;
    fn name(&self) -> String;
}

dyn_clone::clone_trait_object!(IntermediateOperator);
