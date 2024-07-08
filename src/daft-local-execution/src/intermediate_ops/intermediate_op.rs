use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

use crate::{Receiver, Sender};

pub trait IntermediateOperator: dyn_clone::DynClone + Send + Sync {
    fn execute(&self, input: &Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>>;
    fn name(&self) -> String;
}

dyn_clone::clone_trait_object!(IntermediateOperator);

pub async fn run_intermediate_operators(
    mut receiver: Receiver,
    intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
    send_to_sink: Sender,
) -> DaftResult<()> {
    log::debug!(
        "Running intermediate operators: {}",
        intermediate_operators
            .iter()
            .fold(String::new(), |acc, op| { acc + &op.name() + " -> " })
    );

    while let Some(morsel) = receiver.recv().await {
        let mut result = morsel?;
        for op in intermediate_operators.iter() {
            result = op.execute(&result)?;
        }
        let _ = send_to_sink.send(Ok(result)).await;
    }

    log::debug!("Intermediate operators finished");
    Ok(())
}
