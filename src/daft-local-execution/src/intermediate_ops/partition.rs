use std::{collections::VecDeque, sync::Arc};

use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::spawner::ComputeTaskSpawner;

struct PartitionState(VecDeque<MicroPartition>);

impl IntermediateOpState for PartitionState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

struct PartitionParams {
    partition_by: Option<Vec<ExprRef>>,
    num_partitions: usize,
}

pub struct PartitionOperator {
    params: Arc<PartitionParams>,
}

impl PartitionOperator {
    pub fn new(partition_by: Option<Vec<ExprRef>>, num_partitions: usize) -> Self {
        assert!(
            num_partitions > 0,
            "num_partitions must be greater than 0 for PartitionOperator"
        );
        Self {
            params: Arc::new(PartitionParams {
                partition_by,
                num_partitions,
            }),
        }
    }
}

impl IntermediateOperator for PartitionOperator {
    #[instrument(skip_all, name = "PartitionOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn IntermediateOpState>,
        task_spawner: &ComputeTaskSpawner,
    ) -> IntermediateOpExecuteResult {
        let params = self.params.clone();
        task_spawner
            .spawn(
                async move {
                    let partition_state =
                        state.as_any_mut().downcast_mut::<PartitionState>().unwrap();
                    if partition_state.0.len() == 0 {
                        let partitioned = match &params.partition_by {
                            Some(partition_by) => {
                                input.partition_by_hash(&partition_by, params.num_partitions)?
                            }
                            None => input.partition_by_random(params.num_partitions, 0)?,
                        };
                        partition_state.0.extend(partitioned);
                    }
                    let next_partition = partition_state.0.pop_front().unwrap();
                    if partition_state.0.len() == 0 {
                        Ok((
                            state,
                            IntermediateOperatorResult::NeedMoreInput(Some(next_partition.into())),
                        ))
                    } else {
                        Ok((
                            state,
                            IntermediateOperatorResult::HasMoreOutput(
                                MicroPartition::empty(None).into(),
                                next_partition.into(),
                            ),
                        ))
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        match &self.params.partition_by {
            Some(partition_by) => {
                res.push(format!(
                    "Partition by: {}",
                    partition_by.iter().map(|e| e.to_string()).join(", ")
                ));
            }
            None => {
                res.push("Partition by random".to_string());
            }
        }
        res.push(format!("Num partitions: {}", self.params.num_partitions));
        res
    }

    fn name(&self) -> &'static str {
        "Partition"
    }

    fn make_state(&self) -> common_error::DaftResult<Box<dyn IntermediateOpState>> {
        Ok(Box::new(PartitionState(VecDeque::new())))
    }
}
