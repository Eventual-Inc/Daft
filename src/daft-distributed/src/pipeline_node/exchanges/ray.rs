use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, ShuffleWriteBackend};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use super::ExchangeWriteConfig;
use crate::{
    pipeline_node::{NodeID, PipelineNodeImpl, ShufflePartitionRef, TaskOutput},
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::Sender,
};

pub(crate) fn build_write_stage(
    node_id: NodeID,
    num_partitions: usize,
    schema: SchemaRef,
    config: ExchangeWriteConfig,
) -> crate::pipeline_node::TaskBuilderStream {
    config
        .input_node
        .pipeline_instruction(config.producer, move |input| {
            LocalPhysicalPlan::shuffle_write(
                input,
                num_partitions,
                schema.clone(),
                ShuffleWriteBackend::Ray {
                    repartition_spec: config.repartition_spec.clone(),
                },
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)),
            )
        })
}

pub(crate) fn ray_partition_groups_from_outputs(
    outputs: Vec<TaskOutput>,
    num_partitions: usize,
) -> DaftResult<Vec<Vec<PartitionRef>>> {
    let mut partition_groups = (0..num_partitions).map(|_| Vec::new()).collect::<Vec<_>>();

    for output in outputs {
        let TaskOutput::ShuffleWrite(output) = output else {
            return Err(DaftError::InternalError(
                "Expected Ray shuffle write task output".to_string(),
            ));
        };

        if output.partitions.len() != num_partitions {
            return Err(DaftError::InternalError(format!(
                "Expected {} Ray shuffle partitions, got {}",
                num_partitions,
                output.partitions.len()
            )));
        }

        for (partition_idx, partition) in output.partitions.into_iter().enumerate() {
            match partition {
                ShufflePartitionRef::Ray(partition) => {
                    if partition.num_rows() > 0 {
                        partition_groups[partition_idx].push(partition);
                    }
                }
                ShufflePartitionRef::Flight(_) => {
                    return Err(DaftError::InternalError(
                        "Expected Ray shuffle partition ref but received Flight".to_string(),
                    ));
                }
            }
        }
    }

    Ok(partition_groups)
}

pub(crate) async fn emit_read_tasks(
    node_id: NodeID,
    schema: SchemaRef,
    partition_groups: Vec<Vec<PartitionRef>>,
    node: &dyn PipelineNodeImpl,
    result_tx: Sender<SwordfishTaskBuilder>,
) -> DaftResult<()> {
    for partition_group in partition_groups {
        let total_size_bytes = partition_group
            .iter()
            .map(|p| p.size_bytes())
            .sum::<usize>();
        let in_memory_scan = LocalPhysicalPlan::in_memory_scan(
            node_id,
            schema.clone(),
            total_size_bytes,
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(node_id as usize)),
        );

        let builder = SwordfishTaskBuilder::new(in_memory_scan, node, node_id)
            .with_psets(node_id, partition_group);

        let _ = result_tx.send(builder).await;
    }

    Ok(())
}
