use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{partitioning::RepartitionSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, NodeName, PipelineNodeConfig,
        PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    utils::channel::{Sender, create_channel},
};

pub(crate) struct RepartitionNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    repartition_spec: RepartitionSpec,
    num_partitions: usize,
    child: DistributedPipelineNode,
}

impl RepartitionNode {
    const NODE_NAME: NodeName = "Repartition";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        repartition_spec: RepartitionSpec,
        num_partitions: usize,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
            NodeType::Repartition,
            NodeCategory::BlockingSink,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            repartition_spec
                .to_clustering_spec(child.config().clustering_spec.num_partitions())
                .into(),
        );

        Self {
            config,
            context,
            repartition_spec,
            num_partitions,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    // Async execution: consume the materialized stream incrementally, transposing into
    // per-partition buckets as we go, then merge (if enabled) and emit.
    async fn execution_loop(
        self: Arc<Self>,
        local_repartition_node: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let materialized_stream = local_repartition_node.materialize(
            scheduler_handle,
            self.context.query_idx,
            task_id_counter,
        );

        // Build transpose incrementally: one bucket per output partition index.
        // As each materialized output arrives, split it and push each part into the
        // corresponding bucket (no need to wait for the full stream before transposing).
        let num_partitions = self.num_partitions;
        let mut buckets: Vec<Vec<MaterializedOutput>> =
            (0..num_partitions).map(|_| Vec::new()).collect();

        let mut materialized_stream = std::pin::pin!(materialized_stream);
        while let Some(materialized_output) = materialized_stream.try_next().await? {
            let split = materialized_output.split_into_materialized_outputs();
            for (j, part) in split.into_iter().enumerate() {
                if part.num_rows() > 0 {
                    buckets[j].push(part);
                }
            }
        }

        let enable_post_shuffle_merge = self.config.execution_config.enable_post_shuffle_merge;
        let target_size_bytes = self.config.execution_config.post_shuffle_merge_target_size_bytes;

        if !enable_post_shuffle_merge {
            for partition_group in buckets {
                let (in_memory_source_plan, psets) =
                    MaterializedOutput::into_in_memory_scan_with_psets(
                        partition_group,
                        self.config.schema.clone(),
                        self.node_id(),
                    );
                let builder = SwordfishTaskBuilder::new(in_memory_source_plan, self.as_ref())
                    .with_psets(psets);
                let _ = result_tx.send(builder).await;
            }
        } else {
            let sizes: Vec<usize> = buckets
                .iter()
                .map(|g| g.iter().map(|o| o.size_bytes()).sum())
                .collect();

            let mut ranges = Vec::new();
            let mut i = 0;
            while i < buckets.len() {
                let start = i;
                let mut acc_size = 0usize;
                while i < buckets.len() {
                    let s = sizes[i];
                    if acc_size > 0 && acc_size + s > target_size_bytes {
                        break;
                    }
                    acc_size += s;
                    i += 1;
                    if acc_size >= target_size_bytes {
                        break;
                    }
                }
                ranges.push((start, i));
            }

            for (start, end) in ranges {
                let mut merged_group = Vec::new();
                for j in start..end {
                    merged_group.extend(buckets[j].drain(..));
                }
                if merged_group.is_empty() {
                    continue;
                }
                let (in_memory_source_plan, psets) =
                    MaterializedOutput::into_in_memory_scan_with_psets(
                        merged_group,
                        self.config.schema.clone(),
                        self.node_id(),
                    );
                let builder = SwordfishTaskBuilder::new(in_memory_source_plan, self.as_ref())
                    .with_psets(psets);
                let _ = result_tx.send(builder).await;
            }
        }

        Ok(())
    }
}

impl PipelineNodeImpl for RepartitionNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child.clone()]
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let mut res = vec![format!("Repartition: {}", self.repartition_spec.var_name())];
        res.extend(self.repartition_spec.multiline_display());
        if self.config.execution_config.enable_post_shuffle_merge {
            let target_mb = self.config.execution_config.post_shuffle_merge_target_size_bytes
                / (1024 * 1024);
            res.push(format!("Post-merge: enabled, target {}MB", target_mb));
        }
        res
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        // First pipeline the local repartition op
        let self_clone = self.clone();
        let local_repartition_node = input_node.pipeline_instruction(self.clone(), move |input| {
            LocalPhysicalPlan::repartition(
                input,
                self_clone.repartition_spec.clone(),
                self_clone.num_partitions,
                self_clone.config.schema.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(self_clone.node_id() as usize),
                    additional: None,
                },
            )
        });

        let (result_tx, result_rx) = create_channel(1);

        plan_context.spawn(self.execution_loop(
            local_repartition_node,
            plan_context.task_id_counter(),
            result_tx,
            plan_context.scheduler_handle(),
        ));
        TaskBuilderStream::from(result_rx)
    }
}
