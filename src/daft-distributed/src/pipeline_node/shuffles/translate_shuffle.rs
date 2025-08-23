use std::sync::Arc;

use common_error::DaftResult;
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_schema::schema::SchemaRef;

use crate::pipeline_node::{
    shuffles::{
        gather::GatherNode, pre_shuffle_merge::PreShuffleMergeNode, repartition::RepartitionNode,
    },
    translate::LogicalPlanToPipelineNodeTranslator,
    DistributedPipelineNode, NodeID,
};

impl LogicalPlanToPipelineNodeTranslator {
    pub fn gen_shuffle_node(
        &mut self,
        logical_node_id: Option<NodeID>,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> DaftResult<Arc<dyn DistributedPipelineNode>> {
        let num_partitions = match &repartition_spec {
            RepartitionSpec::Hash(config) => config
                .num_partitions
                .unwrap_or_else(|| child.config().clustering_spec.num_partitions()),
            RepartitionSpec::Random(config) => config
                .num_partitions
                .unwrap_or_else(|| child.config().clustering_spec.num_partitions()),
            RepartitionSpec::Range(config) => config
                .num_partitions
                .unwrap_or_else(|| child.config().clustering_spec.num_partitions()),
            RepartitionSpec::IntoPartitions(config) => config.num_partitions,
        };

        let use_pre_shuffle_merge = self.should_use_pre_shuffle_merge(&child, num_partitions)?;

        if use_pre_shuffle_merge {
            // Create merge node first
            let merge_node = PreShuffleMergeNode::new(
                self.get_next_pipeline_node_id(),
                logical_node_id,
                &self.stage_config,
                self.stage_config.config.pre_shuffle_merge_threshold,
                schema.clone(),
                child,
            )
            .arced();

            Ok(RepartitionNode::new(
                self.get_next_pipeline_node_id(),
                logical_node_id,
                &self.stage_config,
                repartition_spec,
                num_partitions,
                schema,
                merge_node,
            )
            .arced())
        } else {
            Ok(RepartitionNode::new(
                self.get_next_pipeline_node_id(),
                logical_node_id,
                &self.stage_config,
                repartition_spec,
                num_partitions,
                schema,
                child,
            )
            .arced())
        }
    }

    /// Determine if we should use pre-shuffle merge strategy
    fn should_use_pre_shuffle_merge(
        &self,
        child: &Arc<dyn DistributedPipelineNode>,
        target_num_partitions: usize,
    ) -> DaftResult<bool> {
        let input_num_partitions = child.config().clustering_spec.num_partitions();

        match self.stage_config.config.shuffle_algorithm.as_str() {
            "pre_shuffle_merge" => Ok(true),
            "map_reduce" => Ok(false),
            "flight_shuffle" => Err(common_error::DaftError::ValueError(
                "Flight shuffle not yet implemented for flotilla".to_string(),
            )),
            "auto" => {
                let total_num_partitions = input_num_partitions * target_num_partitions;
                let geometric_mean = (total_num_partitions as f64).sqrt() as usize;
                const PARTITION_THRESHOLD_TO_USE_PRE_SHUFFLE_MERGE: usize = 200;
                Ok(geometric_mean > PARTITION_THRESHOLD_TO_USE_PRE_SHUFFLE_MERGE)
            }
            _ => Ok(false), // Default to naive map_reduce for unknown strategies
        }
    }

    pub fn gen_gather_node(
        &mut self,
        logical_node_id: Option<NodeID>,
        input_node: Arc<dyn DistributedPipelineNode>,
    ) -> Arc<dyn DistributedPipelineNode> {
        if input_node.config().clustering_spec.num_partitions() == 1 {
            return input_node;
        }

        GatherNode::new(
            self.get_next_pipeline_node_id(),
            logical_node_id,
            &self.stage_config,
            input_node.config().schema.clone(),
            input_node,
        )
        .arced()
    }
}
