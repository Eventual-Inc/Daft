use common_error::DaftResult;
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_schema::schema::SchemaRef;

use crate::pipeline_node::{
    DistributedPipelineNode,
    shuffles::{
        flight_shuffle::FlightShuffleNode, gather::GatherNode,
        pre_shuffle_merge::PreShuffleMergeNode, repartition::RepartitionNode,
    },
    translate::LogicalPlanToPipelineNodeTranslator,
};

impl LogicalPlanToPipelineNodeTranslator {
    pub fn gen_shuffle_node(
        &mut self,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> DaftResult<DistributedPipelineNode> {
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

        // Check if we should use flight shuffle
        if self.plan_config.config.shuffle_algorithm.as_str() == "flight_shuffle" {
            let shuffle_dirs = self.plan_config.config.flight_shuffle_dirs.clone();
            let compression = None;
            let child = if use_pre_shuffle_merge {
                PreShuffleMergeNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    self.plan_config.config.pre_shuffle_merge_threshold,
                    schema.clone(),
                    child,
                )
                .into_node()
            } else {
                child
            };
            return Ok(FlightShuffleNode::new(
                self.get_next_pipeline_node_id(),
                &self.plan_config,
                repartition_spec,
                schema,
                num_partitions,
                shuffle_dirs,
                compression,
                child,
            )
            .into_node());
        }

        if use_pre_shuffle_merge {
            // Create merge node first
            let merge_node = PreShuffleMergeNode::new(
                self.get_next_pipeline_node_id(),
                &self.plan_config,
                self.plan_config.config.pre_shuffle_merge_threshold,
                schema.clone(),
                child,
            )
            .into_node();

            Ok(RepartitionNode::new(
                self.get_next_pipeline_node_id(),
                &self.plan_config,
                repartition_spec,
                num_partitions,
                schema,
                merge_node,
            )
            .into_node())
        } else {
            Ok(RepartitionNode::new(
                self.get_next_pipeline_node_id(),
                &self.plan_config,
                repartition_spec,
                num_partitions,
                schema,
                child,
            )
            .into_node())
        }
    }

    /// Determine if we should use pre-shuffle merge strategy
    fn should_use_pre_shuffle_merge(
        &self,
        child: &DistributedPipelineNode,
        target_num_partitions: usize,
    ) -> DaftResult<bool> {
        match self.plan_config.config.pre_shuffle_merge {
            Some(true) => Ok(true),
            Some(false) => Ok(false),
            None => {
                let input_num_partitions = child.config().clustering_spec.num_partitions();
                let total_num_partitions = input_num_partitions * target_num_partitions;
                let geometric_mean = (total_num_partitions as f64).sqrt() as usize;
                const PARTITION_THRESHOLD_TO_USE_PRE_SHUFFLE_MERGE: usize = 200;
                Ok(geometric_mean > PARTITION_THRESHOLD_TO_USE_PRE_SHUFFLE_MERGE)
            }
        }
    }

    pub fn gen_gather_node(
        &mut self,
        input_node: DistributedPipelineNode,
    ) -> DistributedPipelineNode {
        if input_node.config().clustering_spec.num_partitions() == 1 {
            return input_node;
        }

        GatherNode::new(
            self.get_next_pipeline_node_id(),
            &self.plan_config,
            input_node.config().schema.clone(),
            input_node,
        )
        .into_node()
    }
}
