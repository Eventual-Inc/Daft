use std::sync::Arc;

use daft_common::error::DaftResult;
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_schema::schema::SchemaRef;

use crate::pipeline_node::{
    DistributedPipelineNode,
    shuffles::{
        backends::{DistributedShuffleBackend, FlightShuffleBackendConfig},
        gather::GatherNode,
        pre_shuffle_merge::PreShuffleMergeNode,
        repartition::RepartitionNode,
    },
    translate::LogicalPlanToPipelineNodeTranslator,
};

impl LogicalPlanToPipelineNodeTranslator {
    /// Pick the shuffle backend implied by the current execution config.
    pub(crate) fn select_backend(&self) -> DistributedShuffleBackend {
        if self.plan_config.config.shuffle_algorithm.as_str() == "flight_shuffle" {
            DistributedShuffleBackend::Flight(FlightShuffleBackendConfig {
                shuffle_dirs: self.plan_config.config.flight_shuffle_dirs.clone(),
                ..Default::default()
            })
        } else {
            DistributedShuffleBackend::Ray
        }
    }

    pub fn gen_repartition_node(
        &mut self,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> DaftResult<DistributedPipelineNode> {
        let backend = self.select_backend();
        self.gen_repartition_node_with_backend(repartition_spec, schema, child, backend)
    }

    pub fn gen_repartition_node_with_backend(
        &mut self,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        child: DistributedPipelineNode,
        backend: DistributedShuffleBackend,
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
        };

        let use_pre_shuffle_merge = self.should_use_pre_shuffle_merge(&child, num_partitions)?;

        let child_node = if use_pre_shuffle_merge {
            // Create merge node first
            DistributedPipelineNode::new(
                Arc::new(PreShuffleMergeNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    self.plan_config.config.pre_shuffle_merge_threshold,
                    schema.clone(),
                    child,
                )),
                &self.meter,
            )
        } else {
            child
        };

        Ok(DistributedPipelineNode::new(
            Arc::new(RepartitionNode::new(
                self.get_next_pipeline_node_id(),
                &self.plan_config,
                repartition_spec,
                schema,
                num_partitions,
                backend,
                child_node,
            )),
            &self.meter,
        ))
    }

    /// Determine if we should use pre-shuffle merge strategy
    fn should_use_pre_shuffle_merge(
        &self,
        child: &DistributedPipelineNode,
        target_num_partitions: usize,
    ) -> DaftResult<bool> {
        let input_num_partitions = child.config().clustering_spec.num_partitions();

        match self.plan_config.config.shuffle_algorithm.as_str() {
            "pre_shuffle_merge" => Ok(true),
            "map_reduce" => Ok(false),
            "flight_shuffle" => Ok(false), // Flight shuffle will be handled separately
            "auto" => {
                let total_num_partitions = input_num_partitions * target_num_partitions;
                let geometric_mean = (total_num_partitions as f64).sqrt() as usize;
                Ok(geometric_mean
                    > self
                        .plan_config
                        .config
                        .pre_shuffle_merge_partition_threshold)
            }
            _ => Ok(false), // Default to naive map_reduce for unknown strategies
        }
    }

    pub fn gen_gather_node(
        &mut self,
        input_node: DistributedPipelineNode,
    ) -> DistributedPipelineNode {
        if input_node.config().clustering_spec.num_partitions() == 1 {
            return input_node;
        }

        let backend = self.select_backend();

        let node_id = self.get_next_pipeline_node_id();
        DistributedPipelineNode::new(
            Arc::new(GatherNode::new(
                node_id,
                &self.plan_config,
                input_node.config().schema.clone(),
                backend,
                input_node,
            )),
            &self.meter,
        )
    }
}
