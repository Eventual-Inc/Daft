use std::sync::Arc;

use common_error::DaftResult;
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
    ///
    /// `input_size_hint` is the estimated input-bytes flowing into the shuffle, from
    /// logical-plan stats. When the algorithm is `"auto"` (and the upstream stats are
    /// materialized), this routes large shuffles to Flight (which controls disk spill
    /// directly) and small shuffles to Ray-plasma (which keeps things in-memory and
    /// avoids the per-task disk-write tax). Explicit `"flight"` / `"ray"` settings
    /// force their respective backends regardless of size.
    pub(crate) fn select_backend(
        &self,
        input_size_hint: Option<usize>,
    ) -> DistributedShuffleBackend {
        let flight = || {
            let compression = match self.plan_config.config.flight_shuffle_compression.as_str() {
                "none" => None,
                other => Some(other.to_string()),
            };
            DistributedShuffleBackend::Flight(FlightShuffleBackendConfig {
                shuffle_dirs: self.plan_config.config.flight_shuffle_dirs.clone(),
                compression,
                ..Default::default()
            })
        };
        match self.plan_config.config.shuffle_algorithm.as_str() {
            "flight" => flight(),
            "auto" => match input_size_hint {
                Some(bytes)
                    if bytes
                        >= self
                            .plan_config
                            .config
                            .flight_shuffle_size_threshold_bytes =>
                {
                    flight()
                }
                _ => DistributedShuffleBackend::Ray,
            },
            // "ray" or anything else: keep Ray.
            _ => DistributedShuffleBackend::Ray,
        }
    }

    pub fn gen_repartition_node(
        &mut self,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        child: DistributedPipelineNode,
        input_size_hint: Option<usize>,
    ) -> DaftResult<DistributedPipelineNode> {
        let backend = self.select_backend(input_size_hint);
        self.gen_repartition_node_with_backend(
            repartition_spec,
            schema,
            child,
            backend,
            input_size_hint,
        )
    }

    pub fn gen_repartition_node_with_backend(
        &mut self,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        child: DistributedPipelineNode,
        backend: DistributedShuffleBackend,
        input_size_hint: Option<usize>,
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

        let use_pre_shuffle_merge =
            self.should_use_pre_shuffle_merge(&child, num_partitions, input_size_hint)?;

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

    /// Decide whether to insert a Pre-Shuffle Merge stage. Driven by the
    /// `pre_shuffle_merge` config, independent of `shuffle_algorithm` — pre-merge
    /// works for both Ray and Flight backends:
    /// - `"always"` / `"never"` force the answer
    /// - `"auto"` skips pre-merge when input partitions are already large enough
    ///   that the consolidation ratio (inputs-per-merge-bucket) would be near 1 —
    ///   in that regime the materialize barrier + hard worker affinity of
    ///   `PreShuffleMergeNode` is pure overhead. Otherwise applies the
    ///   geometric-mean heuristic over (input × target) partitions.
    fn should_use_pre_shuffle_merge(
        &self,
        child: &DistributedPipelineNode,
        target_num_partitions: usize,
        input_size_hint: Option<usize>,
    ) -> DaftResult<bool> {
        match self.plan_config.config.pre_shuffle_merge.as_str() {
            "always" => Ok(true),
            "never" => Ok(false),
            "auto" => {
                let input_num_partitions = child.config().clustering_spec.num_partitions();
                // Size-based gate: if avg input partition is already ≥ 1/4 of the
                // merge bucket size, pre-merge would pack <4 inputs per task — too
                // little consolidation to outweigh the barrier + affinity cost.
                // Pure-large-shuffle workloads (e.g. SF100 lineitem repartition)
                // sit in this regime; TPC-H intermediate stages with small
                // post-join partitions do not.
                let threshold = self.plan_config.config.pre_shuffle_merge_threshold;
                if let Some(total_bytes) = input_size_hint
                    && input_num_partitions > 0
                {
                    let avg_partition_size = total_bytes / input_num_partitions;
                    if avg_partition_size >= threshold / 4 {
                        return Ok(false);
                    }
                }
                let total_num_partitions = input_num_partitions * target_num_partitions;
                let geometric_mean = (total_num_partitions as f64).sqrt() as usize;
                Ok(geometric_mean
                    > self
                        .plan_config
                        .config
                        .pre_shuffle_merge_partition_threshold)
            }
            _ => Ok(false),
        }
    }

    pub fn gen_gather_node(
        &mut self,
        input_node: DistributedPipelineNode,
    ) -> DistributedPipelineNode {
        if input_node.config().clustering_spec.num_partitions() == 1 {
            return input_node;
        }

        // Gathers funnel into a single partition; routing them through Flight rarely
        // helps, so don't pass a size hint. select_backend will pick Ray for "auto".
        let backend = self.select_backend(None);

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
