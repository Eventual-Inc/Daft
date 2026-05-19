use std::sync::Arc;

use common_display::utils::bytes_to_human_readable;
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

/// Approx size at which we hint the user to switch to flight_shuffle for memory/IO efficiency.
const LARGE_SHUFFLE_SIZE_BYTES_HINT_THRESHOLD: usize = 10 * 1024 * 1024 * 1024; // 10 GiB

/// Approx num of (input × output) partition slots at which we hint the user to switch to
/// flight_shuffle. The map-reduce shuffle materializes one object per (input, output) slot on
/// the head node (~3KB per slot). 500k slots ≈ 1.5GB head-node memory pressure.
const LARGE_SHUFFLE_PARTITION_PRODUCT_HINT_THRESHOLD: usize = 500_000;

/// Approx bytes of head-node memory used per (input × output) partition slot in map-reduce shuffle.
const PARTITION_SLOT_HEAD_MEMORY_BYTES: usize = 3 * 1024;

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
        input_size_bytes: usize,
    ) -> DaftResult<DistributedPipelineNode> {
        let backend = self.select_backend();
        self.gen_repartition_node_with_backend(
            repartition_spec,
            schema,
            child,
            backend,
            input_size_bytes,
        )
    }

    pub fn gen_repartition_node_with_backend(
        &mut self,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        child: DistributedPipelineNode,
        backend: DistributedShuffleBackend,
        input_size_bytes: usize,
    ) -> DaftResult<DistributedPipelineNode> {
        let input_num_partitions = child.config().clustering_spec.num_partitions();
        let num_partitions = match &repartition_spec {
            RepartitionSpec::Hash(config) => config.num_partitions.unwrap_or(input_num_partitions),
            RepartitionSpec::Random(config) => {
                config.num_partitions.unwrap_or(input_num_partitions)
            }
            RepartitionSpec::Range(config) => config.num_partitions.unwrap_or(input_num_partitions),
        };

        self.maybe_warn_large_shuffle(
            &backend,
            input_size_bytes,
            input_num_partitions,
            num_partitions,
        );

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
        input_size_bytes: usize,
    ) -> DistributedPipelineNode {
        let input_num_partitions = input_node.config().clustering_spec.num_partitions();
        if input_num_partitions == 1 {
            return input_node;
        }

        let backend = self.select_backend();

        self.maybe_warn_large_shuffle(&backend, input_size_bytes, input_num_partitions, 1);

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

    /// Emit a one-time-per-query hint to the user when a shuffle looks large enough that flight
    /// shuffle would likely be a better choice than the default map-reduce / pre-shuffle-merge
    /// algorithms. Two independent checks:
    ///
    /// 1. **Shuffle byte size**: total data passing through the shuffle exceeds
    ///    `LARGE_SHUFFLE_SIZE_BYTES_HINT_THRESHOLD`. Map-reduce shuffles read/write this volume
    ///    through Ray's object store; flight shuffle is more efficient for big payloads.
    ///
    /// 2. **Partition slot count**: `input_num_partitions × output_num_partitions` exceeds
    ///    `LARGE_SHUFFLE_PARTITION_PRODUCT_HINT_THRESHOLD`. Map-reduce materializes one object per
    ///    (input, output) slot on the head node (~3KB each), so high partition fan-out causes
    ///    head-node memory pressure regardless of total bytes.
    ///
    /// Skipped if the user is already on flight_shuffle.
    pub(crate) fn maybe_warn_large_shuffle(
        &mut self,
        backend: &DistributedShuffleBackend,
        input_size_bytes: usize,
        input_num_partitions: usize,
        output_num_partitions: usize,
    ) {
        if matches!(backend, DistributedShuffleBackend::Flight(_)) {
            return;
        }

        if !self.warned_large_shuffle_bytes
            && input_size_bytes >= LARGE_SHUFFLE_SIZE_BYTES_HINT_THRESHOLD
        {
            self.warned_large_shuffle_bytes = true;
            let msg = format!(
                "Large shuffle detected (~{} flowing through a `{}` shuffle). \
                 Consider setting `daft.context.set_execution_config(shuffle_algorithm=\"flight_shuffle\")` \
                 for better performance on large shuffles.",
                bytes_to_human_readable(input_size_bytes),
                self.plan_config.config.shuffle_algorithm,
            );
            tracing::warn!("{}", msg);
            self.shuffle_hints.push(msg);
        }

        let partition_product = input_num_partitions.saturating_mul(output_num_partitions);
        if !self.warned_large_shuffle_partition_product
            && partition_product >= LARGE_SHUFFLE_PARTITION_PRODUCT_HINT_THRESHOLD
        {
            self.warned_large_shuffle_partition_product = true;
            let head_memory = partition_product.saturating_mul(PARTITION_SLOT_HEAD_MEMORY_BYTES);
            let msg = format!(
                "High shuffle fan-out detected ({} input × {} output = {} partitions, \
                 ~{} of head-node memory). Map-reduce shuffles allocate one object per \
                 (input, output) partition slot on the head node, which scales poorly. \
                 Consider setting `daft.context.set_execution_config(shuffle_algorithm=\"flight_shuffle\")` \
                 to avoid head-node memory pressure.",
                input_num_partitions,
                output_num_partitions,
                partition_product,
                bytes_to_human_readable(head_memory),
            );
            tracing::warn!("{}", msg);
            self.shuffle_hints.push(msg);
        }
    }
}
