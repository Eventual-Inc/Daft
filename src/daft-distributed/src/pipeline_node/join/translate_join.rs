use std::cmp::max;

use common_error::DaftResult;
use daft_dsl::{ExprRef, expr::bound_expr::BoundExpr, is_partition_compatible};
use daft_logical_plan::{
    ClusteringSpec, JoinStrategy, JoinType,
    ops::Join,
    partitioning::{HashRepartitionConfig, RepartitionSpec},
    stats::ApproxStats,
};
use daft_schema::schema::SchemaRef;

use crate::pipeline_node::{
    DistributedPipelineNode,
    join::{BroadcastJoinNode, CrossJoinNode, HashJoinNode, SortMergeJoinNode},
    translate::LogicalPlanToPipelineNodeTranslator,
};

impl LogicalPlanToPipelineNodeTranslator {
    pub(crate) fn determine_join_strategy(
        &self,
        left_on: &[ExprRef],
        right_on: &[ExprRef],
        join_type: &JoinType,
        join_strategy: Option<JoinStrategy>,
        left_stats: &ApproxStats,
        right_stats: &ApproxStats,
    ) -> JoinStrategy {
        // If join strategy is explicitly specified, use it
        if let Some(strategy) = join_strategy {
            return strategy;
        }

        // Check for cross join
        if left_on.is_empty() && right_on.is_empty() && *join_type == JoinType::Inner {
            return JoinStrategy::Cross;
        }

        // Determine the smaller side that will be broadcasted
        let (smaller_size_bytes, left_is_larger) = if right_stats.size_bytes < left_stats.size_bytes
        {
            (right_stats.size_bytes, true)
        } else {
            (left_stats.size_bytes, false)
        };

        let smaller_side_is_broadcastable = match join_type {
            JoinType::Inner => true,
            JoinType::Left | JoinType::Anti | JoinType::Semi => left_is_larger,
            JoinType::Right => !left_is_larger,
            JoinType::Outer => false,
        };

        // If the smaller table is under broadcast size threshold AND we are not broadcasting the side we are outer joining by, use broadcast join
        if smaller_size_bytes <= self.plan_config.config.broadcast_join_size_bytes_threshold
            && smaller_side_is_broadcastable
        {
            JoinStrategy::Broadcast
        // Otherwise, use a hash join
        } else {
            JoinStrategy::Hash
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn gen_hash_join_nodes(
        &mut self,
        left: DistributedPipelineNode,
        right: DistributedPipelineNode,
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        null_equals_nulls: Vec<bool>,
        join_type: JoinType,
        output_schema: SchemaRef,
    ) -> DaftResult<DistributedPipelineNode> {
        let left_spec = left.config().clustering_spec.as_ref();
        let right_spec = right.config().clustering_spec.as_ref();

        let is_left_hash_partitioned = matches!(left_spec, ClusteringSpec::Hash(..))
            && is_partition_compatible(
                &left_spec.partition_by(),
                left_on.iter().map(|e| e.inner()),
            );
        let is_right_hash_partitioned = matches!(right_spec, ClusteringSpec::Hash(..))
            && is_partition_compatible(
                &right_spec.partition_by(),
                right_on.iter().map(|e| e.inner()),
            );
        let num_left_partitions = left_spec.num_partitions();
        let num_right_partitions = right_spec.num_partitions();

        let num_partitions = match (
            is_left_hash_partitioned,
            is_right_hash_partitioned,
            num_left_partitions,
            num_right_partitions,
        ) {
            (true, true, a, b) | (false, false, a, b) => max(a, b),
            (_, _, 1, x) | (_, _, x, 1) => x,
            (true, false, a, b)
                if (a as f64)
                    >= (b as f64) * self.plan_config.config.hash_join_partition_size_leniency =>
            {
                a
            }
            (false, true, a, b)
                if (b as f64)
                    >= (a as f64) * self.plan_config.config.hash_join_partition_size_leniency =>
            {
                b
            }
            (_, _, a, b) => max(a, b),
        };

        let left = if num_left_partitions != num_partitions
            || (num_partitions > 1 && !is_left_hash_partitioned)
        {
            self.gen_shuffle_node(
                RepartitionSpec::Hash(HashRepartitionConfig::new(
                    Some(num_partitions),
                    left_on.iter().map(|e| e.clone().into()).collect(),
                )),
                left.config().schema.clone(),
                left,
            )?
        } else {
            left
        };

        let right = if num_right_partitions != num_partitions
            || (num_partitions > 1 && !is_right_hash_partitioned)
        {
            self.gen_shuffle_node(
                RepartitionSpec::Hash(HashRepartitionConfig::new(
                    Some(num_partitions),
                    right_on.iter().map(|e| e.clone().into()).collect(),
                )),
                right.config().schema.clone(),
                right,
            )?
        } else {
            right
        };

        Ok(HashJoinNode::new(
            self.get_next_pipeline_node_id(),
            &self.plan_config,
            left_on,
            right_on,
            Some(null_equals_nulls),
            join_type,
            num_partitions,
            left,
            right,
            output_schema,
        )
        .into_node())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn gen_broadcast_join_node(
        &mut self,
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        null_equals_nulls: Vec<bool>,
        join_type: JoinType,
        left_node: DistributedPipelineNode,
        right_node: DistributedPipelineNode,
        left_stats: &ApproxStats,
        right_stats: &ApproxStats,
        output_schema: SchemaRef,
    ) -> DaftResult<DistributedPipelineNode> {
        // Calculate which side is larger for broadcast join logic
        let left_is_larger = right_stats.size_bytes < left_stats.size_bytes;

        // Determine if we need to swap the sides based on join type and size
        let is_swapped = match (join_type, left_is_larger) {
            (JoinType::Left, _) => true,
            (JoinType::Right, _) => false,
            (JoinType::Inner, left_is_larger) => left_is_larger,
            (JoinType::Outer, _) => {
                return Err(common_error::DaftError::ValueError(
                    "Broadcast join does not support outer joins.".to_string(),
                ));
            }
            (JoinType::Anti, _) => true,
            (JoinType::Semi, _) => true,
        };

        let (broadcaster, receiver) = if is_swapped {
            (right_node, left_node)
        } else {
            (left_node, right_node)
        };

        // Create broadcast join node
        Ok(BroadcastJoinNode::new(
            self.get_next_pipeline_node_id(),
            &self.plan_config,
            left_on,
            right_on,
            Some(null_equals_nulls),
            join_type,
            is_swapped,
            broadcaster,
            receiver,
            output_schema,
        )
        .into_node())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn gen_sort_merge_join_node(
        &mut self,
        left: DistributedPipelineNode,
        right: DistributedPipelineNode,
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        join_type: JoinType,
        output_schema: SchemaRef,
    ) -> DaftResult<DistributedPipelineNode> {
        let num_partitions = {
            let left_num_partitions = left.config().clustering_spec.num_partitions();
            let right_num_partitions = right.config().clustering_spec.num_partitions();
            max(left_num_partitions, right_num_partitions)
        };

        Ok(SortMergeJoinNode::new(
            self.get_next_pipeline_node_id(),
            &self.plan_config,
            left_on,
            right_on,
            join_type,
            num_partitions,
            left,
            right,
            output_schema,
        )
        .into_node())
    }

    pub(crate) fn gen_cross_join_node(
        &mut self,
        left_node: DistributedPipelineNode,
        right_node: DistributedPipelineNode,
        output_schema: SchemaRef,
    ) -> DaftResult<DistributedPipelineNode> {
        let num_partitions = {
            let left_num_partitions = left_node.config().clustering_spec.num_partitions();
            let right_num_partitions = right_node.config().clustering_spec.num_partitions();
            left_num_partitions * right_num_partitions
        };

        Ok(CrossJoinNode::new(
            self.get_next_pipeline_node_id(),
            &self.plan_config,
            num_partitions,
            left_node,
            right_node,
            output_schema,
        )
        .into_node())
    }

    pub(crate) fn translate_join(
        &mut self,
        join: &Join,
        left_node: DistributedPipelineNode,
        right_node: DistributedPipelineNode,
    ) -> DaftResult<DistributedPipelineNode> {
        let (remaining_on, left_on, right_on, null_equals_nulls) = join.on.split_eq_preds();
        if !remaining_on.is_empty() {
            todo!("FLOTILLA_MS?: Implement non-equality joins")
        }

        // Normalize join keys
        let (left_on, right_on) = daft_dsl::join::normalize_join_keys(
            left_on,
            right_on,
            left_node.config().schema.clone(),
            right_node.config().schema.clone(),
        )?;

        // Get stats from the join logical plan's left and right children
        let left_stats = join.left.materialized_stats().approx_stats.clone();
        let right_stats = join.right.materialized_stats().approx_stats.clone();

        // Determine join strategy
        let join_strategy = self.determine_join_strategy(
            &left_on,
            &right_on,
            &join.join_type,
            join.join_strategy,
            &left_stats,
            &right_stats,
        );

        // Bind join keys to schemas
        let left_on = BoundExpr::bind_all(&left_on, &left_node.config().schema)?;
        let right_on = BoundExpr::bind_all(&right_on, &right_node.config().schema)?;

        match join_strategy {
            JoinStrategy::Hash => self.gen_hash_join_nodes(
                left_node,
                right_node,
                left_on,
                right_on,
                null_equals_nulls,
                join.join_type,
                join.output_schema.clone(),
            ),
            JoinStrategy::SortMerge => self.gen_sort_merge_join_node(
                left_node,
                right_node,
                left_on,
                right_on,
                join.join_type,
                join.output_schema.clone(),
            ),
            JoinStrategy::Broadcast => self.gen_broadcast_join_node(
                left_on,
                right_on,
                null_equals_nulls,
                join.join_type,
                left_node,
                right_node,
                &left_stats,
                &right_stats,
                join.output_schema.clone(),
            ),
            JoinStrategy::Cross => {
                self.gen_cross_join_node(left_node, right_node, join.output_schema.clone())
            }
        }
    }
}
