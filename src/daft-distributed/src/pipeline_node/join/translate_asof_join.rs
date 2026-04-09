use std::{cmp::max, sync::Arc};

use common_error::DaftResult;
use daft_dsl::{expr::bound_expr::BoundExpr, is_partition_compatible};
use daft_logical_plan::{
    ClusteringSpec,
    ops::AsofJoin,
    partitioning::{HashRepartitionConfig, RepartitionSpec},
};
use daft_schema::schema::SchemaRef;

use crate::pipeline_node::{
    DistributedPipelineNode, join::AsofJoinNode, translate::LogicalPlanToPipelineNodeTranslator,
};

impl LogicalPlanToPipelineNodeTranslator {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn gen_asof_join_nodes(
        &mut self,
        left: DistributedPipelineNode,
        right: DistributedPipelineNode,
        left_by: Vec<BoundExpr>,
        right_by: Vec<BoundExpr>,
        left_on: BoundExpr,
        right_on: BoundExpr,
        output_schema: SchemaRef,
    ) -> DaftResult<DistributedPipelineNode> {
        let left_spec = left.config().clustering_spec.as_ref();
        let right_spec = right.config().clustering_spec.as_ref();

        let is_left_hash_partitioned = !left_by.is_empty()
            && matches!(left_spec, ClusteringSpec::Hash(..))
            && is_partition_compatible(
                &left_spec.partition_by(),
                left_by.iter().map(|e| e.inner()),
            );
        let is_right_hash_partitioned = !right_by.is_empty()
            && matches!(right_spec, ClusteringSpec::Hash(..))
            && is_partition_compatible(
                &right_spec.partition_by(),
                right_by.iter().map(|e| e.inner()),
            );
        let num_left_partitions = left_spec.num_partitions();
        let num_right_partitions = right_spec.num_partitions();

        let num_partitions = if left_by.is_empty() {
            // No by keys: 1 partition only
            1
        } else {
            match (
                is_left_hash_partitioned,
                is_right_hash_partitioned,
                num_left_partitions,
                num_right_partitions,
            ) {
                (true, true, a, b) | (false, false, a, b) => max(a, b),
                (_, _, 1, x) | (_, _, x, 1) => x,
                (true, false, a, b)
                    if (a as f64)
                        >= (b as f64)
                            * self.plan_config.config.hash_join_partition_size_leniency =>
                {
                    a
                }
                (false, true, a, b)
                    if (b as f64)
                        >= (a as f64)
                            * self.plan_config.config.hash_join_partition_size_leniency =>
                {
                    b
                }
                (_, _, a, b) => max(a, b),
            }
        };

        let left = if left_by.is_empty() {
            // No by keys: gather everything into a single partition
            self.gen_gather_node(left)
        } else if num_left_partitions != num_partitions || !is_left_hash_partitioned {
            self.gen_repartition_node(
                RepartitionSpec::Hash(HashRepartitionConfig::new(
                    Some(num_partitions),
                    left_by.iter().map(|e| e.clone().into()).collect(),
                )),
                left.config().schema.clone(),
                left,
            )?
        } else {
            left
        };

        let right = if right_by.is_empty() {
            // No by keys: gather everything into a single partition
            self.gen_gather_node(right)
        } else if num_right_partitions != num_partitions || !is_right_hash_partitioned {
            self.gen_repartition_node(
                RepartitionSpec::Hash(HashRepartitionConfig::new(
                    Some(num_partitions),
                    right_by.iter().map(|e| e.clone().into()).collect(),
                )),
                right.config().schema.clone(),
                right,
            )?
        } else {
            right
        };

        let node_id = self.get_next_pipeline_node_id();
        Ok(DistributedPipelineNode::new(
            Arc::new(AsofJoinNode::new(
                node_id,
                &self.plan_config,
                left_by,
                right_by,
                left_on,
                right_on,
                num_partitions,
                left,
                right,
                output_schema,
            )),
            &self.meter,
        ))
    }

    pub(crate) fn translate_asof_join(
        &mut self,
        asof_join: &AsofJoin,
        left_node: DistributedPipelineNode,
        right_node: DistributedPipelineNode,
    ) -> DaftResult<DistributedPipelineNode> {
        // Normalize the by
        let (left_by, right_by) = daft_dsl::join::normalize_join_keys(
            asof_join.left_by.clone(),
            asof_join.right_by.clone(),
            left_node.config().schema.clone(),
            right_node.config().schema.clone(),
        )?;

        // Bind keys to schemas
        let left_by = BoundExpr::bind_all(&left_by, &left_node.config().schema)?;
        let right_by = BoundExpr::bind_all(&right_by, &right_node.config().schema)?;
        let left_on = BoundExpr::try_new(asof_join.left_on.clone(), &left_node.config().schema)?;
        let right_on = BoundExpr::try_new(asof_join.right_on.clone(), &right_node.config().schema)?;

        self.gen_asof_join_nodes(
            left_node,
            right_node,
            left_by,
            right_by,
            left_on,
            right_on,
            asof_join.output_schema.clone(),
        )
    }
}
