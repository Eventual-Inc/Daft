use std::{cmp::max, sync::Arc};

use common_error::DaftResult;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::ops::AsofJoin;
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
        let num_left_partitions = left.config().clustering_spec.num_partitions();
        let num_right_partitions = right.config().clustering_spec.num_partitions();

        let (num_partitions, left, right) = if left_by.is_empty() {
            // No by keys: gather everything into a single partition
            let left = self.gen_gather_node(left);
            let right = self.gen_gather_node(right);
            (1, left, right)
        } else {
            // AsofJoinNode handles all repartitioning internally via range partitioning
            let num_partitions = max(num_left_partitions, num_right_partitions);
            (num_partitions, left, right)
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
