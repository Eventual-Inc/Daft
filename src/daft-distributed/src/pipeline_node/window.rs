use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_dsl::{
    WindowFrame,
    expr::bound_expr::{BoundAggExpr, BoundExpr, BoundWindowExpr},
    window_to_agg_exprs,
};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{partitioning::HashClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;
use itertools::Itertools;

use super::{PipelineNodeImpl, SubmittableTaskStream};
use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
    },
    plan::{PlanConfig, PlanExecutionContext},
};

pub(crate) struct WindowNodeBase {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    aliases: Vec<String>,
    child: DistributedPipelineNode,
}

impl WindowNodeBase {
    fn new(
        config: PipelineNodeConfig,
        context: PipelineNodeContext,
        aliases: Vec<String>,
        child: DistributedPipelineNode,
    ) -> Self {
        Self {
            config,
            context,
            aliases,
            child,
        }
    }
}

pub(crate) struct WindowNodePartitionOnly {
    base: WindowNodeBase,
    partition_by: Vec<BoundExpr>,
    agg_exprs: Vec<BoundAggExpr>,
}

impl WindowNodePartitionOnly {
    fn produce_task(&self, input: LocalPhysicalPlanRef) -> LocalPhysicalPlanRef {
        LocalPhysicalPlan::window_partition_only(
            input,
            self.partition_by.clone(),
            self.base.config.schema.clone(),
            StatsState::NotMaterialized,
            self.agg_exprs.clone(),
            self.base.aliases.clone(),
            LocalNodeContext {
                origin_node_id: Some(self.base.context.node_id as usize),
                additional: None,
            },
        )
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            format!(
                "Partition by: {}",
                self.partition_by.iter().map(|e| e.to_string()).join(", ")
            ),
            format!(
                "Agg exprs: {}",
                self.agg_exprs.iter().map(|e| e.to_string()).join(", ")
            ),
        ]
    }
}

pub(crate) struct WindowNodePartitionAndOrderBy {
    base: WindowNodeBase,
    partition_by: Vec<BoundExpr>,
    order_by: Vec<BoundExpr>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    window_exprs: Vec<BoundWindowExpr>,
}

impl WindowNodePartitionAndOrderBy {
    fn produce_task(&self, input: LocalPhysicalPlanRef) -> LocalPhysicalPlanRef {
        LocalPhysicalPlan::window_partition_and_order_by(
            input,
            self.partition_by.clone(),
            self.order_by.clone(),
            self.descending.clone(),
            self.nulls_first.clone(),
            self.base.config.schema.clone(),
            StatsState::NotMaterialized,
            self.window_exprs.clone(),
            self.base.aliases.clone(),
            LocalNodeContext {
                origin_node_id: Some(self.base.context.node_id as usize),
                additional: None,
            },
        )
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            format!(
                "Partition by: {}",
                self.partition_by.iter().map(|e| e.to_string()).join(", ")
            ),
            format!(
                "Order by: {}",
                self.order_by.iter().map(|e| e.to_string()).join(", ")
            ),
            format!(
                "Descending: {}",
                self.descending.iter().map(|e| e.to_string()).join(", ")
            ),
            format!(
                "Nulls first: {}",
                self.nulls_first.iter().map(|e| e.to_string()).join(", ")
            ),
        ]
    }
}

pub(crate) struct WindowNodePartitionAndDynamicFrame {
    base: WindowNodeBase,
    partition_by: Vec<BoundExpr>,
    order_by: Vec<BoundExpr>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    frame: WindowFrame,
    min_periods: usize,
    agg_exprs: Vec<BoundAggExpr>,
}

impl WindowNodePartitionAndDynamicFrame {
    fn produce_task(&self, input: LocalPhysicalPlanRef) -> LocalPhysicalPlanRef {
        LocalPhysicalPlan::window_partition_and_dynamic_frame(
            input,
            self.partition_by.clone(),
            self.order_by.clone(),
            self.descending.clone(),
            self.nulls_first.clone(),
            self.frame.clone(),
            self.min_periods,
            self.base.config.schema.clone(),
            StatsState::NotMaterialized,
            self.agg_exprs.clone(),
            self.base.aliases.clone(),
            LocalNodeContext {
                origin_node_id: Some(self.base.context.node_id as usize),
                additional: None,
            },
        )
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            format!(
                "Partition by: {}",
                self.partition_by.iter().map(|e| e.to_string()).join(", ")
            ),
            format!(
                "Order by: {}",
                self.order_by.iter().map(|e| e.to_string()).join(", ")
            ),
            format!(
                "Descending: {}",
                self.descending.iter().map(|e| e.to_string()).join(", ")
            ),
            format!(
                "Nulls first: {}",
                self.nulls_first.iter().map(|e| e.to_string()).join(", ")
            ),
            format!("Frame: {:?}", self.frame),
            format!("Min periods: {}", self.min_periods),
            format!(
                "Agg exprs: {}",
                self.agg_exprs.iter().map(|e| e.to_string()).join(", ")
            ),
        ]
    }
}

pub(crate) struct WindowNodeOrderByOnly {
    base: WindowNodeBase,
    order_by: Vec<BoundExpr>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    window_exprs: Vec<BoundWindowExpr>,
}

impl WindowNodeOrderByOnly {
    fn produce_task(&self, input: LocalPhysicalPlanRef) -> LocalPhysicalPlanRef {
        LocalPhysicalPlan::window_order_by_only(
            input,
            self.order_by.clone(),
            self.descending.clone(),
            self.nulls_first.clone(),
            self.base.config.schema.clone(),
            StatsState::NotMaterialized,
            self.window_exprs.clone(),
            self.base.aliases.clone(),
            LocalNodeContext {
                origin_node_id: Some(self.base.context.node_id as usize),
                additional: None,
            },
        )
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            format!(
                "Order by: {}",
                self.order_by.iter().map(|e| e.to_string()).join(", ")
            ),
            format!(
                "Descending: {}",
                self.descending.iter().map(|e| e.to_string()).join(", ")
            ),
            format!(
                "Nulls first: {}",
                self.nulls_first.iter().map(|e| e.to_string()).join(", ")
            ),
            format!(
                "Window exprs: {}",
                self.window_exprs.iter().map(|e| e.to_string()).join(", ")
            ),
        ]
    }
}

pub(crate) enum WindowNode {
    PartitionOnly(WindowNodePartitionOnly),
    PartitionAndOrderBy(WindowNodePartitionAndOrderBy),
    PartitionAndDynamicFrame(WindowNodePartitionAndDynamicFrame),
    OrderByOnly(WindowNodeOrderByOnly),
}

impl WindowNode {
    const NODE_NAME: NodeName = "Window";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        partition_by: Vec<BoundExpr>,
        order_by: Vec<BoundExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        frame: Option<WindowFrame>,
        min_periods: usize,
        window_exprs: Vec<BoundWindowExpr>,
        aliases: Vec<String>,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> DaftResult<Self> {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            Arc::new(
                HashClusteringConfig::new(
                    child.config().clustering_spec.num_partitions(),
                    partition_by.clone().into_iter().map(|e| e.into()).collect(),
                )
                .into(),
            ),
        );

        let base = WindowNodeBase::new(config, context, aliases, child);

        let has_partition_by = !partition_by.is_empty();
        let has_order_by = !order_by.is_empty();
        let has_frame = frame.is_some();

        match (has_partition_by, has_order_by, has_frame) {
            (true, false, false) => {
                let agg_exprs = window_to_agg_exprs(window_exprs)?;
                Ok(Self::PartitionOnly(WindowNodePartitionOnly {
                    base,
                    partition_by,
                    agg_exprs,
                }))
            }
            (true, true, false) => Ok(Self::PartitionAndOrderBy(WindowNodePartitionAndOrderBy {
                base,
                partition_by,
                order_by,
                descending,
                nulls_first,
                window_exprs,
            })),
            (true, true, true) => {
                let agg_exprs = window_to_agg_exprs(window_exprs)?;
                Ok(Self::PartitionAndDynamicFrame(
                    WindowNodePartitionAndDynamicFrame {
                        base,
                        partition_by,
                        order_by,
                        descending,
                        nulls_first,
                        frame: frame.unwrap(),
                        min_periods,
                        agg_exprs,
                    },
                ))
            }
            (false, true, false) => Ok(Self::OrderByOnly(WindowNodeOrderByOnly {
                base,
                order_by,
                descending,
                nulls_first,
                window_exprs,
            })),
            (false, true, true) => Err(DaftError::not_implemented(
                "Window with order by and frame not yet implemented",
            )),
            _ => Err(DaftError::ValueError(
                "Window requires either partition by or order by".to_string(),
            )),
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    fn base(&self) -> &WindowNodeBase {
        match self {
            Self::PartitionOnly(node) => &node.base,
            Self::PartitionAndOrderBy(node) => &node.base,
            Self::PartitionAndDynamicFrame(node) => &node.base,
            Self::OrderByOnly(node) => &node.base,
        }
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.base().config
    }

    fn context(&self) -> &PipelineNodeContext {
        &self.base().context
    }

    fn child(&self) -> &DistributedPipelineNode {
        &self.base().child
    }
}

impl PipelineNodeImpl for WindowNode {
    fn context(&self) -> &PipelineNodeContext {
        self.context()
    }

    fn config(&self) -> &PipelineNodeConfig {
        self.config()
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child().clone()]
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let mut res = vec!["Window:".to_string()];
        match self {
            Self::PartitionOnly(node) => {
                res.extend(node.multiline_display());
            }
            Self::PartitionAndOrderBy(node) => {
                res.extend(node.multiline_display());
            }
            Self::PartitionAndDynamicFrame(node) => {
                res.extend(node.multiline_display());
            }
            Self::OrderByOnly(node) => {
                res.extend(node.multiline_display());
            }
        }
        res
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child().clone().produce_tasks(plan_context);

        // Pipeline the window op
        let self_clone = self.clone();
        input_node.pipeline_instruction(self, move |input| match &*self_clone {
            Self::PartitionOnly(node) => node.produce_task(input),
            Self::PartitionAndOrderBy(node) => node.produce_task(input),
            Self::PartitionAndDynamicFrame(node) => node.produce_task(input),
            Self::OrderByOnly(node) => node.produce_task(input),
        })
    }
}
