use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_treenode::{DynTreeNode, Transformed, TreeNode};

use super::OptimizerRule;
use crate::{
    ops::{Shard, Source},
    source_info::SourceInfo,
    LogicalPlan,
};

/// Optimization rules for pushing Limits further into the logical plan.
#[derive(Default, Debug)]
pub struct PushDownShard {}

impl PushDownShard {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownShard {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| self.try_optimize_node(node))
    }
}

impl PushDownShard {
    #[allow(clippy::only_used_in_recursion)]
    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::Shard(Shard { input, sharder, .. }) => {
                match input.as_ref() {
                    // Naive commuting with unary ops.
                    //
                    // Shard-UnaryOp -> UnaryOp-Shard
                    LogicalPlan::Project(_)
                    | LogicalPlan::ActorPoolProject(_)
                    | LogicalPlan::Filter(_)
                    | LogicalPlan::Limit(_)
                    | LogicalPlan::Explode(_)
                    | LogicalPlan::Unpivot(_)
                    | LogicalPlan::Sort(_)
                    | LogicalPlan::Repartition(_)
                    | LogicalPlan::Distinct(_)
                    | LogicalPlan::Aggregate(_)
                    | LogicalPlan::Pivot(_)
                    | LogicalPlan::Sink(_)
                    | LogicalPlan::Sample(_)
                    | LogicalPlan::MonotonicallyIncreasingId(_)
                    | LogicalPlan::Window(_)
                    | LogicalPlan::TopN(_) => {
                        let new_shard = plan
                            .with_new_children(&[input.arc_children()[0].clone()])
                            .into();
                        Ok(Transformed::yes(
                            input.with_new_children(&[new_shard]).into(),
                        ))
                    }
                    // Push shard into source.
                    //
                    // Shard-Source -> Source[with_shard]
                    LogicalPlan::Source(source) => {
                        match source.source_info.as_ref() {
                            // Shard pushdown is not supported for in-memory sources.
                            SourceInfo::InMemory(_) => Err(DaftError::ValueError(
                                "Sharding is not supported for in-memory sources".to_string(),
                            )),
                            // If there are multiple shards, throw an error.
                            SourceInfo::Physical(external_info)
                                if external_info.pushdowns.sharder.is_some() =>
                            {
                                Err(DaftError::ValueError(
                                    "One source cannot have multiple shards".to_string(),
                                ))
                            }
                            // Pushdown shard into the Source node.
                            SourceInfo::Physical(external_info) => {
                                let new_pushdowns =
                                    external_info.pushdowns.with_sharder(Some(sharder.clone()));
                                let new_external_info = external_info.with_pushdowns(new_pushdowns);
                                let new_source = LogicalPlan::Source(Source::new(
                                    source.output_schema.clone(),
                                    SourceInfo::Physical(new_external_info).into(),
                                ))
                                .into();
                                Ok(Transformed::yes(new_source))
                            }
                            SourceInfo::PlaceHolder(..) => {
                                panic!("PlaceHolderInfo should not exist for optimization!");
                            }
                        }
                    }
                    // Shards cannot be folded together.
                    LogicalPlan::Shard(_) => Err(DaftError::ValueError(
                        "Shards cannot be folded together".to_string(),
                    )),
                    op => Err(DaftError::ValueError(format!(
                        "Shard cannot exist above the non-unary {} operator",
                        op.name()
                    ))),
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}
