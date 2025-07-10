use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};

use super::OptimizerRule;
use crate::{
    ops::{Slice as LogicalSlice, Source},
    source_info::SourceInfo,
    LogicalPlan,
};

// TODO add docs by zhenchao 2025-07-11 12:24:12
/// Optimization rules for pushing Limits further into the logical plan.
#[derive(Default, Debug)]
pub struct PushDownSlice {}

impl PushDownSlice {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownSlice {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| self.try_optimize_node(node))
    }
}

impl PushDownSlice {
    #[allow(clippy::only_used_in_recursion)]
    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::Slice(LogicalSlice {
                input,
                limit: Some(limit),
                ..
            }) => {
                let limit = *limit as usize;
                match input.as_ref() {
                    // Push slice into source as a "local" limit.
                    //
                    // Slice-Source -> Slice-Source[with_limit]
                    LogicalPlan::Source(source) => {
                        match source.source_info.as_ref() {
                            // Limit pushdown is not supported for in-memory sources.
                            SourceInfo::InMemory(_) => Ok(Transformed::no(plan)),
                            // Do not pushdown if Source node is already more limited than `limit`
                            SourceInfo::Physical(external_info)
                                if let Some(existing_limit) = external_info.pushdowns.limit
                                    && existing_limit <= limit =>
                            {
                                Ok(Transformed::no(plan))
                            }
                            // Pushdown limit into the Source node as a "local" limit
                            SourceInfo::Physical(external_info) => {
                                let new_pushdowns = external_info.pushdowns.with_limit(Some(limit));
                                let new_external_info = external_info.with_pushdowns(new_pushdowns);
                                let new_source = LogicalPlan::Source(Source::new(
                                    source.output_schema.clone(),
                                    SourceInfo::Physical(new_external_info).into(),
                                ))
                                .into();
                                let out_plan = if external_info
                                    .scan_state
                                    .get_scan_op()
                                    .0
                                    .can_absorb_limit()
                                {
                                    new_source
                                } else {
                                    plan.with_new_children(&[new_source]).into()
                                };
                                Ok(Transformed::yes(out_plan))
                            }
                            SourceInfo::PlaceHolder(..) => {
                                panic!("PlaceHolderInfo should not exist for optimization!");
                            }
                        }
                    }
                    _ => Ok(Transformed::no(plan)),
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

#[cfg(test)]
mod tests {}
