use std::sync::Arc;

use common_error::DaftResult;

use crate::LogicalPlan;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ApplyOrder {
    TopDown,
    #[allow(dead_code)]
    BottomUp,
    #[allow(dead_code)]
    Delegated,
}

// TODO(Clark): Add fixed-point policy if needed.
pub trait OptimizerRule {
    /// Try to optimize the logical plan with this rule.
    ///
    /// This returns Some(new_plan) if the rule modified the plan, None otherwise.
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>>;

    /// The plan tree order in which this rule should be applied (top-down or bottom-up).
    fn apply_order(&self) -> ApplyOrder;
}

pub enum Transformed<T> {
    Yes(T),
    No(T),
}

impl<T> Transformed<T> {
    pub fn or(self, other: Self) -> Self {
        match self {
            Self::Yes(_) => self,
            Self::No(_) => other,
        }
    }

    pub fn is_no(&self) -> bool {
        matches!(self, Self::No(_))
    }

    pub fn unwrap(&self) -> &T {
        match self {
            Self::Yes(inner) => inner,
            Self::No(inner) => inner,
        }
    }
}
