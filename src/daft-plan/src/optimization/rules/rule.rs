use std::sync::Arc;

use common_error::DaftResult;

use crate::LogicalPlan;

/// Application order of a rule or rule batch.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ApplyOrder {
    // Apply a rule to a node and then it's children.
    TopDown,
    #[allow(dead_code)]
    // Apply a rule to a node's children and then the node itself.
    BottomUp,
    #[allow(dead_code)]
    // Delegate tree traversal to the rule.
    Delegated,
}

/// A logical plan optimization rule.
// TODO(Clark): Add fixed-point policy if needed.
pub trait OptimizerRule {
    /// Try to optimize the logical plan with this rule.
    ///
    /// This returns Transformed::Yes(new_plan) if the rule modified the plan, Transformed::No(old_plan) otherwise.
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>>;

    /// The plan tree order in which this rule should be applied (top-down, bottom-up, or delegated to rule).
    fn apply_order(&self) -> ApplyOrder;
}

/// An enum indicating whether or not the wrapped data has been transformed.
pub enum Transformed<T> {
    // Yes, the data has been transformed.
    Yes(T),
    // No, the data has not been transformed.
    No(T),
}

impl<T> Transformed<T> {
    /// Returns self if self is Yes, otherwise returns other.
    pub fn or(self, other: Self) -> Self {
        match self {
            Self::Yes(_) => self,
            Self::No(_) => other,
        }
    }

    /// Returns whether self is No.
    pub fn is_no(&self) -> bool {
        matches!(self, Self::No(_))
    }

    /// Unwraps the enum and returns a reference to the inner value.
    // TODO(Clark): Take ownership of self and return a plain T?
    pub fn unwrap(&self) -> &T {
        match self {
            Self::Yes(inner) => inner,
            Self::No(inner) => inner,
        }
    }
}
