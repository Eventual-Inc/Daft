use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_error::DaftResult;
use daft_dsl::{
    col,
    optimization::{get_required_columns, replace_columns_with_expressions},
    Expr,
};

use crate::{
    ops::{Concat, Filter, Project},
    LogicalPlan,
};

use super::{
    utils::{conjuct, split_conjuction},
    ApplyOrder, OptimizerRule,
};

#[derive(Default)]
pub struct PushDownProjection {}

impl PushDownProjection {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownProjection {
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn try_optimize(&self, plan: &LogicalPlan) -> DaftResult<Option<Arc<LogicalPlan>>> {
        todo!()
    }
}
