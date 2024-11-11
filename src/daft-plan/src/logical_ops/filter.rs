use std::sync::Arc;

use common_error::DaftError;
use daft_core::prelude::*;
use daft_dsl::{resolve_single_expr, ExprRef};
use snafu::ResultExt;

use crate::{
    logical_plan::{CreationSnafu, Result},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Filter {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // The Boolean expression to filter on.
    pub predicate: ExprRef,
}

impl Filter {
    pub(crate) fn try_new(input: Arc<LogicalPlan>, predicate: ExprRef) -> Result<Self> {
        let (predicate, field) =
            resolve_single_expr(predicate, &input.schema(), false).context(CreationSnafu)?;

        if !matches!(field.dtype, DataType::Boolean) {
            return Err(DaftError::ValueError(format!(
                "Expected expression {predicate} to resolve to type Boolean, but received: {}",
                field.dtype
            )))
            .context(CreationSnafu);
        }
        Ok(Self { input, predicate })
    }
}

use crate::stats::{ApproxStats, Stats};
impl Stats for Filter {
    fn approximate_stats(&self) -> ApproxStats {
        // Assume no row/column pruning in cardinality-affecting operations.
        // TODO(desmond): We can do better estimations here. For now, reuse the old logic.
        let input_stats = self.input.approximate_stats();
        ApproxStats {
            lower_bound_rows: 0,
            upper_bound_rows: input_stats.upper_bound_rows,
            lower_bound_bytes: 0,
            upper_bound_bytes: input_stats.upper_bound_bytes,
        }
    }
}
