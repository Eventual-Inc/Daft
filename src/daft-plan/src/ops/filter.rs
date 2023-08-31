use std::sync::Arc;

use daft_core::DataType;
use daft_dsl::Expr;
use snafu::ResultExt;

use crate::logical_plan::{CreationSnafu, Result};
use crate::LogicalPlan;
use common_error::DaftError;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Filter {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // The Boolean expression to filter on.
    pub predicate: Expr,
}

impl Filter {
    pub(crate) fn try_new(input: Arc<LogicalPlan>, predicate: Expr) -> Result<Self> {
        let field = predicate
            .to_field(input.schema().as_ref())
            .context(CreationSnafu)?;
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
