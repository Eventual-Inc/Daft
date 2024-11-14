use std::sync::Arc;

use common_error::DaftError;
use daft_core::prelude::*;
use daft_dsl::{ExprRef, ExprResolver};
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
        let expr_resolver = ExprResolver::default();

        let (predicate, field) = expr_resolver
            .resolve_single(predicate, &input.schema())
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
