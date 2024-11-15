use std::sync::Arc;

use common_error::DaftError;
use daft_core::prelude::*;
use daft_dsl::{ExprRef, ExprResolver};
use itertools::Itertools;
use snafu::ResultExt;

use crate::{logical_plan, logical_plan::CreationSnafu, LogicalPlan};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Sort {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub sort_by: Vec<ExprRef>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
}

impl Sort {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        sort_by: Vec<ExprRef>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
    ) -> logical_plan::Result<Self> {
        if sort_by.is_empty() {
            return Err(DaftError::ValueError(
                "df.sort() must be given at least one column/expression to sort by".to_string(),
            ))
            .context(CreationSnafu);
        }

        let expr_resolver = ExprResolver::default();

        let (sort_by, sort_by_fields) = expr_resolver
            .resolve(sort_by, &input.schema())
            .context(CreationSnafu)?;

        let sort_by_resolved_schema = Schema::new(sort_by_fields).context(CreationSnafu)?;

        for (field, expr) in sort_by_resolved_schema.fields.values().zip(sort_by.iter()) {
            // Disallow sorting by null, binary, and boolean columns.
            // TODO(Clark): This is a port of an existing constraint, we should look at relaxing this.
            if let dt @ (DataType::Null | DataType::Binary) = &field.dtype {
                return Err(DaftError::ValueError(format!(
                    "Cannot sort on expression {expr} with type: {dt}",
                )))
                .context(CreationSnafu);
            }
        }
        Ok(Self {
            input,
            sort_by,
            descending,
            nulls_first,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        // Must have at least one expression to sort by.
        assert!(!self.sort_by.is_empty());
        let pairs = self
            .sort_by
            .iter()
            .zip(self.descending.iter())
            .zip(self.nulls_first.iter())
            .map(|((sb, d), nf)| {
                format!(
                    "({}, {}, {})",
                    sb,
                    if *d { "descending" } else { "ascending" },
                    if *nf { "nulls first" } else { "nulls last" }
                )
            })
            .join(", ");
        res.push(format!("Sort: Sort by = {}", pairs));
        res
    }
}
