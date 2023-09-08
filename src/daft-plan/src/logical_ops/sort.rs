use std::sync::Arc;

use common_error::DaftError;
use daft_core::schema::Schema;
use daft_core::DataType;
use daft_dsl::Expr;
use snafu::ResultExt;

use crate::logical_plan;
use crate::logical_plan::CreationSnafu;
use crate::LogicalPlan;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Sort {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub sort_by: Vec<Expr>,
    pub descending: Vec<bool>,
}

impl Sort {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        sort_by: Vec<Expr>,
        descending: Vec<bool>,
    ) -> logical_plan::Result<Self> {
        if sort_by.is_empty() {
            return Err(DaftError::ValueError(
                "df.sort() must be given at least one column/expression to sort by".to_string(),
            ))
            .context(CreationSnafu);
        }
        let upstream_schema = input.schema();
        let sort_by_resolved_schema = {
            let sort_by_fields = sort_by
                .iter()
                .map(|e| e.to_field(&upstream_schema))
                .collect::<common_error::DaftResult<Vec<_>>>()
                .context(CreationSnafu)?;
            Schema::new(sort_by_fields).context(CreationSnafu)?
        };
        for (field, expr) in sort_by_resolved_schema.fields.values().zip(sort_by.iter()) {
            // Disallow sorting by null, binary, and boolean columns.
            // TODO(Clark): This is a port of an existing constraint, we should look at relaxing this.
            if let dt @ (DataType::Null | DataType::Binary | DataType::Boolean) = &field.dtype {
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
            .map(|(sb, d)| format!("({}, {})", sb, if *d { "descending" } else { "ascending" },))
            .collect::<Vec<_>>()
            .join(", ");
        res.push(format!("Sort: Sort by = {}", pairs));
        res
    }
}
