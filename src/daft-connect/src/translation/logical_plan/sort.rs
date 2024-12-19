use daft_logical_plan::LogicalPlanBuilder;
use eyre::{bail, WrapErr};
use spark_connect::expression::{
    sort_order::{NullOrdering, SortDirection},
    SortOrder,
};
use tracing::warn;

use crate::translation::{to_daft_expr, SparkAnalyzer};

impl SparkAnalyzer<'_> {
    pub async fn sort(&self, sort: spark_connect::Sort) -> eyre::Result<LogicalPlanBuilder> {
        let spark_connect::Sort {
            input,
            order,
            is_global,
        } = sort;

        if let Some(is_global) = is_global {
            warn!("Ignoring is_global {is_global}; not yet implemented");
        }

        let Some(input) = input else {
            bail!("Input is required");
        };

        let plan = Box::pin(self.to_logical_plan(*input)).await?;

        let mut sort_by = Vec::new();
        let mut descending = Vec::new();
        let mut nulls_first = Vec::new();

        for o in &order {
            let SortOrder {
                child,
                direction,
                null_ordering,
            } = o;

            let Some(child) = child else {
                bail!("Child is required");
            };

            let child = to_daft_expr(child)?;

            let direction = SortDirection::try_from(*direction)
                .wrap_err_with(|| format!("Invalid sort direction: {direction:?}"))?;

            let null_ordering = NullOrdering::try_from(*null_ordering)
                .wrap_err_with(|| format!("Invalid null ordering: {null_ordering:?}"))?;

            let is_descending = match direction {
                SortDirection::Unspecified => {
                    // default to ascending order
                    false
                }
                SortDirection::Ascending => false,
                SortDirection::Descending => true,
            };

            let sort_nulls_first = match null_ordering {
                NullOrdering::SortNullsUnspecified => {
                    // default: match is_descending
                    is_descending
                }
                NullOrdering::SortNullsFirst => true,
                NullOrdering::SortNullsLast => false,
            };

            sort_by.push(child);
            descending.push(is_descending);
            nulls_first.push(sort_nulls_first);
        }

        let plan = plan.sort(sort_by, descending, nulls_first)?;

        Ok(plan)
    }
}
