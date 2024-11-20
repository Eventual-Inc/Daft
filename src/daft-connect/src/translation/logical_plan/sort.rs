use eyre::{bail, WrapErr};
use spark_connect::expression::{
    sort_order::{NullOrdering, SortDirection},
    SortOrder,
};
use tracing::warn;

use crate::translation::{to_daft_expr, to_logical_plan, Plan};

pub async fn sort(sort: spark_connect::Sort) -> eyre::Result<Plan> {
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

    let mut plan = Box::pin(to_logical_plan(*input)).await?;

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

        // todo(correctness): is this correct?
        let is_descending = match direction {
            SortDirection::Unspecified => {
                bail!("Unspecified sort direction is not yet supported")
            }
            SortDirection::Ascending => false,
            SortDirection::Descending => true,
        };

        // todo(correctness): is this correct?
        let tentative_sort_nulls_first = match null_ordering {
            NullOrdering::SortNullsUnspecified => {
                bail!("Unspecified null ordering is not yet supported")
            }
            NullOrdering::SortNullsFirst => true,
            NullOrdering::SortNullsLast => false,
        };

        // https://github.com/Eventual-Inc/Daft/blob/7922d2d810ff92b00008d877aa9a6553bc0dedab/src/daft-core/src/utils/mod.rs#L10-L19
        let sort_nulls_first = is_descending;

        if sort_nulls_first != tentative_sort_nulls_first {
            warn!("Ignoring nulls_first {sort_nulls_first}; not yet implemented");
        }

        sort_by.push(child);
        descending.push(is_descending);
        nulls_first.push(sort_nulls_first);
    }

    plan.builder = plan.builder.sort(sort_by, descending, nulls_first)?;

    Ok(plan)
}
