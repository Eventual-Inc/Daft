use eyre::bail;

use crate::translation::{to_logical_plan, Plan};

pub async fn drop(drop: spark_connect::Drop) -> eyre::Result<Plan> {
    let spark_connect::Drop {
        input,
        columns,
        column_names,
    } = drop;

    let Some(input) = input else {
        bail!("input is required");
    };

    if !columns.is_empty() {
        bail!("columns is not supported; use column_names instead");
    }

    let mut plan = Box::pin(to_logical_plan(*input)).await?;

    // Get all column names from the schema
    let all_columns = plan.builder.schema().names();

    // Create a set of columns to drop for efficient lookup
    let columns_to_drop: std::collections::HashSet<_> = column_names.iter().collect();

    // Create expressions for all columns except the ones being dropped
    let to_select = all_columns
        .iter()
        .filter(|col_name| !columns_to_drop.contains(*col_name))
        .map(|col_name| daft_dsl::col(col_name.clone()))
        .collect();

    // Use select to keep only the columns we want
    plan.builder = plan.builder.select(to_select)?;

    Ok(plan)
}
