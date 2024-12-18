use daft_logical_plan::LogicalPlanBuilder;
use eyre::bail;

use super::SparkAnalyzer;

impl SparkAnalyzer<'_> {
    pub async fn drop(&self, drop: spark_connect::Drop) -> eyre::Result<LogicalPlanBuilder> {
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

        let plan = Box::pin(self.to_logical_plan(*input)).await?;

        // Get all column names from the schema
        let all_columns = plan.schema().names();

        // Create a set of columns to drop for efficient lookup
        let columns_to_drop: std::collections::HashSet<_> = column_names.iter().collect();

        // Create expressions for all columns except the ones being dropped
        let to_select = all_columns
            .iter()
            .filter(|col_name| !columns_to_drop.contains(*col_name))
            .map(|col_name| daft_dsl::col(col_name.clone()))
            .collect();

        // Use select to keep only the columns we want
        Ok(plan.select(to_select)?)
    }
}
