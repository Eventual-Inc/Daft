use daft_logical_plan::LogicalPlanBuilder;
use eyre::{bail, WrapErr};

use super::SparkAnalyzer;
impl SparkAnalyzer<'_> {
    pub async fn to_df(&self, to_df: spark_connect::ToDf) -> eyre::Result<LogicalPlanBuilder> {
        let spark_connect::ToDf {
            input,
            column_names,
        } = to_df;

        let Some(input) = input else {
            bail!("Input is required");
        };

        let mut plan = Box::pin(self.to_logical_plan(*input)).await?;

        let column_names: Vec<_> = column_names
            .iter()
            .map(|s| daft_dsl::col(s.as_str()))
            .collect();

        plan = plan
            .select(column_names)
            .wrap_err("Failed to add columns to logical plan")?;
        Ok(plan)
    }
}
