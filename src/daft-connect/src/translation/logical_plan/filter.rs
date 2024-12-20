use daft_logical_plan::LogicalPlanBuilder;
use eyre::bail;

use super::SparkAnalyzer;
use crate::translation::to_daft_expr;

impl SparkAnalyzer<'_> {
    pub async fn filter(&self, filter: spark_connect::Filter) -> eyre::Result<LogicalPlanBuilder> {
        let spark_connect::Filter { input, condition } = filter;

        let Some(input) = input else {
            bail!("input is required");
        };

        let Some(condition) = condition else {
            bail!("condition is required");
        };

        let condition = to_daft_expr(&condition)?;

        let plan = Box::pin(self.to_logical_plan(*input)).await?;
        Ok(plan.filter(condition)?)
    }
}
