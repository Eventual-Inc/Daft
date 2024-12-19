use daft_logical_plan::LogicalPlanBuilder;
use eyre::bail;
use spark_connect::{expression::ExprType, Expression};

use super::SparkAnalyzer;
use crate::translation::to_daft_expr;

impl SparkAnalyzer<'_> {
    pub async fn with_columns(
        &self,
        with_columns: spark_connect::WithColumns,
    ) -> eyre::Result<LogicalPlanBuilder> {
        let spark_connect::WithColumns { input, aliases } = with_columns;

        let Some(input) = input else {
            bail!("input is required");
        };

        let plan = Box::pin(self.to_logical_plan(*input)).await?;

        let daft_exprs: Vec<_> = aliases
            .into_iter()
            .map(|alias| {
                let expression = Expression {
                    common: None,
                    expr_type: Some(ExprType::Alias(Box::new(alias))),
                };

                to_daft_expr(&expression)
            })
            .try_collect()?;

        Ok(plan.with_columns(daft_exprs)?)
    }
}
