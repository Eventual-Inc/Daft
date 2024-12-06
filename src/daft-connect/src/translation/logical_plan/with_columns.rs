use eyre::bail;
use spark_connect::{expression::ExprType, Expression};

use crate::translation::{to_daft_expr, to_logical_plan, Plan};

pub fn with_columns(with_columns: spark_connect::WithColumns) -> eyre::Result<Plan> {
    let spark_connect::WithColumns { input, aliases } = with_columns;

    let Some(input) = input else {
        bail!("input is required");
    };

    let mut plan = to_logical_plan(*input)?;

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

    plan.builder = plan.builder.with_columns(daft_exprs)?;

    Ok(plan)
}
