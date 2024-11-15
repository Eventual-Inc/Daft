use std::sync::Arc;

use common_error::DaftError;
use daft_core::prelude::*;
use daft_dsl::{AggExpr, Expr, ExprRef, ExprResolver};
use daft_schema::schema::{Schema, SchemaRef};
use itertools::Itertools;
use snafu::ResultExt;

use crate::{
    logical_plan::{self, CreationSnafu},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Pivot {
    pub input: Arc<LogicalPlan>,
    pub group_by: Vec<ExprRef>,
    pub pivot_column: ExprRef,
    pub value_column: ExprRef,
    pub aggregation: AggExpr,
    pub names: Vec<String>,
    pub output_schema: SchemaRef,
}

impl Pivot {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        group_by: Vec<ExprRef>,
        pivot_column: ExprRef,
        value_column: ExprRef,
        aggregation: ExprRef,
        names: Vec<String>,
    ) -> logical_plan::Result<Self> {
        let upstream_schema = input.schema();

        let expr_resolver = ExprResolver::default();
        let agg_resolver = ExprResolver::builder().in_agg_context(true).build();

        let (group_by, group_by_fields) = expr_resolver
            .resolve(group_by, &upstream_schema)
            .context(CreationSnafu)?;
        let (pivot_column, _) = expr_resolver
            .resolve_single(pivot_column, &upstream_schema)
            .context(CreationSnafu)?;
        let (value_column, value_col_field) = expr_resolver
            .resolve_single(value_column, &upstream_schema)
            .context(CreationSnafu)?;

        let (aggregation, _) = agg_resolver
            .resolve_single(aggregation, &upstream_schema)
            .context(CreationSnafu)?;

        let Expr::Agg(agg_expr) = aggregation.as_ref() else {
            return Err(DaftError::ValueError(format!(
                "Pivot only supports using top level aggregation expressions, received {aggregation}",
            ))
            .into());
        };

        let output_schema = {
            let value_col_dtype = value_col_field.dtype;
            let pivot_value_fields = names
                .iter()
                .map(|f| Field::new(f, value_col_dtype.clone()))
                .collect::<Vec<_>>();
            let fields = group_by_fields
                .into_iter()
                .chain(pivot_value_fields)
                .collect::<Vec<_>>();
            Schema::new(fields).context(CreationSnafu)?.into()
        };

        Ok(Self {
            input,
            group_by,
            pivot_column,
            value_column,
            aggregation: agg_expr.clone(),
            names,
            output_schema,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("Pivot:".to_string());
        res.push(format!(
            "Group by = {}",
            self.group_by.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!("Pivot column: {}", self.pivot_column));
        res.push(format!("Value column: {}", self.value_column));
        res.push(format!("Aggregation: {}", self.aggregation));
        res.push(format!(
            "Output schema = {}",
            self.output_schema.short_string()
        ));
        res
    }
}
