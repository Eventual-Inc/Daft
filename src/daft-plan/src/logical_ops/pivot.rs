use std::sync::Arc;

use common_error::DaftResult;
use daft_core::datatypes::Field;
use itertools::Itertools;
use snafu::ResultExt;

use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::{AggExpr, ExprRef};

use crate::logical_plan::{self, CreationSnafu};
use crate::LogicalPlan;

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
        aggregation: AggExpr,
        names: Vec<String>,
    ) -> logical_plan::Result<Self> {
        let output_schema = {
            let upstream_schema = input.schema();
            let group_by_fields = group_by
                .iter()
                .map(|e| e.to_field(&upstream_schema))
                .collect::<DaftResult<Vec<_>>>()
                .context(CreationSnafu)?;
            let value_col_field = value_column
                .to_field(&upstream_schema)
                .context(CreationSnafu)?;
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
            aggregation,
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
