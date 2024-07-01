use std::sync::Arc;

use common_error::DaftError;
use daft_core::{
    datatypes::Field,
    schema::{Schema, SchemaRef},
    utils::supertype::try_get_supertype,
    DataType,
};
use daft_dsl::{resolve_exprs, ExprRef};

use itertools::Itertools;
use snafu::ResultExt;

use crate::{
    logical_plan::{self, CreationSnafu},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Unpivot {
    pub input: Arc<LogicalPlan>,
    pub ids: Vec<ExprRef>,
    pub values: Vec<ExprRef>,
    pub variable_name: String,
    pub value_name: String,
    pub output_schema: SchemaRef,
}

impl Unpivot {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        ids: Vec<ExprRef>,
        values: Vec<ExprRef>,
        variable_name: &str,
        value_name: &str,
    ) -> logical_plan::Result<Self> {
        if values.is_empty() {
            return Err(DaftError::ValueError(
                "Unpivot requires at least one value column".to_string(),
            ))
            .context(CreationSnafu);
        }

        let input_schema = input.schema();
        let (values, values_fields) =
            resolve_exprs(values, &input_schema).context(CreationSnafu)?;

        let value_dtype = values_fields
            .iter()
            .map(|f| f.dtype.clone())
            .try_reduce(|a, b| try_get_supertype(&a, &b))
            .context(CreationSnafu)?
            .unwrap();

        let variable_field = Field::new(variable_name, DataType::Utf8);
        let value_field = Field::new(value_name, value_dtype);

        let (ids, ids_fields) = resolve_exprs(ids, &input_schema).context(CreationSnafu)?;

        let output_fields = ids_fields
            .into_iter()
            .chain([variable_field, value_field])
            .collect::<Vec<_>>();

        let output_schema = Schema::new(output_fields).context(CreationSnafu)?.into();

        Ok(Self {
            input,
            ids,
            values,
            variable_name: variable_name.to_string(),
            value_name: value_name.to_string(),
            output_schema,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Unpivot: {}",
            self.values.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Ids = {}",
            self.ids.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!("Schema = {}", self.output_schema.short_string()));
        res
    }
}
