use std::{collections::HashSet, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::Expr;
use snafu::ResultExt;

use crate::{
    column_stats::TruthValue,
    micropartition::{MicroPartition, TableState},
    table_metadata::TableMetadata,
    DaftCoreComputeSnafu,
};

fn infer_schema(exprs: &[Expr], schema: &Schema) -> DaftResult<SchemaRef> {
    let fields = exprs
        .iter()
        .map(|e| e.to_field(schema).context(DaftCoreComputeSnafu))
        .collect::<crate::Result<Vec<_>>>()?;

    let mut seen: HashSet<String> = HashSet::new();
    for field in fields.iter() {
        let name = &field.name;
        if seen.contains(name) {
            return Err(DaftError::ValueError(format!(
                "Duplicate name found when evaluating expressions: {name}"
            )));
        }
        seen.insert(name.clone());
    }
    Ok(Schema::new(fields)?.into())
}

impl MicroPartition {
    pub fn eval_expression_list(&self, exprs: &[Expr]) -> DaftResult<Self> {
        let expected_schema = infer_schema(exprs, &self.schema)?;
        let tables = self.tables_or_read(None)?;
        let evaluated_tables = tables
            .iter()
            .map(|t| t.eval_expression_list(exprs))
            .collect::<DaftResult<Vec<_>>>()?;

        let eval_stats = self
            .statistics
            .as_ref()
            .and_then(|s| Some(s.eval_expression_list(exprs, expected_schema.as_ref())))
            .transpose()?;

        Ok(MicroPartition::new(
            expected_schema,
            TableState::Loaded(Arc::new(evaluated_tables)),
            TableMetadata { length: self.len() },
            eval_stats,
        ))
    }
}
