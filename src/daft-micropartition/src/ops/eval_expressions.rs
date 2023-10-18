use common_error::DaftResult;
use daft_dsl::Expr;
use snafu::ResultExt;

use crate::{
    column_stats::TruthValue,
    micropartition::{MicroPartition, TableState},
    table_metadata::TableMetadata,
    DaftCoreComputeSnafu,
};

impl MicroPartition {
    pub fn eval_expression_list(&self, exprs: &[Expr]) -> DaftResult<Self> {
        let tables = self.tables_or_read(None)?;
        let evaluated_tables = tables
            .iter()
            .map(|t| t.eval_expression_list(exprs))
            .collect::<DaftResult<Vec<_>>>()?;
        let eval_stats = self
            .statistics
            .and_then(|s| Some(s.eval_expression_list(exprs, self.schema.as_ref())))
            .transpose()?;
    }
}
