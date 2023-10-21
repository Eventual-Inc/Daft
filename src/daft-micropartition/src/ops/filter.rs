use common_error::DaftResult;
use daft_dsl::Expr;
use snafu::ResultExt;

use crate::{
    column_stats::TruthValue,
    micropartition::{MicroPartition, TableState},
    DaftCoreComputeSnafu,
};

use daft_scan::TableMetadata;

impl MicroPartition {
    pub fn filter(&self, predicate: &[Expr]) -> DaftResult<Self> {
        if predicate.is_empty() {
            return Ok(Self::empty(Some(self.schema.clone())));
        }
        if let Some(statistics) = &self.statistics {
            let folded_expr = predicate
                .iter()
                .cloned()
                .reduce(|a, b| a.and(&b))
                .expect("should have at least 1 expr");
            let eval_result = statistics.eval_expression(&folded_expr)?;
            let tv = eval_result.to_truth_value();

            if matches!(tv, TruthValue::False) {
                return Ok(Self::empty(Some(self.schema.clone())));
            }
        }
        // TODO figure out defered IOStats
        let tables = self
            .tables_or_read(None)?
            .iter()
            .map(|t| t.filter(predicate))
            .collect::<DaftResult<Vec<_>>>()
            .context(DaftCoreComputeSnafu)?;

        let new_len = tables.iter().map(|t| t.len()).sum();

        Ok(Self::new(
            self.schema.clone(),
            TableState::Loaded(tables.into()),
            TableMetadata { length: new_len },
            self.statistics.clone(), // update these values based off the filter we just ran
        ))
    }
}
