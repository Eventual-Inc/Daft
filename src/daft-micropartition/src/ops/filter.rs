use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_io::IOStatsContext;
use snafu::ResultExt;

use crate::{micropartition::MicroPartition, DaftCoreComputeSnafu};

use daft_stats::TruthValue;

impl MicroPartition {
    pub fn filter(&self, predicate: &[ExprRef]) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::filter");
        if predicate.is_empty() {
            return Ok(Self::empty(Some(self.schema.clone())));
        }
        if let Some(statistics) = &self.statistics {
            let folded_expr = predicate
                .iter()
                .cloned()
                .reduce(|a, b| a.and(b))
                .expect("should have at least 1 expr");
            let eval_result = statistics.eval_expression(&folded_expr)?;
            let tv = eval_result.to_truth_value();

            if matches!(tv, TruthValue::False) {
                return Ok(Self::empty(Some(self.schema.clone())));
            }
        }
        // TODO figure out deferred IOStats
        let tables = self
            .tables_or_read(io_stats)?
            .iter()
            .map(|t| t.filter(predicate))
            .collect::<DaftResult<Vec<_>>>()
            .context(DaftCoreComputeSnafu)?;

        Ok(Self::new_loaded(
            self.schema.clone(),
            tables.into(),
            self.statistics.clone(), // update these values based off the filter we just ran
        ))
    }
}
