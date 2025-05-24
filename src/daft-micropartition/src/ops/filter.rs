use common_error::DaftResult;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_io::IOStatsContext;
use daft_stats::TruthValue;
use snafu::ResultExt;

use crate::{micropartition::MicroPartition, DaftCoreComputeSnafu};

impl MicroPartition {
    pub fn filter(&self, predicate: &[BoundExpr]) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::filter");
        if predicate.is_empty() {
            return Ok(Self::empty(Some(self.schema.clone())));
        }
        if let Some(statistics) = &self.statistics {
            let folded_expr = BoundExpr::new_unchecked(
                predicate
                    .iter()
                    .map(BoundExpr::inner)
                    .cloned()
                    .reduce(daft_dsl::Expr::and)
                    .expect("should have at least 1 expr"),
            );
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
