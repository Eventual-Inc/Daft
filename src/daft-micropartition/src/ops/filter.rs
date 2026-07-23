use common_error::DaftResult;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_stats::TruthValue;
use snafu::ResultExt;

use crate::{DaftCoreComputeSnafu, micropartition::MicroPartition};

impl MicroPartition {
    pub fn filter(&self, predicate: &[BoundExpr]) -> DaftResult<Self> {
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

        let tables = self
            .record_batches()
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::{
        datatypes::{DataType, Field, Int64Array},
        prelude::Schema,
        series::IntoSeries,
    };
    use daft_dsl::{expr::bound_expr::BoundExpr, lit, resolved_col};
    use daft_recordbatch::RecordBatch;
    use daft_stats::TableStatistics;

    use crate::MicroPartition;

    #[test]
    fn test_filter_short_circuits_on_false_stats() {
        // Stats: min=10, max=20. Predicate: col < 5 → TruthValue::False.
        // MicroPartition.filter() should return empty WITHOUT scanning data.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));

        // Create data that would NOT satisfy the predicate if scanned normally
        let data = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_slice("a", &[1, 2, 3]).into_series(),
        ])
        .unwrap();

        // Create stats with min=10, max=20 (different from actual data)
        let stats_table = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_slice("a", &[10, 20]).into_series(),
        ])
        .unwrap();
        let stats = TableStatistics::from_stats_table(&stats_table).unwrap();

        let mp = MicroPartition::new_loaded(schema.clone(), Arc::new(vec![data]), Some(stats));

        // Predicate: col < 5 → should be False based on stats (min=10 > 5)
        let pred = BoundExpr::try_new(resolved_col("a").lt(lit(5i64)), &schema).unwrap();
        let result = mp.filter(&[pred]).unwrap();

        // Should return empty MicroPartition (short-circuit, not scan)
        assert_eq!(result.len(), 0);
        assert_eq!(result.schema, schema);
    }

    #[test]
    fn test_filter_does_not_short_circuit_on_maybe_stats() {
        // Stats: min=10, max=20. Predicate: col < 15 → TruthValue::Maybe.
        // MicroPartition.filter() should actually scan the data.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));

        // Data: [1, 12, 3] — only 12 satisfies col < 15
        let data = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_slice("a", &[1, 12, 3]).into_series(),
        ])
        .unwrap();

        let stats_table = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_slice("a", &[10, 20]).into_series(),
        ])
        .unwrap();
        let stats = TableStatistics::from_stats_table(&stats_table).unwrap();

        let mp = MicroPartition::new_loaded(schema.clone(), Arc::new(vec![data]), Some(stats));

        // Predicate: col >= 10 → Maybe (some values in [10,20] satisfy, some don't)
        let pred = BoundExpr::try_new(resolved_col("a").gt_eq(lit(10i64)), &schema).unwrap();
        let result = mp.filter(&[pred]).unwrap();

        // Should NOT be empty — data was actually scanned
        // Only row with a=12 satisfies a >= 10
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_filter_without_stats_scans_data() {
        // No stats → filter must scan data normally.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));

        let data = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_slice("a", &[1, 2, 3]).into_series(),
        ])
        .unwrap();

        let mp = MicroPartition::new_loaded(schema.clone(), Arc::new(vec![data]), None);

        let pred = BoundExpr::try_new(resolved_col("a").lt(lit(3i64)), &schema).unwrap();
        let result = mp.filter(&[pred]).unwrap();

        // Rows 1 and 2 satisfy a < 3
        assert_eq!(result.len(), 2);
    }
}
