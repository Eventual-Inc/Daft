use common_error::DaftResult;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_io::IOStatsContext;
use daft_recordbatch::RecordBatch;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn pivot(
        &self,
        group_by: &[BoundExpr],
        pivot_col: BoundExpr,
        values_col: BoundExpr,
        names: Vec<String>,
    ) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::pivot");

        let tables = self.concat_or_get(io_stats)?;

        match tables {
            None => {
                let empty_table = RecordBatch::empty(Some(self.schema.clone()));
                let pivoted = empty_table.pivot(group_by, pivot_col, values_col, names)?;
                Ok(Self::empty(Some(pivoted.schema)))
            }
            Some(t) => {
                let pivoted = t.pivot(group_by, pivot_col, values_col, names)?;
                Ok(Self::new_loaded(
                    pivoted.schema.clone(),
                    vec![pivoted].into(),
                    None,
                ))
            }
        }
    }
}
