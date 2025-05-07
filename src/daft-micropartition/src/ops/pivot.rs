use common_error::{DaftError, DaftResult};
use daft_dsl::{expr::bound_expr::BoundExpr, ExprRef};
use daft_io::IOStatsContext;
use daft_recordbatch::RecordBatch;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn pivot(
        &self,
        group_by: &[ExprRef],
        pivot_col: ExprRef,
        values_col: ExprRef,
        names: Vec<String>,
    ) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::pivot");

        let tables = self.concat_or_get(io_stats)?;

        let group_by = group_by
            .iter()
            .map(|expr| BoundExpr::try_new(expr.clone(), &self.schema))
            .try_collect::<Vec<_>>()?;

        let pivot_col = BoundExpr::try_new(pivot_col, &self.schema)?;
        let values_col = BoundExpr::try_new(values_col, &self.schema)?;

        match tables.as_slice() {
            [] => {
                let empty_table = RecordBatch::empty(Some(self.schema.clone()))?;
                let pivoted = empty_table.pivot(&group_by, pivot_col, values_col, names)?;
                Ok(Self::empty(Some(pivoted.schema)))
            }
            [t] => {
                let pivoted = t.pivot(&group_by, pivot_col, values_col, names)?;
                Ok(Self::new_loaded(
                    pivoted.schema.clone(),
                    vec![pivoted].into(),
                    None,
                ))
            }
            _ => Err(DaftError::ComputeError(
                "Pivot operation is not supported on multiple tables".to_string(),
            )),
        }
    }
}
