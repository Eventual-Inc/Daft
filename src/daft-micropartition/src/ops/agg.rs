use common_error::DaftResult;
use daft_dsl::{expr::bound_expr::BoundExpr, ExprRef};
use daft_io::IOStatsContext;
use daft_recordbatch::RecordBatch;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn agg(&self, to_agg: &[ExprRef], group_by: &[ExprRef]) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::agg");

        let tables = self.concat_or_get(io_stats)?;

        let to_agg = to_agg
            .iter()
            .map(|expr| BoundExpr::try_new(expr.clone(), &self.schema))
            .try_collect::<Vec<_>>()?;
        let group_by = group_by
            .iter()
            .map(|expr| BoundExpr::try_new(expr.clone(), &self.schema))
            .try_collect::<Vec<_>>()?;

        match tables.as_slice() {
            [] => {
                let empty_table = RecordBatch::empty(Some(self.schema.clone()))?;
                let agged = empty_table.agg(&to_agg, &group_by)?;
                Ok(Self::new_loaded(
                    agged.schema.clone(),
                    vec![agged].into(),
                    None,
                ))
            }
            [t] => {
                let agged = t.agg(&to_agg, &group_by)?;
                Ok(Self::new_loaded(
                    agged.schema.clone(),
                    vec![agged].into(),
                    None,
                ))
            }
            _ => unreachable!(),
        }
    }
}
