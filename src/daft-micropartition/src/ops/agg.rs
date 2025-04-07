use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_io::IOStatsContext;
use daft_recordbatch::RecordBatch;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    /// Common implementation for both regular aggregation and window aggregation
    fn agg_internal(
        &self,
        to_agg: &[ExprRef],
        group_by: &[ExprRef],
        is_window_agg: bool,
    ) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new(if is_window_agg {
            "MicroPartition::window_agg"
        } else {
            "MicroPartition::agg"
        });

        let tables = self.concat_or_get(io_stats)?;

        match tables.as_slice() {
            [] => {
                let empty_table = RecordBatch::empty(Some(self.schema.clone()))?;
                let agged = if is_window_agg {
                    empty_table.window_agg(to_agg, group_by)?
                } else {
                    empty_table.agg(to_agg, group_by)?
                };
                Ok(Self::new_loaded(
                    agged.schema.clone(),
                    vec![agged].into(),
                    None,
                ))
            }
            [t] => {
                let agged = if is_window_agg {
                    t.window_agg(to_agg, group_by)?
                } else {
                    t.agg(to_agg, group_by)?
                };
                Ok(Self::new_loaded(
                    agged.schema.clone(),
                    vec![agged].into(),
                    None,
                ))
            }
            _ => unreachable!(),
        }
    }

    pub fn agg(&self, to_agg: &[ExprRef], group_by: &[ExprRef]) -> DaftResult<Self> {
        self.agg_internal(to_agg, group_by, false)
    }

    pub fn window_agg(&self, to_agg: &[ExprRef], group_by: &[ExprRef]) -> DaftResult<Self> {
        self.agg_internal(to_agg, group_by, true)
    }
}
