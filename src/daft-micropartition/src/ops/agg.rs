use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_io::IOStatsContext;
use daft_recordbatch::RecordBatch;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn agg(&self, to_agg: &[ExprRef], group_by: &[ExprRef]) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::agg");

        let tables = self.concat_or_get(io_stats)?;

        match tables.as_slice() {
            [] => {
                let empty_table = RecordBatch::empty(Some(self.schema.clone()))?;
                let agged = empty_table.agg(to_agg, group_by)?;
                Ok(Self::new_loaded(
                    agged.schema.clone(),
                    vec![agged].into(),
                    None,
                ))
            }
            [t] => {
                let agged = t.agg(to_agg, group_by)?;
                Ok(Self::new_loaded(
                    agged.schema.clone(),
                    vec![agged].into(),
                    None,
                ))
            }
            _ => unreachable!(),
        }
    }

    pub fn window_agg(&self, to_agg: &[ExprRef], group_by: &[ExprRef]) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::window_agg");

        let tables = self.concat_or_get(io_stats)?;

        match tables.as_slice() {
            [] => {
                let empty_table = RecordBatch::empty(Some(self.schema.clone()))?;
                Ok(Self::new_loaded(
                    empty_table.schema.clone(),
                    vec![empty_table].into(),
                    None,
                ))
            }
            [t] => {
                let agged = t.agg(to_agg, group_by)?;

                let result = Self::new_loaded(agged.schema.clone(), vec![agged].into(), None);

                Ok(result)
            }
            _ => unreachable!(),
        }
    }
}
