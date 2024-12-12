use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_io::IOStatsContext;
use daft_table::Table;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn agg(&self, to_agg: &[ExprRef], group_by: &[ExprRef]) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::agg");

        let tables = self.concat_or_get(io_stats)?;

        match tables.as_slice() {
            [] => {
                let empty_table = Table::empty(Some(self.schema.clone()))?;
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

    pub fn sharded_group_agg(
        &self,
        to_agg: &[ExprRef],
        group_by: &[ExprRef],
        num_shards: usize,
    ) -> DaftResult<Vec<Self>> {
        let io_stats = IOStatsContext::new("MicroPartition::agg");

        let tables = self.concat_or_get(io_stats)?;

        match tables.as_slice() {
            [] => {
                todo!("handle non grouped case")
                // let empty_table = Table::empty(Some(self.schema.clone()))?;
                // let agged = empty_table.agg(to_agg, group_by)?;
                // Ok(vec![Self::new_loaded(
                //     agged.schema.clone(),
                //     vec![agged].into(),
                //     None,
                // )])
            }
            [t] => {
                let agged = t.sharded_agg_groupby(to_agg, group_by, num_shards)?;
                Ok(agged
                    .into_iter()
                    .map(|t| Self::new_loaded(t.schema.clone(), vec![t].into(), None))
                    .collect())
            }
            _ => unreachable!(),
        }
    }
}
