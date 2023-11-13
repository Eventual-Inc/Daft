use common_error::DaftResult;
use daft_dsl::Expr;
use daft_io::IOStatsContext;
use daft_table::Table;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn agg(&self, to_agg: &[Expr], group_by: &[Expr]) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::agg");

        let tables = self.concat_or_get(io_stats)?;

        match tables.as_slice() {
            [] => {
                let empty_table = Table::empty(Some(self.schema.clone()))?;
                let agged = empty_table.agg(to_agg, group_by)?;
                Ok(MicroPartition::new_loaded(
                    agged.schema.clone(),
                    vec![agged].into(),
                    None,
                ))
            }
            [t] => {
                let agged = t.agg(to_agg, group_by)?;
                Ok(MicroPartition::new_loaded(
                    agged.schema.clone(),
                    vec![agged].into(),
                    None,
                ))
            }
            _ => unreachable!(),
        }
    }
}
