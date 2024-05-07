use common_error::{DaftError, DaftResult};
use daft_dsl::ExprRef;
use daft_io::IOStatsContext;
use daft_table::Table;

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

        match tables.as_slice() {
            [] => {
                let empty_table = Table::empty(Some(self.schema.clone()))?;
                let pivoted = empty_table.pivot(group_by, pivot_col, values_col, names)?;
                Ok(Self::empty(Some(pivoted.schema.clone())))
            }
            [t] => {
                let pivoted = t.pivot(group_by, pivot_col, values_col, names)?;
                Ok(MicroPartition::new_loaded(
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
