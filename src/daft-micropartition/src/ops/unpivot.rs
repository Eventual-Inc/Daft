use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_io::IOStatsContext;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn unpivot(
        &self,
        ids: &[ExprRef],
        values: &[ExprRef],
        variable_name: &str,
        value_name: &str,
    ) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::unpivot");

        let tables = self.concat_or_get(io_stats)?;

        match tables.as_slice() {
            [] => Ok(Self::empty(Some(self.schema.clone()))),
            [t] => {
                let unpivoted = t.unpivot(ids, values, variable_name, value_name)?;
                Ok(Self::new_loaded(
                    unpivoted.schema.clone(),
                    vec![unpivoted].into(),
                    None,
                ))
            }
            _ => unreachable!(),
        }
    }
}
