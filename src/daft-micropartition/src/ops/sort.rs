use std::sync::Arc;

use common_error::DaftResult;
use daft_core::Series;
use daft_dsl::ExprRef;
use daft_io::IOStatsContext;
use daft_table::Table;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn sort(&self, sort_keys: &[ExprRef], descending: &[bool]) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::sort");

        let tables = self.concat_or_get(io_stats)?;
        match tables.as_slice() {
            [] => Ok(Self::empty(Some(self.schema.clone()))),
            [single] => {
                let sorted = single.sort(sort_keys, descending)?;
                Ok(Self::new_loaded(
                    self.schema.clone(),
                    Arc::new(vec![sorted]),
                    self.statistics.clone(),
                ))
            }
            _ => unreachable!(),
        }
    }

    pub fn argsort(&self, sort_keys: &[ExprRef], descending: &[bool]) -> DaftResult<Series> {
        let io_stats = IOStatsContext::new("MicroPartition::argsort");

        let tables = self.concat_or_get(io_stats)?;
        match tables.as_slice() {
            [] => {
                let empty_table = Table::empty(Some(self.schema.clone()))?;
                empty_table.argsort(sort_keys, descending)
            }
            [single] => single.argsort(sort_keys, descending),
            _ => unreachable!(),
        }
    }
}
