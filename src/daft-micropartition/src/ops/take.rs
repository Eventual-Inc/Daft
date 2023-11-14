use std::sync::Arc;

use common_error::DaftResult;
use daft_core::Series;
use daft_io::IOStatsContext;
use daft_table::Table;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn take(&self, idx: &Series) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::take");

        if idx.is_empty() {
            return Ok(Self::empty(Some(self.schema.clone())));
        }

        let tables = self.concat_or_get(io_stats)?;
        match tables.as_slice() {
            // Fallback onto `[empty_table]` behavior
            [] => {
                let empty_table = Table::empty(Some(self.schema.clone()))?;
                let taken = empty_table.take(idx)?;
                Ok(Self::new_loaded(
                    self.schema.clone(),
                    Arc::new(vec![taken]),
                    self.statistics.clone(),
                ))
            }
            [single] => {
                let taken = single.take(idx)?;
                Ok(Self::new_loaded(
                    self.schema.clone(),
                    Arc::new(vec![taken]),
                    self.statistics.clone(),
                ))
            }
            _ => unreachable!(),
        }
    }

    pub fn sample(&self, num: usize) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new(format!("MicroPartition::sample({num})"));

        if num == 0 {
            return Ok(Self::empty(Some(self.schema.clone())));
        }

        let tables = self.concat_or_get(io_stats)?;

        match tables.as_slice() {
            [] => Ok(Self::empty(Some(self.schema.clone()))),
            [single] => {
                let taken = single.sample(num)?;
                Ok(Self::new_loaded(
                    self.schema.clone(),
                    Arc::new(vec![taken]),
                    self.statistics.clone(),
                ))
            }
            _ => unreachable!(),
        }
    }

    pub fn quantiles(&self, num: usize) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new(format!("MicroPartition::quantiles({num})"));

        if num <= 1 {
            return Ok(Self::empty(Some(self.schema.clone())));
        }

        let tables = self.concat_or_get(io_stats)?;
        match tables.as_slice() {
            [] => Ok(Self::empty(Some(self.schema.clone()))),
            [single] => {
                let taken = single.quantiles(num)?;
                Ok(Self::new_loaded(
                    self.schema.clone(),
                    Arc::new(vec![taken]),
                    self.statistics.clone(),
                ))
            }
            _ => unreachable!(),
        }
    }
}
