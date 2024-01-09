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

    pub fn sample_by_fraction(
        &self,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new(format!("MicroPartition::sample({fraction})"));

        if fraction == 0.0 {
            return Ok(Self::empty(Some(self.schema.clone())));
        }

        let tables = self.concat_or_get(io_stats)?;

        match tables.as_slice() {
            [] => Ok(Self::empty(Some(self.schema.clone()))),
            [single] => {
                let taken = single.sample_by_fraction(fraction, with_replacement, seed)?;
                Ok(Self::new_loaded(
                    self.schema.clone(),
                    Arc::new(vec![taken]),
                    self.statistics.clone(),
                ))
            }
            _ => unreachable!(),
        }
    }

    pub fn sample_by_size(
        &self,
        size: usize,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new(format!("MicroPartition::sample({size})"));

        if size == 0 {
            return Ok(Self::empty(Some(self.schema.clone())));
        }

        let tables = self.concat_or_get(io_stats)?;

        match tables.as_slice() {
            [] => Ok(Self::empty(Some(self.schema.clone()))),
            [single] => {
                let taken = single.sample(size, with_replacement, seed)?;
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
