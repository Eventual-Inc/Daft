use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::series::Series;
use daft_io::IOStatsContext;
use daft_recordbatch::RecordBatch;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn take(&self, idx: &Series) -> DaftResult<RecordBatch> {
        let io_stats = IOStatsContext::new("MicroPartition::take");

        if idx.is_empty() {
            return Ok(RecordBatch::empty(Some(self.schema.clone())));
        }

        let tables = self.concat_or_get_update(io_stats)?;
        match tables {
            // Fallback onto `[empty_table]` behavior
            None => {
                let empty_table = RecordBatch::empty(Some(self.schema.clone()));
                empty_table.take(idx)
            }
            Some(single) => single.take(idx),
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

        match tables {
            None => Ok(Self::empty(Some(self.schema.clone()))),
            Some(single) => {
                let taken = single.sample_by_fraction(fraction, with_replacement, seed)?;
                Ok(Self::new_loaded(
                    self.schema.clone(),
                    Arc::new(vec![taken]),
                    self.statistics.clone(),
                ))
            }
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

        match tables {
            None => {
                // Empty dataframe: check with_replacement
                if !with_replacement {
                    return Err(DaftError::ValueError(
                        "Cannot take a sample larger than the population when 'replace=False'"
                            .to_string(),
                    ));
                }
                // For with_replacement=True, we can't sample from empty, so return empty
                Ok(Self::empty(Some(self.schema.clone())))
            }
            Some(single) => {
                let taken = single.sample(size, with_replacement, seed)?;
                Ok(Self::new_loaded(
                    self.schema.clone(),
                    Arc::new(vec![taken]),
                    self.statistics.clone(),
                ))
            }
        }
    }

    pub fn quantiles(&self, num: usize) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new(format!("MicroPartition::quantiles({num})"));

        if num <= 1 {
            return Ok(Self::empty(Some(self.schema.clone())));
        }

        let tables = self.concat_or_get(io_stats)?;
        match tables {
            None => Ok(Self::empty(Some(self.schema.clone()))),
            Some(single) => {
                let taken = single.quantiles(num)?;
                Ok(Self::new_loaded(
                    self.schema.clone(),
                    Arc::new(vec![taken]),
                    self.statistics.clone(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_by_size_empty_size_zero_returns_empty() {
        let mp = MicroPartition::empty(None);
        let out = mp.sample_by_size(0, false, None).unwrap();
        assert!(out.is_empty());
        assert_eq!(out.schema().names(), mp.schema().names());
    }

    #[test]
    fn sample_by_size_empty_without_replacement_errors() {
        let mp = MicroPartition::empty(None);
        let err = mp.sample_by_size(5, false, Some(42)).unwrap_err();
        match err {
            DaftError::ValueError(msg) => {
                assert!(msg.contains(
                    "Cannot take a sample larger than the population when 'replace=False'"
                ));
            }
            other => panic!("expected ValueError, got {other:?}"),
        }
    }

    #[test]
    fn sample_by_size_empty_with_replacement_returns_empty() {
        let mp = MicroPartition::empty(None);
        let out = mp.sample_by_size(5, true, Some(123)).unwrap();
        assert!(out.is_empty());
        assert_eq!(out.schema().names(), mp.schema().names());
    }
}
