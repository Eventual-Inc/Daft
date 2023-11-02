use std::sync::Arc;

use common_error::DaftResult;
use daft_core::Series;
use daft_table::Table;

use crate::micropartition::{MicroPartition, TableState};
use daft_stats::TableMetadata;

impl MicroPartition {
    pub fn take(&self, idx: &Series) -> DaftResult<Self> {
        let tables = self.concat_or_get()?;
        match tables.as_slice() {
            // Fallback onto `[empty_table]` behavior
            [] => {
                let empty_table = Table::empty(Some(self.schema.clone()))?;
                let taken = empty_table.take(idx)?;
                Ok(Self::new(
                    self.schema.clone(),
                    TableState::Loaded(Arc::new(vec![taken])),
                    Some(TableMetadata { length: idx.len() }),
                    self.statistics.clone(),
                ))
            }
            [single] => {
                let taken = single.take(idx)?;
                Ok(Self::new(
                    self.schema.clone(),
                    TableState::Loaded(Arc::new(vec![taken])),
                    Some(TableMetadata { length: idx.len() }),
                    self.statistics.clone(),
                ))
            }
            _ => unreachable!(),
        }
    }

    pub fn sample(&self, num: usize) -> DaftResult<Self> {
        let tables = self.concat_or_get()?;

        match tables.as_slice() {
            [] => Ok(Self::empty(Some(self.schema.clone()))),
            [single] => {
                let taken = single.sample(num)?;
                let taken_len = taken.len();
                Ok(Self::new(
                    self.schema.clone(),
                    TableState::Loaded(Arc::new(vec![taken])),
                    Some(TableMetadata { length: taken_len }),
                    self.statistics.clone(),
                ))
            }
            _ => unreachable!(),
        }
    }

    pub fn quantiles(&self, num: usize) -> DaftResult<Self> {
        let tables = self.concat_or_get()?;
        match tables.as_slice() {
            [] => Ok(Self::empty(Some(self.schema.clone()))),
            [single] => {
                let taken = single.quantiles(num)?;
                let taken_len = taken.len();
                Ok(Self::new(
                    self.schema.clone(),
                    TableState::Loaded(Arc::new(vec![taken])),
                    Some(TableMetadata { length: taken_len }),
                    self.statistics.clone(),
                ))
            }
            _ => unreachable!(),
        }
    }
}
