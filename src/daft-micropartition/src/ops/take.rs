use std::sync::Arc;

use common_error::DaftResult;
use daft_core::Series;

use crate::{
    micropartition::{MicroPartition, TableState},
    table_metadata::TableMetadata,
};

impl MicroPartition {
    pub fn take(&self, idx: &Series) -> DaftResult<Self> {
        let tables = self.concat_or_get()?;
        if let [single] = tables.as_slice() {
            let taken = single.take(idx)?;
            Ok(Self::new(
                self.schema.clone(),
                TableState::Loaded(Arc::new(vec![taken])),
                TableMetadata { length: idx.len() },
                self.statistics.clone(),
            ))
        } else {
            unreachable!()
        }
    }

    pub fn sample(&self, num: usize) -> DaftResult<Self> {
        let tables = self.concat_or_get()?;
        if let [single] = tables.as_slice() {
            let taken = single.sample(num)?;
            Ok(Self::new(
                self.schema.clone(),
                TableState::Loaded(Arc::new(vec![taken])),
                TableMetadata {
                    length: num.min(self.len()),
                },
                self.statistics.clone(),
            ))
        } else {
            unreachable!()
        }
    }

    pub fn quantiles(&self, num: usize) -> DaftResult<Self> {
        let tables = self.concat_or_get()?;
        if let [single] = tables.as_slice() {
            let taken = single.quantiles(num)?;
            Ok(Self::new(
                self.schema.clone(),
                TableState::Loaded(Arc::new(vec![taken])),
                TableMetadata {
                    length: (num - 1).max(0),
                },
                self.statistics.clone(),
            ))
        } else {
            unreachable!()
        }
    }
}
