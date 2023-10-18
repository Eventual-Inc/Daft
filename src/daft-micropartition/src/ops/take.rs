use std::sync::Arc;

use common_error::DaftResult;
use daft_core::Series;
use snafu::ResultExt;

use crate::{
    column_stats::TruthValue,
    micropartition::{MicroPartition, TableState},
    table_metadata::TableMetadata,
    DaftCoreComputeSnafu,
};

impl MicroPartition {
    pub fn take(&self, idx: &Series) -> DaftResult<Self> {
        let tables = self.concat_or_get()?;
        if let [single] = tables.as_slice() {
            let taken = single.take(idx)?;
            Ok(Self::new(
                self.schema.clone(),
                TableState::Loaded(Arc::new(vec![taken])),
                self.metadata.clone(),
                self.statistics.clone(),
            ))
        } else {
            unreachable!()
        }
    }
}
