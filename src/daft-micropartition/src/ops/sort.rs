use std::sync::Arc;

use common_error::DaftResult;
use daft_core::Series;
use daft_dsl::Expr;

use crate::micropartition::{MicroPartition, TableState};

impl MicroPartition {
    pub fn sort(&self, sort_keys: &[Expr], descending: &[bool]) -> DaftResult<Self> {
        let tables = self.concat_or_get()?;
        if let [single] = tables.as_slice() {
            let sorted = single.sort(sort_keys, descending)?;
            Ok(Self::new(
                self.schema.clone(),
                TableState::Loaded(Arc::new(vec![sorted])),
                self.metadata.clone(),
                self.statistics.clone(),
            ))
        } else {
            unreachable!()
        }
    }

    pub fn argsort(&self, sort_keys: &[Expr], descending: &[bool]) -> DaftResult<Series> {
        let tables = self.concat_or_get()?;
        if let [single] = tables.as_slice() {
            single.argsort(sort_keys, descending)
        } else {
            unreachable!()
        }
    }
}
