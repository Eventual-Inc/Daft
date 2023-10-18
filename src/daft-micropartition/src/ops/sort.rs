use std::sync::Arc;

use common_error::DaftResult;
use daft_core::Series;
use daft_dsl::Expr;
use daft_table::Table;

use crate::micropartition::{MicroPartition, TableState};

impl MicroPartition {
    pub fn sort(&self, sort_keys: &[Expr], descending: &[bool]) -> DaftResult<Self> {
        let tables = self.concat_or_get()?;
        match tables.as_slice() {
            [] => Ok(Self::empty(Some(self.schema.clone()))),
            [single] => {
                let sorted = single.sort(sort_keys, descending)?;
                Ok(Self::new(
                    self.schema.clone(),
                    TableState::Loaded(Arc::new(vec![sorted])),
                    self.metadata.clone(),
                    self.statistics.clone(),
                ))
            }
            _ => unreachable!(),
        }
    }

    pub fn argsort(&self, sort_keys: &[Expr], descending: &[bool]) -> DaftResult<Series> {
        let tables = self.concat_or_get()?;
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
