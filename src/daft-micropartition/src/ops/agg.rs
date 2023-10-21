use common_error::DaftResult;
use daft_dsl::Expr;
use daft_table::Table;

use crate::micropartition::{MicroPartition, TableState};

use daft_scan::TableMetadata;

impl MicroPartition {
    pub fn agg(&self, to_agg: &[Expr], group_by: &[Expr]) -> DaftResult<Self> {
        let tables = self.concat_or_get()?;

        match tables.as_slice() {
            [] => {
                let empty_table = Table::empty(Some(self.schema.clone()))?;
                let agged = empty_table.agg(to_agg, group_by)?;
                let agged_len = agged.len();
                Ok(MicroPartition::new(
                    agged.schema.clone(),
                    TableState::Loaded(vec![agged].into()),
                    TableMetadata { length: agged_len },
                    None,
                ))
            }
            [t] => {
                let agged = t.agg(to_agg, group_by)?;
                let agged_len = agged.len();
                Ok(MicroPartition::new(
                    agged.schema.clone(),
                    TableState::Loaded(vec![agged].into()),
                    TableMetadata { length: agged_len },
                    None,
                ))
            }
            _ => unreachable!(),
        }
    }
}
