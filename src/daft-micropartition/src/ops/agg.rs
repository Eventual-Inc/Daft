use common_error::{DaftError, DaftResult};
use daft_dsl::expr::bound_expr::{BoundAggExpr, BoundExpr};
use daft_recordbatch::RecordBatch;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn agg(&self, to_agg: &[BoundAggExpr], group_by: &[BoundExpr]) -> DaftResult<Self> {
        match self.concat_or_get()? {
            None => {
                let empty_table = RecordBatch::empty(Some(self.schema.clone()));
                let agged = empty_table.agg(to_agg, group_by)?;
                Ok(Self::new_loaded(
                    agged.schema.clone(),
                    vec![agged].into(),
                    None,
                ))
            }
            Some(t) => {
                let agged = t.agg(to_agg, group_by)?;
                Ok(Self::new_loaded(
                    agged.schema.clone(),
                    vec![agged].into(),
                    None,
                ))
            }
        }
    }

    pub fn dedup(&self, columns: &[BoundExpr]) -> DaftResult<Self> {
        if columns.is_empty() {
            return Err(DaftError::ValueError(
                "Attempting to deduplicate with no columns".to_string(),
            ));
        }

        match self.concat_or_get()? {
            None => {
                let empty_table = RecordBatch::empty(Some(self.schema.clone()));
                Ok(Self::new_loaded(
                    empty_table.schema.clone(),
                    vec![empty_table].into(),
                    None,
                ))
            }
            Some(t) => {
                let deduped = t.dedup(columns)?;
                Ok(Self::new_loaded(
                    deduped.schema.clone(),
                    vec![deduped].into(),
                    None,
                ))
            }
        }
    }
}
