use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::UInt64Array;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_recordbatch::RecordBatch;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn sort(
        &self,
        sort_keys: &[BoundExpr],
        descending: &[bool],
        nulls_first: &[bool],
    ) -> DaftResult<Self> {
        match self.concat_or_get()? {
            None => Ok(Self::empty(Some(self.schema.clone()))),
            Some(single) => {
                let sorted = single.sort(sort_keys, descending, nulls_first)?;
                Ok(Self::new_loaded(
                    self.schema.clone(),
                    Arc::new(vec![sorted]),
                    self.statistics.clone(),
                ))
            }
        }
    }

    pub fn argsort(
        &self,
        sort_keys: &[BoundExpr],
        descending: &[bool],
        nulls_first: &[bool],
    ) -> DaftResult<UInt64Array> {
        match self.concat_or_get()? {
            None => {
                let empty_table = RecordBatch::empty(Some(self.schema.clone()));
                empty_table.argsort(sort_keys, descending, nulls_first)
            }
            Some(single) => single.argsort(sort_keys, descending, nulls_first),
        }
    }

    pub fn top_n(
        &self,
        sort_keys: &[BoundExpr],
        descending: &[bool],
        nulls_first: &[bool],
        limit: usize,
        offset: Option<usize>,
    ) -> DaftResult<Self> {
        match self.concat_or_get()? {
            None => Ok(Self::empty(Some(self.schema.clone()))),
            Some(single) => {
                let sorted = single.top_n(sort_keys, descending, nulls_first, limit, offset)?;
                Ok(Self::new_loaded(
                    self.schema.clone(),
                    Arc::new(vec![sorted]),
                    self.statistics.clone(),
                ))
            }
        }
    }
}
