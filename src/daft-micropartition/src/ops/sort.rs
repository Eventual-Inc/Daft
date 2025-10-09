use std::sync::Arc;

use common_error::DaftResult;
use daft_core::series::Series;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_io::IOStatsContext;
use daft_recordbatch::RecordBatch;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn sort(
        &self,
        sort_keys: &[BoundExpr],
        descending: &[bool],
        nulls_first: &[bool],
    ) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::sort");

        let tables = self.concat_or_get(io_stats)?;

        match tables {
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
    ) -> DaftResult<Series> {
        let io_stats = IOStatsContext::new("MicroPartition::argsort");

        let tables = self.concat_or_get(io_stats)?;

        match tables {
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
        let io_stats = IOStatsContext::new("MicroPartition::top_n");

        let tables = self.concat_or_get(io_stats)?;

        match tables {
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
