use common_error::{DaftError, DaftResult};
use daft_core::{prelude::UInt64Array, series::Series};
use daft_dsl::expr::bound_expr::BoundExpr;

use crate::RecordBatch;

impl RecordBatch {
    pub fn sort(
        &self,
        sort_keys: &[BoundExpr],
        descending: &[bool],
        nulls_first: &[bool],
    ) -> DaftResult<Self> {
        let argsort = self.argsort(sort_keys, descending, nulls_first)?;
        self.take(&argsort)
    }

    pub fn argsort(
        &self,
        sort_keys: &[BoundExpr],
        descending: &[bool],
        nulls_first: &[bool],
    ) -> DaftResult<UInt64Array> {
        if sort_keys.len() != descending.len() {
            return Err(DaftError::ValueError(format!(
                "sort_keys and descending length must match, got {} vs {}",
                sort_keys.len(),
                descending.len()
            )));
        }
        let sort_values = self.eval_expression_list(sort_keys)?;
        if sort_values.num_columns() == 1 {
            sort_values
                .get_column(0)
                .argsort(*descending.first().unwrap(), *nulls_first.first().unwrap())
        } else {
            Series::argsort_multikey(sort_values.columns.as_slice(), descending, nulls_first)
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
        let argsort = self.argsort(sort_keys, descending, nulls_first)?;
        let offset = offset.unwrap_or(0);

        // DataArray::slice doesn't bound the start and end, so we need to do it manually.
        let len = argsort.len();
        let top_n = argsort.slice(offset.min(len), (offset + limit).min(len))?;
        self.take(&top_n)
    }
}
