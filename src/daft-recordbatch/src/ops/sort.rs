use common_error::{DaftError, DaftResult};
use daft_core::series::Series;
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
    ) -> DaftResult<Series> {
        if sort_keys.len() != descending.len() {
            return Err(DaftError::ValueError(format!(
                "sort_keys and descending length must match, got {} vs {}",
                sort_keys.len(),
                descending.len()
            )));
        }
        if sort_keys.len() == 1 {
            self.eval_expression(sort_keys.first().unwrap())?
                .argsort(*descending.first().unwrap(), *nulls_first.first().unwrap())
        } else {
            let expr_result = self.eval_expression_list(sort_keys)?;
            Series::argsort_multikey(expr_result.columns.as_slice(), descending, nulls_first)
        }
    }

    pub fn top_n(
        &self,
        sort_keys: &[BoundExpr],
        descending: &[bool],
        nulls_first: &[bool],
        limit: usize,
    ) -> DaftResult<Self> {
        let argsort = self.argsort(sort_keys, descending, nulls_first)?;
        let top_n = argsort.slice(0, limit)?;
        self.take(&top_n)
    }
}
