use common_error::{DaftError, DaftResult};
use daft_core::series::Series;
use daft_dsl::ExprRef;

use crate::Table;

impl Table {
    pub fn sort(&self, sort_keys: &[ExprRef], descending: &[bool]) -> DaftResult<Self> {
        let argsort = self.argsort(sort_keys, descending)?;
        self.take(&argsort)
    }

    pub fn argsort(&self, sort_keys: &[ExprRef], descending: &[bool]) -> DaftResult<Series> {
        if sort_keys.len() != descending.len() {
            return Err(DaftError::ValueError(format!(
                "sort_keys and descending length must match, got {} vs {}",
                sort_keys.len(),
                descending.len()
            )));
        }
        if sort_keys.len() == 1 {
            self.eval_expression(sort_keys.first().unwrap())?
                .argsort(*descending.first().unwrap())
        } else {
            let expr_result = self.eval_expression_list(sort_keys)?;
            Series::argsort_multikey(expr_result.columns.as_slice(), descending)
        }
    }
}
