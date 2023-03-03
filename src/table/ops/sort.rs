use arrow2::compute::sort::{lexsort_to_indices_impl, SortColumn, SortOptions};

use crate::{
    array::BaseArray,
    datatypes::UInt64Array,
    dsl::Expr,
    error::{DaftError, DaftResult},
    kernels::search_sorted::build_compare_with_nan,
    series::Series,
    table::Table,
};

impl Table {
    pub fn sort(&self, sort_keys: &[Expr], descending: &[bool]) -> DaftResult<Table> {
        let argsort = self.argsort(sort_keys, descending)?;
        self.take(&argsort)
    }

    pub fn argsort(&self, sort_keys: &[Expr], descending: &[bool]) -> DaftResult<Series> {
        if sort_keys.len() != descending.len() {
            return Err(DaftError::ValueError(format!(
                "sort_keys and descending length must match, got {} vs {}",
                sort_keys.len(),
                descending.len()
            )));
        }
        if sort_keys.len() == 1 {
            self.eval_expression(sort_keys.get(0).unwrap())?
                .argsort(*descending.first().unwrap())
        } else {
            let expr_result = self.eval_expression_list(sort_keys)?;
            multi_series_argsort(expr_result.columns.as_slice(), descending)
        }
    }
}

fn multi_series_argsort(sort_keys: &[Series], descending: &[bool]) -> DaftResult<Series> {
    if sort_keys.len() != descending.len() {
        return Err(DaftError::ValueError(format!(
            "sort_keys and descending length must match, got {} vs {}",
            sort_keys.len(),
            descending.len()
        )));
    }
    let sort_columns: Vec<_> = sort_keys
        .iter()
        .zip(descending.iter())
        .map(|(series, desc)| SortColumn {
            values: series.array().data(),
            options: Some(SortOptions {
                descending: *desc,
                nulls_first: *desc,
            }),
        })
        .collect();

    let result =
        lexsort_to_indices_impl::<u64>(sort_columns.as_slice(), None, &build_compare_with_nan)?;

    Ok(UInt64Array::from((sort_keys.first().unwrap().name(), Box::new(result))).into_series())
}
