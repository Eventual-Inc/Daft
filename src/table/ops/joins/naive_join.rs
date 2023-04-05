use crate::{
    error::{DaftError, DaftResult},
    series::Series,
    table::Table,
};

pub(super) fn naive_inner_join(left: &Table, right: &Table) -> DaftResult<(Series, Series)> {
    #![allow(dead_code)]
    if left.num_columns() != right.num_columns() {
        return Err(DaftError::ValueError(format!(
            "Mismatch of join on clauses: left: {:?} vs right: {:?}",
            left.num_columns(),
            right.num_columns()
        )));
    }
    if left.num_columns() == 0 {
        return Err(DaftError::ValueError(
            "No columns were passed in to join on".to_string(),
        ));
    }
    if left.num_columns() == 1 {
        let left_series = left.get_column_by_index(0)?;
        let right_series = right.get_column_by_index(0)?;
        return left_series.pairwise_equal(right_series);
    }
    Series::pairwise_multi_equal(left.columns.as_slice(), right.columns.as_slice())
}
