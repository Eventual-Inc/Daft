#![allow(deprecated, reason = "arrow2 migration")]
use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::{
    count_mode::CountMode,
    datatypes::{DataType, Field, UInt64Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{Expr, expr::bound_expr::BoundExpr, functions::scalar::ScalarFn};
use daft_functions_list::SeriesListExtension;

use crate::RecordBatch;

fn lengths_to_indices(lengths: &UInt64Array, capacity: usize) -> DaftResult<UInt64Array> {
    let mut indices = Vec::with_capacity(capacity);
    for (i, l) in lengths.into_iter().enumerate() {
        let l = std::cmp::max(*l.unwrap_or(&1), 1u64);
        (0..l).for_each(|_| indices.push(i as u64));
    }
    Ok(UInt64Array::from(("indices", indices)))
}

/// For each list element, generate its position within the list.
/// For null or empty lists, generate a single None (null index).
fn generate_explode_indices(
    lengths: &UInt64Array,
    capacity: usize,
    name: &str,
) -> DaftResult<UInt64Array> {
    let mut indices = Vec::with_capacity(capacity);
    for len in lengths {
        if let Some(&l) = len
            && l > 0
        {
            indices.extend((0..l).map(Some));
        } else {
            indices.push(None);
        }
    }
    Ok(UInt64Array::from_iter(
        Field::new(name, DataType::UInt64),
        indices.into_iter(),
    ))
}

impl RecordBatch {
    pub fn explode(&self, exprs: &[BoundExpr], index_column: Option<&str>) -> DaftResult<Self> {
        if exprs.is_empty() {
            return Err(DaftError::ValueError(format!(
                "Explode needs at least 1 expression, received: {}",
                exprs.len()
            )));
        }

        let mut evaluated_columns = Vec::with_capacity(exprs.len());
        for expr in exprs {
            match expr.as_ref() {
                Expr::ScalarFn(ScalarFn::Builtin(func)) => {
                    if func.name() == "explode" {
                        let inputs = &func.inputs.clone().into_inner();
                        if inputs.len() != 1 {
                            return Err(DaftError::ValueError(format!(
                                "ListExpr::Explode function expression must have one input only, received: {}",
                                inputs.len()
                            )));
                        }
                        let expr = BoundExpr::new_unchecked(inputs.first().unwrap().clone());
                        let exploded_name = expr.inner().get_name(&self.schema)?;
                        let evaluated = self.eval_expression(&expr)?;
                        if !matches!(
                            evaluated.data_type(),
                            DataType::List(..) | DataType::FixedSizeList(..)
                        ) {
                            return Err(DaftError::ValueError(format!(
                                "Expected Expression for series: `{exploded_name}` to be a List Type, but is {}",
                                evaluated.data_type()
                            )));
                        }
                        evaluated_columns.push(evaluated);
                    }
                }
                _ => {
                    return Err(DaftError::ValueError(
                        "Can only explode a ListExpr::Explode function expression".to_string(),
                    ));
                }
            }
        }
        let first_len = evaluated_columns
            .first()
            .unwrap()
            .list_count(CountMode::All)?;
        if evaluated_columns
            .iter()
            .skip(1)
            .any(|c| c.list_count(CountMode::All).unwrap().ne(&first_len))
        {
            return Err(DaftError::ValueError(
                "In multicolumn explode, list length did not match".to_string(),
            ));
        }
        let mut exploded_columns = evaluated_columns
            .iter()
            .map(daft_core::series::Series::explode)
            .collect::<DaftResult<Vec<_>>>()?;

        let capacity_expected = exploded_columns.first().unwrap().len();
        let take_idx = lengths_to_indices(&first_len, capacity_expected)?;

        let mut new_series = Arc::unwrap_or_clone(self.columns.clone());

        for i in 0..self.num_columns() {
            let name = new_series.get(i).unwrap().name();
            let result: Option<(usize, &Series)> = exploded_columns
                .iter()
                .enumerate()
                .find(|(_, s)| s.name().eq(name));
            if let Some((j, _)) = result {
                new_series[i] = exploded_columns.remove(j);
            } else {
                new_series[i] = new_series[i].take(&take_idx)?;
            }
        }
        new_series.extend_from_slice(exploded_columns.as_slice());

        if let Some(idx_col_name) = index_column {
            let index_series =
                generate_explode_indices(&first_len, capacity_expected, idx_col_name)?
                    .into_series();
            new_series.push(index_series);
        }

        Self::from_nonempty_columns(new_series)
    }
}
