use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::as_arrow::AsArrow,
    count_mode::CountMode,
    datatypes::{DataType, UInt64Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{expr::bound_expr::BoundExpr, Expr};
use daft_functions_list::SeriesListExtension;

use crate::RecordBatch;

fn lengths_to_indices(lengths: &UInt64Array, capacity: usize) -> DaftResult<UInt64Array> {
    let mut indices = Vec::with_capacity(capacity);
    for (i, l) in lengths.as_arrow().iter().enumerate() {
        let l = std::cmp::max(*l.unwrap_or(&1), 1u64);
        (0..l).for_each(|_| indices.push(i as u64));
    }
    Ok(UInt64Array::from(("indices", indices)))
}

impl RecordBatch {
    pub fn explode(&self, exprs: &[BoundExpr]) -> DaftResult<Self> {
        if exprs.is_empty() {
            return Err(DaftError::ValueError(format!(
                "Explode needs at least 1 expression, received: {}",
                exprs.len()
            )));
        }

        let mut evaluated_columns = Vec::with_capacity(exprs.len());
        for expr in exprs {
            match expr.as_ref() {
                Expr::ScalarFunction(func) => {
                    if func.name() == "explode" {
                        let inputs = &func.inputs.clone().into_inner();
                        if inputs.len() != 1 {
                            return Err(DaftError::ValueError(format!("ListExpr::Explode function expression must have one input only, received: {}", inputs.len())));
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
                    ))
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
        let take_idx = lengths_to_indices(&first_len, capacity_expected)?.into_series();

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
        Self::from_nonempty_columns(new_series)
    }
}
