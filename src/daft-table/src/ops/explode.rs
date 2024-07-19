use common_error::{DaftError, DaftResult};
use daft_core::series::IntoSeries;
use daft_core::CountMode;
use daft_core::{
    array::ops::as_arrow::AsArrow,
    datatypes::{DataType, UInt64Array},
    series::Series,
};
use daft_dsl::Expr;

use crate::Table;

fn lengths_to_indices(lengths: &UInt64Array, capacity: usize) -> DaftResult<UInt64Array> {
    let mut indices = Vec::with_capacity(capacity);
    for (i, l) in lengths.as_arrow().iter().enumerate() {
        let l = std::cmp::max(*l.unwrap_or(&1), 1u64);
        (0..l).for_each(|_| indices.push(i as u64));
    }
    Ok(UInt64Array::from(("indices", indices)))
}

impl Table {
    pub fn explode<E: AsRef<Expr>>(&self, exprs: &[E]) -> DaftResult<Self> {
        if exprs.is_empty() {
            return Err(DaftError::ValueError(format!(
                "Explode needs at least 1 expression, received: {}",
                exprs.len()
            )));
        }

        use daft_dsl::functions::{list::ListExpr, FunctionExpr};

        let mut evaluated_columns = Vec::with_capacity(exprs.len());
        for expr in exprs {
            match expr.as_ref() {
                Expr::Function {
                    func: FunctionExpr::List(ListExpr::Explode),
                    inputs,
                } => {
                    if inputs.len() != 1 {
                        return Err(DaftError::ValueError(format!("ListExpr::Explode function expression must have one input only, received: {}", inputs.len())));
                    }
                    let expr = inputs.first().unwrap();
                    let exploded_name = expr.name();
                    let evaluated = self.eval_expression(expr)?;
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
            .map(|c| c.explode())
            .collect::<DaftResult<Vec<_>>>()?;

        let capacity_expected = exploded_columns.first().unwrap().len();
        let take_idx = lengths_to_indices(&first_len, capacity_expected)?.into_series();

        let mut new_series = self.columns.clone();

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
