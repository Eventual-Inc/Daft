use crate::{
    array::{ops::downcast::Downcastable, BaseArray},
    datatypes::{DataType, UInt64Array},
    dsl::Expr,
    error::{DaftError, DaftResult},
    series::Series,
    table::Table,
};

fn lengths_to_indices(lengths: &UInt64Array, capacity: usize) -> DaftResult<UInt64Array> {
    let mut indices = Vec::with_capacity(capacity);
    for (i, l) in lengths.downcast().iter().enumerate() {
        let l = std::cmp::max(*l.unwrap_or(&1), 1u64);
        (0..l).for_each(|_| indices.push(i as u64));
    }
    Ok(UInt64Array::from(("indices", indices)))
}

impl Table {
    pub fn explode(&self, exprs: &[&Expr]) -> DaftResult<Self> {
        if exprs.len() == 0 {
            return Err(DaftError::ValueError(format!(
                "Explode needs at least 1 expression, received: {}",
                exprs.len()
            )));
        }

        use crate::dsl::functions::{list::ListExpr, FunctionExpr};

        let mut evaluated_columns = Vec::with_capacity(exprs.len());
        for expr in exprs {
            match expr {
                Expr::Function {
                    func: FunctionExpr::List(ListExpr::Explode),
                    inputs,
                } => {
                    if inputs.len() != 1 {
                        return Err(DaftError::ValueError(format!("ListExpr::Explode function expression must have one input only, received: {}", inputs.len())));
                    }
                    let expr = inputs.get(0).unwrap();
                    let exploded_name = expr.name()?;
                    let evaluated = self.eval_expression(expr)?;
                    if !matches!(
                        evaluated.data_type(),
                        DataType::List(..) | DataType::FixedSizeList(..)
                    ) {
                        return Err(DaftError::ValueError(format!(
                            "Evaluated Expression for {exploded_name} is not List Type but is {}",
                            evaluated.data_type()
                        )));
                    }
                    evaluated_columns.push(evaluated);
                    // let lengths = evaluated.lengths()?;
                    // let take_idx = lengths_to_indices(&lengths, exploded.len())?.into_series();
                    // let table_to_repeat = self.get_columns(
                    //     self.column_names()
                    //         .iter()
                    //         .filter(|name| name.as_str().ne(exploded_name))
                    //         .collect::<Vec<&String>>()
                    //         .as_slice(),
                    // )?;
                    // let mut table_to_repeat = table_to_repeat.take(&take_idx)?;
                    // let mut cols: Vec<Series> = table_to_repeat.columns.drain(..).collect();
                    // cols.push(exploded);
                    // Self::from_columns(cols);
                }
                _ => {
                    return Err(DaftError::ValueError(
                        "Can only explode a ListExpr::Explode function expression".to_string(),
                    ))
                }
            }
        }
        let first_len = evaluated_columns.first().unwrap().lengths()?;
        if evaluated_columns
            .iter()
            .skip(1)
            .any(|c| c.lengths().unwrap().ne(&first_len))
        {
            return Err(DaftError::ValueError(format!(
                "When performing multicolumn explode, list lengths did not match up"
            )));
        }
        let exploded_columns = evaluated_columns
            .iter()
            .map(|c| c.explode())
            .collect::<DaftResult<Vec<_>>>()?;
        let capacity_expected = exploded_columns.first().unwrap().len();
        let take_idx = lengths_to_indices(&first_len, capacity_expected)?.into_series();

        let mut new_series = self.columns.clone();
        for c in exploded_columns {
            new_series.re
        }

        Ok(self.clone())
    }
}
