use crate::{
    array::{ops::downcast::Downcastable, BaseArray},
    datatypes::UInt64Array,
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
    pub fn explode(&self, expr: &Expr) -> DaftResult<Self> {
        use crate::dsl::functions::{list::ListExpr, FunctionExpr};
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
                let exploded = evaluated.explode()?;
                let lengths = evaluated.lengths()?;
                let take_idx = lengths_to_indices(&lengths, exploded.len())?.into_series();
                let table_to_repeat = self.get_columns(
                    self.column_names()
                        .iter()
                        .filter(|name| name.as_str().ne(exploded_name))
                        .collect::<Vec<&String>>()
                        .as_slice(),
                )?;
                let mut table_to_repeat = table_to_repeat.take(&take_idx)?;
                let mut cols: Vec<Series> = table_to_repeat.columns.drain(..).collect();
                cols.push(exploded);
                Self::from_columns(cols)
            }
            _ => Err(DaftError::ValueError(
                "Can only explode a ListExpr::Explode function expression".to_string(),
            )),
        }
    }
}
