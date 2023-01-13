use std::sync::Arc;

use crate::dsl::Expr;
use crate::error::DaftResult;
use crate::schema::Schema;
use crate::series::Series;

pub struct Table {
    schema: Arc<Schema>,
    columns: Vec<Series>,
}

impl Table {
    pub fn new(schema: Schema, columns: &[Series]) -> Self {
        Table {
            schema: schema.into(),
            columns: columns.into(),
        }
    }

    fn get_column(&self, name: &str) -> DaftResult<Series> {
        let i = self.schema.get_index(name)?;
        Ok(self.columns.get(i).unwrap().clone())
    }

    fn eval_expression(&self, expr: &Expr) -> DaftResult<Series> {
        use crate::dsl::Expr::*;
        match expr {
            Alias(child, name) => self.eval_expression(child.as_ref()),
            Column(name) => self.get_column(name),
            BinaryOp { op, left, right } => {
                let lhs = self.eval_expression(&left)?;
                let rhs = self.eval_expression(&right)?;
                use crate::dsl::Operator::*;
                match op {
                    Plus => Ok(lhs + rhs),
                    Minus => Ok(lhs - rhs),
                    Divide => Ok(lhs / rhs),
                    Multiply => Ok(lhs * rhs),
                    Modulus => Ok(lhs % rhs),
                    _ => panic!("{:?} not supported", op),
                }
            }
            Literal(lit_value) => Ok(lit_value.to_series()),
        }
    }
}

#[cfg(test)]
mod test {

    use crate::array::BaseArray;
    use crate::datatypes::{DataType, Int64Array};
    use crate::dsl::col;
    use crate::schema::Schema;
    use crate::table::Table;
    use crate::{datatypes::Float64Array, error::DaftResult};
    #[test]
    fn add_int_and_float_expression() -> DaftResult<()> {
        let a = Int64Array::from(vec![1, 2, 3].as_slice()).into_series();
        let b = Float64Array::from(vec![1., 2., 3.].as_slice()).into_series();
        let schema = Schema::new(
            vec![
                ("a".into(), a.data_type().clone()),
                ("b".into(), b.data_type().clone()),
            ]
            .as_slice(),
        );
        let table = Table::new(schema, [a, b].as_slice());
        let result = table.eval_expression(&(col("a") + col("b")))?;
        println!("{:?}", result.len());
        assert_eq!(*result.data_type(), DataType::Float64);
        Ok(())
    }
}
