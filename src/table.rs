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
    fn get_column(&self, name: &str) -> DaftResult<Series> {
        let i = self.schema.get_index(name)?;
        Ok(self.columns.get(i).unwrap().clone())
    }

    fn eval_expression(&self, expr: &Expr) -> DaftResult<Series> {
        let result_field = expr.to_field(self.schema.as_ref())?;
        use Expr::*;
        match expr {
            Alias(child, name) => self.eval_expression(child.as_ref()),
            Column(name) => self.get_column(name),
            BinaryOp { op, left, right } => self
                .eval_expression(&left)?
                .binary_op(&self.eval_expression(&right)?, *op),
            Literal(lit_value) => lit_value.to_series(),
        }
    }
}
