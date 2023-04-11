use crate::{datatypes::Field, dsl::Expr, error::DaftResult, schema::Schema, series::Series};

use super::super::FunctionEvaluator;
use super::PythonExpr;

impl FunctionEvaluator for PythonExpr {
    fn fn_name(&self) -> &'static str {
        "py_udf"
    }

    fn to_field(&self, _inputs: &[Expr], _schema: &Schema) -> DaftResult<Field> {
        todo!()
    }

    fn evaluate(&self, _inputs: &[Series]) -> DaftResult<Series> {
        todo!()
    }
}
