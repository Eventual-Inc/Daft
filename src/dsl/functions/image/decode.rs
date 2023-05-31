use crate::{datatypes::Field, dsl::Expr, error::DaftResult, schema::Schema, series::Series};

use super::super::FunctionEvaluator;

pub struct DecodeEvaluator {}

impl FunctionEvaluator for DecodeEvaluator {
    fn fn_name(&self) -> &'static str {
        "decode"
    }

    fn to_field(&self, _: &[Expr], _: &Schema) -> DaftResult<Field> {
        todo!("not implemented");
    }

    fn evaluate(&self, _: &[Series]) -> DaftResult<Series> {
        todo!("not implemented");
    }
}
