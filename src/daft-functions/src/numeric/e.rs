use std::sync::Arc;

use arrow_array::Float64Array;
use daft_common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct EulersNumber;

#[typetag::serde]
impl ScalarUDF for EulersNumber {
    fn name(&self) -> &'static str {
        "e"
    }

    fn call(
        &self,
        _inputs: FunctionArgs<Series>,
        ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let len = ctx.row_count;
        let arrow_arr: arrow_array::ArrayRef =
            Arc::new(Float64Array::from(vec![std::f64::consts::E; len]));
        Series::from_arrow(Arc::new(Field::new("", DataType::Float64)), arrow_arr)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        _schema: &Schema,
    ) -> DaftResult<Field> {
        if !inputs.is_empty() {
            return Err(daft_common_error::DaftError::ValueError(format!(
                "Expected 0 input args, got {}",
                inputs.len()
            )));
        }
        Ok(Field::new("", DataType::Float64))
    }

    fn docstring(&self) -> &'static str {
        "Returns Euler's number (e = 2.71828...)."
    }
}

#[must_use]
pub fn e() -> ExprRef {
    ScalarFn::builtin(EulersNumber {}, vec![]).into()
}
