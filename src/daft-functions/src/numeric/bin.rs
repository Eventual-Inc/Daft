use daft_common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::Utf8Array,
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, UnaryArg, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Bin;

#[typetag::serde]
impl ScalarUDF for Bin {
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        bin_impl(input)
    }

    fn name(&self) -> &'static str {
        "bin"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        if !field.dtype.is_integer() {
            return Err(DaftError::TypeError(format!(
                "Expected input to bin to be integer, got {}",
                field.dtype
            )));
        }
        Ok(Field::new(field.name, DataType::Utf8))
    }

    fn docstring(&self) -> &'static str {
        "Returns the string representation of the binary value of an integer."
    }
}

fn bin_impl(s: Series) -> DaftResult<Series> {
    let result = if matches!(s.data_type(), DataType::UInt64) {
        let arr = s.u64().unwrap();
        Utf8Array::from_iter(
            arr.name(),
            arr.iter().map(|v| v.map(|n| format!("{:b}", n))),
        )
    } else {
        let casted = s.cast(&DataType::Int64)?;
        let arr = casted.i64().unwrap();
        // `as u64` matches PySpark: negatives use 64-bit two's complement.
        Utf8Array::from_iter(
            arr.name(),
            arr.iter().map(|v| v.map(|n| format!("{:b}", n as u64))),
        )
    };
    Ok(result.into_series())
}

#[must_use]
pub fn bin(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Bin {}, vec![input]).into()
}
