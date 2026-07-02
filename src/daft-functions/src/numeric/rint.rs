use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, UnaryArg, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Rint;

#[typetag::serde]
impl ScalarUDF for Rint {
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        rint_impl(&input)
    }

    fn name(&self) -> &'static str {
        "rint"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        let dtype = field.dtype.to_floating_representation()?;
        Ok(Field::new(field.name, dtype))
    }

    fn docstring(&self) -> &'static str {
        "Rounds a number to the nearest integer, returning a float. Ties round to the nearest even integer (banker's rounding)."
    }
}

#[must_use]
pub fn rint(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Rint {}, vec![input]).into()
}

fn rint_impl(s: &Series) -> DaftResult<Series> {
    match s.data_type() {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => s.clone().cast(&s.to_floating_data_type()?),
        DataType::Float16 => {
            let ca = s.f16().unwrap();
            ca.apply(|v| {
                let f = f32::from(v);
                half::f16::from_f32(f.round_ties_even())
            })
            .map(|arr| arr.into_series())
        }
        DataType::Float32 => {
            let ca = s.f32().unwrap();
            Ok(ca.apply(|v| v.round_ties_even())?.into_series())
        }
        DataType::Float64 => {
            let ca = s.f64().unwrap();
            Ok(ca.apply(|v| v.round_ties_even())?.into_series())
        }
        dt => Err(DaftError::TypeError(format!(
            "rint not implemented for {}",
            dt
        ))),
    }
}