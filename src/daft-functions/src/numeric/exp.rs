use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use super::evaluate_single_numeric;

macro_rules! exp {
    ($name:ident, $impl:ident, $variant:ident) => {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
        pub struct $variant;

        #[typetag::serde]
        impl ScalarUDF for $variant {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn name(&self) -> &'static str {
                stringify!($name)
            }

            fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
                if inputs.len() != 1 {
                    return Err(DaftError::SchemaMismatch(format!(
                        "Expected 1 input arg, got {}",
                        inputs.len()
                    )));
                };
                let field = inputs.first().unwrap().to_field(schema)?;
                let dtype = match field.dtype {
                    DataType::Float32 => DataType::Float32,
                    dt if dt.is_numeric() => DataType::Float64,
                    _ => {
                        return Err(DaftError::TypeError(format!(
                            "Expected input to compute exp to be numeric, got {}",
                            field.dtype
                        )))
                    }
                };
                Ok(Field::new(field.name, dtype))
            }

            fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
                evaluate_single_numeric(inputs, $impl)
            }
        }

        #[must_use]
        pub fn $name(input: ExprRef) -> ExprRef {
            ScalarFunction::new($variant, vec![input]).into()
        }
    };
}

exp!(exp, exp_impl, Exp);
exp!(expm1, expm1_impl, Expm1);

fn exp_impl(s: &Series) -> DaftResult<Series> {
    match s.data_type() {
        DataType::Float32 => Ok(s.f32().unwrap().exp()?.into_series()),
        DataType::Float64 => Ok(s.f64().unwrap().exp()?.into_series()),
        dt if dt.is_integer() => exp_impl(&s.cast(&DataType::Float64).unwrap()),
        dt => Err(DaftError::TypeError(format!(
            "exp not implemented for {}",
            dt
        ))),
    }
}

fn expm1_impl(s: &Series) -> DaftResult<Series> {
    match s.data_type() {
        DataType::Float32 => Ok(s.f32().unwrap().expm1()?.into_series()),
        DataType::Float64 => Ok(s.f64().unwrap().expm1()?.into_series()),
        dt if dt.is_integer() => expm1_impl(&s.cast(&DataType::Float64).unwrap()),
        dt => Err(DaftError::TypeError(format!(
            "expm1 not implemented for {}",
            dt
        ))),
    }
}
