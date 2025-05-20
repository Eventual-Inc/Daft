use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF, UnaryArg},
    ExprRef,
};
use serde::{Deserialize, Serialize};

// super annoying, but using an enum with typetag::serde doesn't work with bincode because it uses Deserializer::deserialize_identifier
macro_rules! log {
    ($name:ident, $variant:ident, $docstring:literal) => {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
        pub struct $variant;

        #[typetag::serde]
        impl ScalarUDF for $variant {
            fn evaluate(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
                let UnaryArg { input } = inputs.try_into()?;

                input.$name()
            }

            fn name(&self) -> &'static str {
                stringify!($name)
            }

            fn function_args_to_field(
                &self,
                inputs: FunctionArgs<ExprRef>,
                schema: &Schema,
            ) -> DaftResult<Field> {
                let UnaryArg { input } = inputs.try_into()?;
                let field = input.to_field(schema)?;
                let dtype = match field.dtype {
                    DataType::Float32 => DataType::Float32,
                    dt if dt.is_numeric() => DataType::Float64,
                    _ => {
                        return Err(DaftError::TypeError(format!(
                            "Expected input to log to be numeric, got {}",
                            field.dtype
                        )))
                    }
                };
                Ok(Field::new(field.name, dtype))
            }

            fn docstring(&self) -> &'static str {
                $docstring
            }
        }

        #[must_use]
        pub fn $name(input: ExprRef) -> ExprRef {
            ScalarFunction::new($variant, vec![input]).into()
        }
    };
}

log!(log2, Log2, "Calculates the base-2 logarithm of a number.");
log!(
    log10,
    Log10,
    "Calculates the base-10 logarithm of a number."
);
log!(ln, Ln, "Calculates the natural logarithm of a number.");
log!(
    log1p,
    Log1p,
    "Calculates the natural logarithm of a number plus one (ln(x + 1))."
);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Log;

#[typetag::serde]
impl ScalarUDF for Log {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inner = inputs.into_inner();
        self.evaluate_from_series(&inner)
    }

    fn name(&self) -> &'static str {
        "log"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        ensure!(inputs.len() == 2, "log takes two arguments");
        let field = inputs.first().unwrap().to_field(schema)?;
        let base = inputs.get(1).unwrap().to_field(schema)?;
        if !base.dtype.is_numeric() {
            return Err(DaftError::TypeError(format!(
                "Expected base to log to be numeric, got {}",
                base.dtype
            )));
        }

        let dtype = match field.dtype {
            DataType::Float32 => DataType::Float32,
            dt if dt.is_numeric() => DataType::Float64,
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Expected input to log to be numeric, got {}",
                    field.dtype
                )))
            }
        };
        Ok(Field::new(field.name, dtype))
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        ensure!(inputs.len() == 2, "log takes two arguments");
        let input = &inputs[0];
        let base = &inputs[1];
        let base = {
            ensure!(base.len() == 1, "expected scalar value");
            let s = base.cast(&DataType::Float64)?;

            s.f64().unwrap().get(0).unwrap()
        };
        input.log(base)
    }
    fn docstring(&self) -> &'static str {
        "Calculates the first argument-based logarithm of the second argument log_x(y)."
    }
}

#[must_use]
pub fn log(input: ExprRef, base: ExprRef) -> ExprRef {
    ScalarFunction::new(Log, vec![input, base]).into()
}
