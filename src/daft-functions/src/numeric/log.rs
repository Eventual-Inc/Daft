use common_error::{DaftError, DaftResult};
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
            fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
                let UnaryArg { input } = inputs.try_into()?;

                input.$name()
            }

            fn name(&self) -> &'static str {
                stringify!($name)
            }

            fn get_return_field(
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
#[derive(FunctionArgs)]
struct LogArgs<T> {
    input: T,
    base: f64,
}

#[typetag::serde]
impl ScalarUDF for Log {
    fn name(&self) -> &'static str {
        "log"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let LogArgs { input, base } = inputs.try_into()?;

        input.log(base)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let LogArgs { input, base: _ } = inputs.try_into()?;
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
        "Calculates the first argument-based logarithm of the second argument log_x(y)."
    }
}

#[must_use]
pub fn log(input: ExprRef, base: ExprRef) -> ExprRef {
    ScalarFunction::new(Log, vec![input, base]).into()
}
