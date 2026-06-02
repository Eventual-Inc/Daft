use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, UnaryArg, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Md5Function;

#[typetag::serde]
impl ScalarUDF for Md5Function {
    fn name(&self) -> &'static str {
        "md5"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;

        match input.data_type() {
            DataType::Utf8 => {
                let arr = input.utf8()?;
                let out = Utf8Array::from_iter(
                    input.name(),
                    arr.into_iter()
                        .map(|v| v.map(|s| format!("{:x}", ::md5::compute(s.as_bytes())))),
                );
                Ok(out.into_series())
            }
            DataType::Null => Ok(input.clone()),
            dtype => Err(DaftError::TypeError(format!(
                "Expected input to 'md5' to be utf8, but received {}",
                dtype
            ))),
        }
    }

    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        let field = input.to_field(schema)?;

        match field.dtype {
            DataType::Utf8 => Ok(Field::new(field.name, DataType::Utf8)),
            DataType::Null => Ok(Field::new(field.name, DataType::Null)),
            dtype => Err(DaftError::TypeError(format!(
                "Expected input to 'md5' to be utf8, but received {}",
                dtype
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Returns the MD5 digest (hex string) for each UTF-8 input value."
    }
}

#[must_use]
pub fn md5(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Md5Function, vec![input]).into()
}
