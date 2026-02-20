use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{AsArrow, DataType, Field, Schema, UInt64Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, UnaryArg, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Length;

#[typetag::serde]
impl ScalarUDF for Length {
    fn name(&self) -> &'static str {
        "length"
    }
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;

        Ok(match input.data_type() {
            DataType::Utf8 => {
                let arr = input.utf8()?;

                let field = Field::new(input.name(), DataType::UInt64);
                let output_arr = UInt64Array::from_iter(
                    field,
                    arr.into_iter().map(|val| {
                        let v = val?;
                        Some(v.chars().count() as u64)
                    }),
                );

                output_arr.into_series()
            }
            DataType::Binary => {
                let arr = input.binary()?;

                let length_vec = arr
                    .as_arrow()?
                    .offsets()
                    .lengths()
                    .map(|l| l as u64)
                    .collect::<Vec<_>>();
                let length_arr = UInt64Array::from_vec(input.name(), length_vec)
                    .with_nulls(arr.nulls().cloned().map(Into::into))?;
                length_arr.into_series()
            }
            DataType::List(_) => {
                let list_arr = input.list()?;

                let length_vec = list_arr
                    .offsets()
                    .lengths()
                    .map(|l| l as u64)
                    .collect::<Vec<_>>();
                let length_arr = UInt64Array::from_vec(input.name(), length_vec)
                    .with_nulls(list_arr.nulls().cloned())?;
                length_arr.into_series()
            }
            DataType::FixedSizeBinary(length) | DataType::FixedSizeList(_, length) => {
                let nulls = input.nulls();

                let length_arr =
                    UInt64Array::from_vec(input.name(), vec![*length as u64; input.len()])
                        .with_nulls(nulls.cloned())?;

                length_arr.into_series()
            }
            DataType::Null => input,
            dtype => {
                return Err(DaftError::TypeError(format!(
                    "Expected input to 'length' function to be utf8, binary, or list, but received {}",
                    dtype
                )));
            }
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        let field = input.to_field(schema)?;

        match field.dtype {
            DataType::Utf8
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::List(_)
            | DataType::FixedSizeList(..) => Ok(Field::new(field.name, DataType::UInt64)),
            DataType::Null => Ok(Field::new(field.name, DataType::Null)),
            _ => Err(DaftError::TypeError(format!(
                "Expected input to 'length' function to be utf8, binary, or list, but received {}",
                field.dtype
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Returns the length of the string, binary, or list"
    }
}

#[must_use]
pub fn length(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Length, vec![input]).into()
}
