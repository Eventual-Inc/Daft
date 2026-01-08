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
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;

        Ok(match input.data_type() {
            DataType::Utf8 => {
                let arrow_arr = input.utf8()?.as_arrow2();

                let field = Field::new(input.name(), DataType::UInt64);
                let output_arr = UInt64Array::from_iter(
                    field,
                    arrow_arr.iter().map(|val| {
                        let v = val?;
                        Some(v.chars().count() as u64)
                    }),
                );

                output_arr.into_series()
            }
            DataType::Binary => {
                let arrow_arr = input.binary()?.as_arrow2();

                let length_vec = arrow_arr
                    .offsets()
                    .lengths()
                    .map(|l| l as u64)
                    .collect::<Vec<_>>();
                let length_arr = UInt64Array::from((input.name(), length_vec))
                    .with_validity(arrow_arr.validity().cloned().map(Into::into))?;
                length_arr.into_series()
            }
            DataType::List(_) => {
                let list_arr = input.list()?;

                let length_vec = list_arr
                    .offsets()
                    .lengths()
                    .map(|l| l as u64)
                    .collect::<Vec<_>>();
                let length_arr = UInt64Array::from((input.name(), length_vec))
                    .with_validity(list_arr.validity().cloned())?;
                length_arr.into_series()
            }
            DataType::FixedSizeBinary(length) | DataType::FixedSizeList(_, length) => {
                let validity = input.validity();

                let length_arr =
                    UInt64Array::from((input.name(), vec![*length as u64; input.len()]))
                        .with_validity(validity.cloned())?;

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

        // let (offsets, validity) = match input.data_type() {
        //     DataType::Binary => {
        //         let arrow_arr = input.binary()?.as_arrow2();
        //         (arrow_arr.offsets(), arrow_arr.validity())
        //     }
        //     DataType::List(_) => {
        //         let list_arr = input.list()?;
        //         (list_arr.offsets(), list_arr.validity())
        //     }
        //     DataType::FixedSizeBinary(length) | DataType::FixedSizeList(_, length) => {
        //         let validity = input.validity();

        //         let length_arr =
        //             UInt64Array::from((input.name(), vec![*length as u64; input.len()]))
        //                 .with_validity(validity.cloned())?;

        //         return Ok(length_arr.into_series());
        //     }
        //     DataType::Utf8 => {
        //         let arrow_arr = input.utf8()?.as_arrow2();
        //         (arrow_arr.offsets(), arrow_arr.validity())
        //     }
        //     DataType::Null => return Ok(input),
        //     dtype => {
        //         return Err(DaftError::TypeError(format!(
        //             "Expected input to 'length' function to be utf8, binary, or list, but received {}",
        //             dtype
        //         )));
        //     }
        // };

        // let length_vec = offsets.lengths().map(|l| l as u64).collect::<Vec<_>>();
        // let length_arr =
        //     UInt64Array::from((input.name(), length_vec)).with_validity(validity.cloned())?;
        // Ok(length_arr.into_series())
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
