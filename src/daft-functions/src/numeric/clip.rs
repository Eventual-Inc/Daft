use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::InferDataType,
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
    with_match_numeric_daft_types,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Clip;

#[derive(FunctionArgs)]
struct ClipArgs<T> {
    input: T,
    #[arg(optional)]
    min: Option<T>,
    #[arg(optional)]
    max: Option<T>,
}

#[typetag::serde]
impl ScalarUDF for Clip {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let ClipArgs { input, min, max } = inputs.try_into()?;

        let input_dtype = input.data_type();
        let null_lit = |name: &str| Series::full_null(name, input_dtype, 1);

        let min = min.unwrap_or_else(|| null_lit("min"));
        let max = max.unwrap_or_else(|| null_lit("max"));

        let output_type = InferDataType::clip_op(
            &InferDataType::from(input.data_type()),
            &InferDataType::from(min.data_type()),
            &InferDataType::from(max.data_type()),
        )?;
        match &output_type {
            output_type if output_type.is_numeric() => {
                with_match_numeric_daft_types!(output_type, |$T| {
                    let self_casted = input.cast(output_type)?;
                    let min_casted = min.cast(output_type)?;
                    let max_casted = max.cast(output_type)?;

                    let self_downcasted = self_casted.downcast::<<$T as DaftDataType>::ArrayType>()?;
                    let min_downcasted = min_casted.downcast::<<$T as DaftDataType>::ArrayType>()?;
                    let max_downcasted = max_casted.downcast::<<$T as DaftDataType>::ArrayType>()?;
                    Ok(self_downcasted.clip(min_downcasted, max_downcasted)?.into_series())
                })
            }
            dt => Err(DaftError::TypeError(format!(
                "clip not implemented for {}",
                dt
            ))),
        }
    }

    fn name(&self) -> &'static str {
        "clip"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let ClipArgs { input, min, max } = inputs.try_into()?;

        let input_field = input.to_field(schema)?;
        let min_field = min.map_or(Ok(Field::new("min", DataType::Null)), |min| {
            min.to_field(schema)
        })?;
        let max_field = max.map_or(Ok(Field::new("max", DataType::Null)), |max| {
            max.to_field(schema)
        })?;

        let output_type = InferDataType::clip_op(
            &InferDataType::from(&input_field.dtype),
            &InferDataType::from(&min_field.dtype),
            &InferDataType::from(&max_field.dtype),
        )?;

        Ok(Field::new(input_field.name, output_type))
    }
    fn docstring(&self) -> &'static str {
        "Clips a number to a specified range. If left bound is None, no lower clipping is applied. If right bound is None, no upper clipping is applied. Panics if right bound < left bound."
    }
}

#[must_use]
pub fn clip(array: ExprRef, min: ExprRef, max: ExprRef) -> ExprRef {
    ScalarFn::builtin(Clip, vec![array, min, max]).into()
}
