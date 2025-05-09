use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    datatypes::InferDataType,
    prelude::{Field, Schema},
    series::{IntoSeries, Series},
    with_match_numeric_daft_types,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Clip;

#[typetag::serde]
impl ScalarUDF for Clip {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(
            inputs.len() == 2 || inputs.len() == 3,
            ComputeError: "clip takes exactly two or three arguments"
        );

        let arr = inputs.required((0, "input"))?;
        let arr_dtype = arr.data_type();
        let null_lit = |name: &str| Series::full_null(name, arr_dtype, 1);

        let min = inputs
            .optional((1, "min"))?
            .cloned()
            .unwrap_or_else(|| null_lit("min"));

        let max = inputs
            .optional((2, "max"))?
            .cloned()
            .unwrap_or_else(|| null_lit("max"));

        let output_type = InferDataType::clip_op(
            &InferDataType::from(arr.data_type()),
            &InferDataType::from(min.data_type()),
            &InferDataType::from(max.data_type()),
        )?;
        match &output_type {
            output_type if output_type.is_numeric() => {
                with_match_numeric_daft_types!(output_type, |$T| {
                    let self_casted = arr.cast(output_type)?;
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

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        ensure!(
            inputs.len() == 3,
            SchemaMismatch: "clip takes exactly three arguments (input, min, max) got {}", inputs.len()
        );

        let array_field = inputs[0].to_field(schema)?;
        let min_field = inputs[1].to_field(schema)?;
        let max_field = inputs[2].to_field(schema)?;

        let output_type = InferDataType::clip_op(
            &InferDataType::from(&array_field.dtype),
            &InferDataType::from(&min_field.dtype),
            &InferDataType::from(&max_field.dtype),
        )?;

        Ok(Field::new(array_field.name, output_type))
    }
    fn docstring(&self) -> &'static str {
        "Clips a number to a specified range. If left bound is None, no lower clipping is applied. If right bound is None, no upper clipping is applied. Panics if right bound < left bound."
    }
}

#[must_use]
pub fn clip(array: ExprRef, min: ExprRef, max: ExprRef) -> ExprRef {
    ScalarFunction::new(Clip, vec![array, min, max]).into()
}
