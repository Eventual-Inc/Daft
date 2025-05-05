use common_error::{DaftError, DaftResult};
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
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "clip"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        if inputs.len() != 3 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input arguments (array, min, max), got {}",
                inputs.len()
            )));
        }
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

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        if inputs.len() != 3 {
            return Err(DaftError::ValueError(format!(
                "Expected 3 input arguments (array, min, max), got {}",
                inputs.len()
            )));
        }
        let array = &inputs[0];
        let min = &inputs[1];
        let max = &inputs[2];

        clip_impl(array, min, max)
    }
}

#[must_use]
pub fn clip(array: ExprRef, min: ExprRef, max: ExprRef) -> ExprRef {
    ScalarFunction::new(Clip, vec![array, min, max]).into()
}

fn clip_impl(arr: &Series, min: &Series, max: &Series) -> DaftResult<Series> {
    let output_type = InferDataType::clip_op(
        &InferDataType::from(arr.data_type()),
        &InferDataType::from(min.data_type()),
        &InferDataType::from(max.data_type()),
    )?;

    // It's possible that we pass in something like .clip(None, 2) on the Python binding side,
    // in which case we need to cast the None to the output type.
    let create_null_series = |name: &str| Series::full_null(name, &output_type, 1);
    let min = if min.data_type().is_null() {
        create_null_series(min.name())
    } else {
        min.clone()
    };
    let max = if max.data_type().is_null() {
        create_null_series(max.name())
    } else {
        max.clone()
    };

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
