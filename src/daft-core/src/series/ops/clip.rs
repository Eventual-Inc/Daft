use common_error::{DaftError, DaftResult};

use crate::{
    datatypes::InferDataType,
    series::{IntoSeries, Series},
    with_match_numeric_daft_types,
};

impl Series {
    /// Alias for .clip()
    pub fn clamp(&self, min: &Self, max: &Self) -> DaftResult<Self> {
        self.clip(min, max)
    }

    /// Clip function to clamp values to a range
    pub fn clip(&self, min: &Self, max: &Self) -> DaftResult<Self> {
        let output_type = InferDataType::clip_op(
            &InferDataType::from(self.data_type()),
            &InferDataType::from(min.data_type()),
            &InferDataType::from(max.data_type()),
        )?;

        // It's possible that we pass in something like .clamp(None, 2) on the Python binding side,
        // in which case we need to cast the None to the output type.
        let create_null_series = |name: &str| Self::full_null(name, &output_type, 1);
        let min = min
            .data_type()
            .is_null()
            .then(|| create_null_series(min.name()))
            .unwrap_or_else(|| min.clone());
        let max = max
            .data_type()
            .is_null()
            .then(|| create_null_series(max.name()))
            .unwrap_or_else(|| max.clone());

        match &output_type {
            output_type if output_type.is_numeric() => {
                with_match_numeric_daft_types!(output_type, |$T| {
                    let self_casted = self.cast(output_type)?;
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
}
