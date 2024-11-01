use common_error::{DaftError, DaftResult};
use daft_schema::prelude::*;

use crate::{
    datatypes::InferDataType,
    series::{IntoSeries, Series},
};

// clip helper

macro_rules! cast_and_clip {
    ($self:expr, $min:expr, $max:expr, $output_type:expr, $method:ident) => {{
        let self_casted = $self.cast(&$output_type)?;
        let min_casted = $min.cast(&$output_type)?;
        let max_casted = $max.cast(&$output_type)?;
        Ok(self_casted
            .$method()?
            .clip(min_casted.$method()?, max_casted.$method()?)?
            .into_series())
    }};
}

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

        // It's possible that we pass in something like .clamp(None, 2) on the Python binding side, in which case we need to replace the None (which gets casted into a Series with Null type) with a dummy series of the output type.
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
            // DataType::Int8 => cast_and_clip!(self, min, max, output_type, i8),
            DataType::Int8 => cast_and_clip!(self, min, max, output_type, i8),
            DataType::Int16 => cast_and_clip!(self, min, max, output_type, i16),
            DataType::Int32 => cast_and_clip!(self, min, max, output_type, i32),
            DataType::Int64 => cast_and_clip!(self, min, max, output_type, i64),
            DataType::UInt8 => cast_and_clip!(self, min, max, output_type, u8),
            DataType::UInt16 => cast_and_clip!(self, min, max, output_type, u16),
            DataType::UInt32 => cast_and_clip!(self, min, max, output_type, u32),
            DataType::UInt64 => cast_and_clip!(self, min, max, output_type, u64),
            DataType::Float32 => cast_and_clip!(self, min, max, output_type, f32),
            DataType::Float64 => cast_and_clip!(self, min, max, output_type, f64),
            dt => Err(DaftError::TypeError(format!(
                "clip not implemented for {}",
                dt
            ))),
        }
    }
}
