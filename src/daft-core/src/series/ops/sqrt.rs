use common_error::DaftResult;

use crate::{
    datatypes::DataType,
    series::{array_impl::IntoSeries, Series},
};

impl Series {
    pub fn sqrt(&self) -> DaftResult<Self> {
        let casted_dtype = self.to_floating_data_type()?;
        let casted_self = self
            .cast(&casted_dtype)
            .expect("Casting numeric types to their floating point analogues should not fail");
        match casted_dtype {
            DataType::Float32 => Ok(casted_self.f32().unwrap().sqrt()?.into_series()),
            DataType::Float64 => Ok(casted_self.f64().unwrap().sqrt()?.into_series()),
            _ => unreachable!(),
        }
    }
}
