use common_error::DaftError;
use common_error::DaftResult;

use crate::datatypes::DataType;
use crate::series::Series;

impl Series {
    pub fn exp(&self) -> DaftResult<Series> {
        use crate::series::array_impl::IntoSeries;

        use DataType::*;
        match self.data_type() {
            Float32 => Ok(self.f32().unwrap().exp()?.into_series()),
            Float64 => Ok(self.f64().unwrap().exp()?.into_series()),
            dt if dt.is_integer() => self.cast(&Float64).unwrap().exp(),
            dt => Err(DaftError::TypeError(format!(
                "exp not implemented for {}",
                dt
            ))),
        }
    }
}
