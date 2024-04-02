use crate::datatypes::DataType;
use crate::series::Series;
use common_error::DaftError;
use common_error::DaftResult;

impl Series {
    pub fn round(&self, decimal: i32) -> DaftResult<Series> {
        use crate::series::array_impl::IntoSeries;
        use DataType::*;
        match self.data_type() {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => Ok(self.clone()),
            Float32 => Ok(self.f32().unwrap().round(decimal)?.into_series()),
            Float64 => Ok(self.f64().unwrap().round(decimal)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "round not implemented for {}",
                dt
            ))),
        }
    }
}
