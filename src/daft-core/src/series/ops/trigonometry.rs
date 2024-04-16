use crate::array::ops::trigonometry::TrigonometricFunction;
use crate::datatypes::DataType;
use crate::series::Series;
use crate::IntoSeries;
use common_error::DaftError;
use common_error::DaftResult;

impl Series {
    pub fn trigonometry(&self, trig_function: &TrigonometricFunction) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Float32 => {
                let ca = self.f32().unwrap();
                Ok(ca.trigonometry(trig_function)?.into_series())
            }
            DataType::Float64 => {
                let ca = self.f64().unwrap();
                Ok(ca.trigonometry(trig_function)?.into_series())
            }
            dt if dt.is_numeric() => {
                let s = self.cast(&DataType::Float64)?;
                let ca = s.f64().unwrap();
                Ok(ca.trigonometry(trig_function)?.into_series())
            }
            dt => Err(DaftError::TypeError(format!(
                "Expected input to trigonometry to be numeric, got {}",
                dt
            ))),
        }
    }
}
