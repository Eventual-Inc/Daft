use crate::array::ops::trigonometry::TrigonometricFunction;
use crate::array::ops::DaftAtan2;
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

    pub fn atan2(&self, rhs: &Self) -> DaftResult<Self> {
        match (self.data_type(), rhs.data_type()) {
            (DataType::Float32, DataType::Float32) => {
                let lhs_ca = self.f32().unwrap();
                let rhs_ca = rhs.f32().unwrap();
                Ok(lhs_ca.atan2(rhs_ca)?.into_series())
            }
            (DataType::Float64, DataType::Float64) => {
                let lhs_ca = self.f64().unwrap();
                let rhs_ca = rhs.f64().unwrap();
                Ok(lhs_ca.atan2(rhs_ca)?.into_series())
            }
            // avoid extra casting if one side is already f64
            (DataType::Float64, rhs_dt) if rhs_dt.is_numeric() => {
                let rhs_s = rhs.cast(&DataType::Float64)?;
                self.atan2(&rhs_s)
            }
            (lhs_dt, DataType::Float64) if lhs_dt.is_numeric() => {
                let lhs_s = self.cast(&DataType::Float64)?;
                lhs_s.atan2(rhs)
            }
            (lhs_dt, rhs_dt) if lhs_dt.is_numeric() && rhs_dt.is_numeric() => {
                let lhs_s = self.cast(&DataType::Float64)?;
                let rhs_s = rhs.cast(&DataType::Float64)?;
                lhs_s.atan2(&rhs_s)
            }
            (lhs_dt, rhs_dt) => Err(DaftError::TypeError(format!(
                "Expected inputs to trigonometry to be numeric, got {} and {}",
                lhs_dt, rhs_dt
            ))),
        }
    }
}
