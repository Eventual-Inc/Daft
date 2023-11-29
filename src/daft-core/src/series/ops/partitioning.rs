use crate::datatypes::logical::TimestampArray;
use crate::datatypes::{Int32Array, Int64Array, TimeUnit};
use crate::series::array_impl::IntoSeries;
use crate::{
    datatypes::{logical::DateArray, DataType},
    series::Series,
};
use common_error::{DaftError, DaftResult};

impl Series {
    pub fn partitioning_years(&self) -> DaftResult<Self> {
        let years_since_ce = self.dt_year()?;
        let epoch_year = Int32Array::from(("1970", vec![1970])).into_series();
        &years_since_ce - &epoch_year
    }

    pub fn partitioning_months(&self) -> DaftResult<Self> {
        let years_since_1970 = self.partitioning_years()?;
        let months_of_this_year = self.dt_month()?;
        let months_in_year = Int32Array::from(("months", vec![12])).into_series();
        let month_of_epoch = Int32Array::from(("months", vec![1])).into_series();

        ((&years_since_1970 * &months_in_year)? + months_of_this_year)? - month_of_epoch
    }

    pub fn partitioning_days(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Date => {
                let downcasted = self.downcast::<DateArray>()?;
                downcasted.cast(&DataType::Int32)
            }
            DataType::Timestamp(..) => {
                let ts_array = self.downcast::<TimestampArray>()?;
                ts_array.date()?.cast(&DataType::Int32)
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run partitioning_days() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }
    pub fn partitioning_hours(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Timestamp(unit, _) => {
                let ts_array = self.downcast::<TimestampArray>()?;
                let physical = &ts_array.physical;
                let unit_to_hours: i64 = match unit {
                    TimeUnit::Nanoseconds => 3_600_000_000_000,
                    TimeUnit::Microseconds => 3_600_000_000,
                    TimeUnit::Milliseconds => 3_600_000,
                    TimeUnit::Seconds => 3_600,
                };
                let divider = Int64Array::from(("divider", vec![unit_to_hours]));
                let hours = (physical / &divider)?;
                Ok(hours.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run partitioning_hours() operation on timestamp types, got {}",
                self.data_type()
            ))),
        }
    }
}
