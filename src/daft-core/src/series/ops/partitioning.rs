use crate::array::ops::as_arrow::AsArrow;
use crate::datatypes::logical::TimestampArray;
use crate::datatypes::{Int32Array, Int64Array, TimeUnit};
use crate::series::array_impl::IntoSeries;
use crate::with_match_integer_daft_types;
use crate::{datatypes::DataType, series::Series};
use common_error::{DaftError, DaftResult};

impl Series {
    pub fn partitioning_years(&self) -> DaftResult<Self> {
        let epoch_year = Int32Array::from(("1970", vec![1970])).into_series();

        let value = match self.data_type() {
            DataType::Date | DataType::Timestamp(_, None) => {
                let years_since_ce = self.dt_year()?;
                &years_since_ce - &epoch_year
            }
            DataType::Timestamp(tu, Some(_)) => {
                let array = self.cast(&DataType::Timestamp(*tu, None))?;
                let years_since_ce = array.dt_year()?;
                &years_since_ce - &epoch_year
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run partitioning_years() operation on temporal types, got {}",
                self.data_type()
            ))),
        }?;
        value
            .rename(format!("{}_years", self.name()))
            .cast(&DataType::Int32)
    }

    pub fn partitioning_months(&self) -> DaftResult<Self> {
        let months_in_year = Int32Array::from(("months", vec![12])).into_series();
        let month_of_epoch = Int32Array::from(("months", vec![1])).into_series();
        let value = match self.data_type() {
            DataType::Date | DataType::Timestamp(_, None) => {
                let years_since_1970 = self.partitioning_years()?;
                let months_of_this_year = self.dt_month()?;
                ((&years_since_1970 * &months_in_year)? + months_of_this_year)? - month_of_epoch
            }
            DataType::Timestamp(tu, Some(_)) => {
                let array = self.cast(&DataType::Timestamp(*tu, None))?;
                let years_since_1970 = array.partitioning_years()?;
                let months_of_this_year = array.dt_month()?;
                ((&years_since_1970 * &months_in_year)? + months_of_this_year)? - month_of_epoch
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run partitioning_years() operation on temporal types, got {}",
                self.data_type()
            ))),
        }?;
        value
            .rename(format!("{}_months", self.name()))
            .cast(&DataType::Int32)
    }

    pub fn partitioning_days(&self) -> DaftResult<Self> {
        let result = match self.data_type() {
            DataType::Date => Ok(self.clone()),
            DataType::Timestamp(_, None) => {
                let ts_array = self.downcast::<TimestampArray>()?;
                Ok(ts_array.date()?.into_series())
            }

            DataType::Timestamp(tu, Some(_)) => {
                let array = self.cast(&DataType::Timestamp(*tu, None))?;
                let ts_array = array.downcast::<TimestampArray>()?;
                Ok(ts_array.date()?.into_series())
            }

            _ => Err(DaftError::ComputeError(format!(
                "Can only run partitioning_days() operation on temporal types, got {}",
                self.data_type()
            ))),
        }?;

        Ok(result.rename(format!("{}_days", self.name())))
    }

    pub fn partitioning_hours(&self) -> DaftResult<Self> {
        let value = match self.data_type() {
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
        }?;
        value
            .rename(format!("{}_hours", self.name()))
            .cast(&DataType::Int32)
    }

    pub fn partitioning_iceberg_bucket(&self, n: i32) -> DaftResult<Self> {
        assert!(n >= 0, "Expected n to be non negative, got {n}");
        let hashes = self.murmur3_32()?;
        let buckets = hashes
            .as_arrow()
            .into_iter()
            .map(|v| v.map(|v| (v & i32::MAX) % n));
        let array = Box::new(arrow2::array::Int32Array::from_iter(buckets));
        Ok(Int32Array::from((format!("{}_bucket", self.name()).as_str(), array)).into_series())
    }

    pub fn partitioning_iceberg_truncate(&self, w: i64) -> DaftResult<Self> {
        assert!(w > 0, "Expected w to be positive, got {w}");
        let trunc = match self.data_type() {
            i if i.is_integer() => {
                with_match_integer_daft_types!(i, |$T| {
                    let downcasted = self.downcast::<<$T as DaftDataType>::ArrayType>()?;
                    Ok(downcasted.iceberg_truncate(w)?.into_series())
                })
            }
            DataType::Decimal128(..) => Ok(self.decimal128()?.iceberg_truncate(w)?.into_series()),
            DataType::Utf8 => Ok(self.utf8()?.iceberg_truncate(w)?.into_series()),
            _ =>  Err(DaftError::ComputeError(format!(
                "Can only run partitioning_iceberg_truncate() operation on integers, decimal and string, got {}",
                self.data_type()
            ))),
        }?;

        Ok(trunc.rename(format!("{}_truncate", self.name())))
    }
}
