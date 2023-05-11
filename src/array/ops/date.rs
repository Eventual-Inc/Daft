use crate::{
    datatypes::{logical::DateArray, Int32Array, UInt32Array},
    error::DaftResult,
};
use arrow2::compute::arithmetics::ArraySub;

impl DateArray {
    pub fn day(&self) -> DaftResult<UInt32Array> {
        let day_arr = arrow2::compute::temporal::day(self.physical.data())?;
        Ok((self.name(), Box::new(day_arr)).into())
    }

    pub fn month(&self) -> DaftResult<UInt32Array> {
        let month_arr = arrow2::compute::temporal::month(self.physical.data())?;
        Ok((self.name(), Box::new(month_arr)).into())
    }

    pub fn year(&self) -> DaftResult<Int32Array> {
        let year_arr = arrow2::compute::temporal::year(self.physical.data())?;
        Ok((self.name(), Box::new(year_arr)).into())
    }

    pub fn day_of_week(&self) -> DaftResult<UInt32Array> {
        let day_arr = arrow2::compute::temporal::weekday(self.physical.data())?;
        Ok((self.name(), Box::new(day_arr.sub(&1))).into())
    }
}
