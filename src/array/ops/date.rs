use crate::{
    array::BaseArray,
    datatypes::{DateArray, Int32Array},
    error::DaftResult,
};

impl DateArray {
    pub fn year(&self) -> DaftResult<Int32Array> {
        let year_arr = arrow2::compute::temporal::year(self.data.as_ref())?;
        Ok((self.name(), Box::new(year_arr)).into())
    }
}
