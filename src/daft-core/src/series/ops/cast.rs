use crate::{
    datatypes::{DataType, UInt64Array},
    series::Series,
    IntoSeries,
};
use common_error::{DaftError, DaftResult};

impl Series {
    pub fn cast(&self, datatype: &DataType) -> DaftResult<Series> {
        self.inner.cast(datatype)
    }

    pub fn cast_to_uint64(&self) -> DaftResult<Series> {
        if self.data_type().is_unsigned() {
            self.inner.cast(&DataType::UInt64)
        } else if self.data_type().is_integer() {
            let temp = self.inner.cast(&DataType::Int64)?;
            let arr = temp.i64()?;
            let name = arr.name();
            let result = arr
                .into_iter()
                .map(|x| {
                    x.map(|x| {
                        if *x < 0 {
                            Err(DaftError::ValueError(
                                "Cannot cast negative integer to unsigned integer".to_string(),
                            ))
                        } else {
                            Ok(*x as u64)
                        }
                    })
                    .transpose()
                })
                .collect::<DaftResult<arrow2::array::UInt64Array>>()?;
            Ok(UInt64Array::from((name, Box::new(result))).into_series())
        } else {
            Err(DaftError::TypeError(format!(
                "Expected input to cast_to_uint64 to be integer, got {}",
                self.data_type()
            )))
        }
    }
}
