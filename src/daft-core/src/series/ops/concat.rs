use crate::datatypes::logical::LogicalArray;
use crate::{
    series::{IntoSeries, Series},
    with_match_daft_logical_types, with_match_physical_daft_types,
};
use common_error::{DaftError, DaftResult};

impl Series {
    pub fn concat(series: &[&Series]) -> DaftResult<Self> {
        if series.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 series to perform concat".to_string(),
            ));
        }

        if series.len() == 1 {
            return Ok((*series.first().unwrap()).clone());
        }

        let first_dtype = series.first().unwrap().data_type();
        for s in series.iter().skip(1) {
            if first_dtype != s.data_type() {
                return Err(DaftError::TypeError(format!(
                    "Series concat requires all data types to match, {} vs {}",
                    first_dtype,
                    s.data_type()
                )));
            }
        }
        if first_dtype.is_logical() {
            return Ok(with_match_daft_logical_types!(first_dtype, |$T| {
                let downcasted = series.into_iter().map(|s| s.downcast_logical::<$T>()).collect::<DaftResult<Vec<_>>>()?;
                LogicalArray::<$T>::concat(downcasted.as_slice())?.into_series()
            }));
        }

        with_match_physical_daft_types!(first_dtype, |$T| {
            let downcasted = series.into_iter().map(|s| s.downcast::<$T>()).collect::<DaftResult<Vec<_>>>()?;
            Ok(DataArray::<$T>::concat(downcasted.as_slice())?.into_series())
        })
    }
}
