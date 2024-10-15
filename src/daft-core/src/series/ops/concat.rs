use common_error::{DaftError, DaftResult};

use crate::{
    series::{IntoSeries, Series},
    with_match_daft_types,
};

impl Series {
    pub fn concat(series: &[&Self]) -> DaftResult<Self> {
        let all_types: Vec<_> = series.iter().map(|s| s.data_type().clone()).collect();

        match series {
            [] => Err(DaftError::ValueError(
                "Need at least 1 series to perform concat".to_string(),
            )),
            [single_series] => Ok((*single_series).clone()),
            [first, rest @ ..] => {
                let first_dtype = first.data_type();
                for s in rest {
                    if first_dtype != s.data_type() {
                        return Err(DaftError::TypeError(format!(
                            "Series concat requires all data types to match. Found mismatched types. All types: {:?}",
                            all_types
                        )));
                    }
                }

                with_match_daft_types!(first_dtype, |$T| {
                    let downcasted = series.into_iter().map(|s| s.downcast::<<$T as DaftDataType>::ArrayType>()).collect::<DaftResult<Vec<_>>>()?;
                    Ok(<$T as DaftDataType>::ArrayType::concat(downcasted.as_slice())?.into_series())
                })
            }
        }
    }
}
