use common_error::DaftResult;

#[cfg(feature = "python")]
use crate::series::utils::python_fn::py_membership_op_utilfn;
use crate::{
    array::ops::DaftIsIn,
    datatypes::{BooleanArray, DataType, InferDataType},
    series::{IntoSeries, Series},
    with_match_comparable_daft_types,
};

fn default(name: &str, size: usize) -> DaftResult<Series> {
    Ok(BooleanArray::from((name, vec![false; size].as_slice())).into_series())
}

impl Series {
    pub fn is_in(&self, items: &Self) -> DaftResult<Self> {
        if items.is_empty() {
            return default(self.name(), self.len());
        }

        let (output_type, intermediate, comp_type) = InferDataType::from(self.data_type())
            .membership_op(&InferDataType::from(items.data_type()))?;

        let (lhs, rhs) = if let Some(ref it) = intermediate {
            (self.cast(it)?, items.cast(it)?)
        } else {
            (self.clone(), items.clone())
        };

        if output_type == DataType::Boolean {
            match comp_type {
                #[cfg(feature = "python")]
                DataType::Python => Ok(py_membership_op_utilfn(self, items)?
                    .downcast::<BooleanArray>()?
                    .clone()
                    .into_series()),
                _ => with_match_comparable_daft_types!(comp_type, |$T| {
                        let casted_lhs = lhs.cast(&comp_type)?;
                        let casted_rhs = rhs.cast(&comp_type)?;
                        let lhs = casted_lhs.downcast::<<$T as DaftDataType>::ArrayType>()?;
                        let rhs = casted_rhs.downcast::<<$T as DaftDataType>::ArrayType>()?;

                        Ok(lhs.is_in(rhs)?.into_series())
                }),
            }
        } else {
            unreachable!()
        }
    }
}
