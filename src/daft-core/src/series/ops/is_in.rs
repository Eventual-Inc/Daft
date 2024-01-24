use common_error::DaftResult;

use crate::{
    array::ops::DaftIsIn, datatypes::BooleanArray, with_match_comparable_daft_types, DataType,
    IntoSeries, Series,
};

#[cfg(feature = "python")]
use crate::series::ops::py_membership_op_utilfn;

impl Series {
    pub fn is_in(&self, items: &Self) -> DaftResult<Series> {
        let default =
            BooleanArray::from((self.name(), vec![false; self.len()].as_slice())).into_series();
        if items.is_empty() {
            return Ok(default);
        }

        let (output_type, intermediate, comp_type) =
            match self.data_type().membership_op(items.data_type()) {
                Ok(value) => value,
                Err(_) => {
                    return Ok(default);
                }
            };

        let (lhs, rhs) = if let Some(ref it) = intermediate {
            (self.cast(it)?, items.cast(it)?)
        } else {
            (self.clone(), items.clone())
        };

        if let DataType::Boolean = output_type {
            match comp_type {
                #[cfg(feature = "python")]
                DataType::Python => Ok(py_membership_op_utilfn!(self, items)
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
