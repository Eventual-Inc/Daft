use common_error::DaftResult;

use crate::{
    array::ops::DaftBetween, datatypes::BooleanArray, with_match_numeric_daft_types, DataType,
    IntoSeries, Series,
};

use super::py_between_op_utilfn;

impl Series {
    pub fn between(&self, lower: &Series, upper: &Series) -> DaftResult<Series> {
        let (_output_type, _intermediate, lower_comp_type) =
            self.data_type().comparison_op(lower.data_type())?;
        let (_output_type, _intermediate, upper_comp_type) =
            self.data_type().comparison_op(upper.data_type())?;
        let (output_type, intermediate, comp_type) =
            lower_comp_type.comparison_op(&upper_comp_type)?;
        let (it_value, it_lower, it_upper) = if let Some(ref it) = intermediate {
            (self.cast(it)?, lower.cast(it)?, upper.cast(it)?)
        } else {
            (self.clone(), lower.clone(), upper.clone())
        };
        if let DataType::Boolean = output_type {
            match comp_type {
                #[cfg(feature = "python")]
                DataType::Python => Ok(py_between_op_utilfn(self, upper, lower)?
                    .downcast::<BooleanArray>()?
                    .clone()
                    .into_series()),
                _ => with_match_numeric_daft_types!(comp_type, |$T| {
                        let casted_value = it_value.cast(&comp_type)?;
                        let casted_lower = it_lower.cast(&comp_type)?;
                        let casted_upper = it_upper.cast(&comp_type)?;
                        let value = casted_value.downcast::<<$T as DaftDataType>::ArrayType>()?;
                        let lower = casted_lower.downcast::<<$T as DaftDataType>::ArrayType>()?;
                        let upper = casted_upper.downcast::<<$T as DaftDataType>::ArrayType>()?;
                        Ok(value.between(lower, upper)?.into_series())
                }),
            }
        } else {
            unreachable!()
        }
    }
}
