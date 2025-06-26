use arrow2::array::{Array, BooleanArray};
use common_error::DaftResult;

use crate::{
    array::{
        ops::{DaftBoolAggable, GroupIndices},
        DataArray,
    },
    datatypes::BooleanType,
};

impl DaftBoolAggable for DataArray<BooleanType> {
    type Output = DaftResult<Self>;

    fn bool_and(&self) -> Self::Output {
        let array = self.data();
        let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();

        // If array is empty or all null, return null
        if array.null_count() == array.len() {
            return Ok(Self::from((
                self.field.name.as_ref(),
                Box::new(BooleanArray::from_iter(std::iter::once(None))),
            )));
        }

        // Look for first non-null false value
        let mut result = true;
        for i in 0..array.len() {
            if !array.is_null(i) && !array.value(i) {
                result = false;
                break;
            }
        }

        Ok(Self::from((
            self.field.name.as_ref(),
            Box::new(BooleanArray::from_iter(std::iter::once(Some(result)))),
        )))
    }

    fn bool_or(&self) -> Self::Output {
        let array = self.data();
        let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();

        // If array is empty or all null, return null
        if array.null_count() == array.len() {
            return Ok(Self::from((
                self.field.name.as_ref(),
                Box::new(BooleanArray::from_iter(std::iter::once(None))),
            )));
        }

        // Look for first non-null true value
        let mut result = false;
        for i in 0..array.len() {
            if !array.is_null(i) && array.value(i) {
                result = true;
                break;
            }
        }

        Ok(Self::from((
            self.field.name.as_ref(),
            Box::new(BooleanArray::from_iter(std::iter::once(Some(result)))),
        )))
    }

    fn grouped_bool_and(&self, groups: &GroupIndices) -> Self::Output {
        let array = self.data();
        let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        let mut results = Vec::with_capacity(groups.len());

        for group in groups {
            if group.is_empty() {
                results.push(None);
                continue;
            }

            let mut all_null = true;
            let mut result = true;

            for &idx in group {
                if !array.is_null(idx as usize) {
                    all_null = false;
                    if !array.value(idx as usize) {
                        result = false;
                        break;
                    }
                }
            }

            results.push(if all_null { None } else { Some(result) });
        }

        Ok(Self::from((
            self.field.name.as_ref(),
            Box::new(BooleanArray::from_iter(results)),
        )))
    }

    fn grouped_bool_or(&self, groups: &GroupIndices) -> Self::Output {
        let array = self.data();
        let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        let mut results = Vec::with_capacity(groups.len());

        for group in groups {
            if group.is_empty() {
                results.push(None);
                continue;
            }

            let mut all_null = true;
            let mut result = false;

            for &idx in group {
                if !array.is_null(idx as usize) {
                    all_null = false;
                    if array.value(idx as usize) {
                        result = true;
                        break;
                    }
                }
            }

            results.push(if all_null { None } else { Some(result) });
        }

        Ok(Self::from((
            self.field.name.as_ref(),
            Box::new(BooleanArray::from_iter(results)),
        )))
    }
}
