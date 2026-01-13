use arrow::compute::{bool_and, bool_or};
use common_error::DaftResult;

use crate::{
    array::{
        DataArray,
        ops::{DaftBoolAggable, GroupIndices},
    },
    datatypes::BooleanType,
    prelude::AsArrow,
};

impl DaftBoolAggable for DataArray<BooleanType> {
    type Output = DaftResult<Self>;

    fn bool_and(&self) -> Self::Output {
        let value = bool_and(&self.as_arrow()?);
        Ok(Self::from((self.name(), std::slice::from_ref(&value))))
    }

    fn bool_or(&self) -> Self::Output {
        let value = bool_or(&self.as_arrow()?);
        Ok(Self::from((self.name(), std::slice::from_ref(&value))))
    }

    fn grouped_bool_and(&self, groups: &GroupIndices) -> Self::Output {
        let mut results = Vec::with_capacity(groups.len());

        for group in groups {
            if group.is_empty() {
                results.push(None);
                continue;
            }

            let mut all_null = true;
            let mut result = true;

            for &idx in group {
                if let Some(value) = self.get(idx as usize) {
                    all_null = false;
                    if !value {
                        result = false;
                        break;
                    }
                }
            }

            results.push(if all_null { None } else { Some(result) });
        }

        Ok(Self::from((self.field.name.as_ref(), results.as_slice())))
    }

    fn grouped_bool_or(&self, groups: &GroupIndices) -> Self::Output {
        let mut results = Vec::with_capacity(groups.len());

        for group in groups {
            if group.is_empty() {
                results.push(None);
                continue;
            }

            let mut all_null = true;
            let mut result = false;

            for &idx in group {
                if let Some(value) = self.get(idx as usize) {
                    all_null = false;
                    if value {
                        result = true;
                        break;
                    }
                }
            }

            results.push(if all_null { None } else { Some(result) });
        }

        Ok(Self::from((self.field.name.as_ref(), results.as_slice())))
    }
}
