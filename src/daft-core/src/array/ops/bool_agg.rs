use arrow::{
    array::BooleanBuilder,
    compute::{bool_and, bool_or},
};
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
        Ok(Self::from_iter(self.name(), std::iter::once(value)))
    }

    fn bool_or(&self) -> Self::Output {
        let value = bool_or(&self.as_arrow()?);
        Ok(Self::from_iter(self.name(), std::iter::once(value)))
    }

    fn grouped_bool_and(&self, groups: &GroupIndices) -> Self::Output {
        let mut results = BooleanBuilder::with_capacity(groups.len());

        for group in groups {
            if group.is_empty() {
                results.append_null();
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

            if all_null {
                results.append_null();
            } else {
                results.append_value(result);
            }
        }
        Ok(Self::from_builder(self.name(), results))
    }

    fn grouped_bool_or(&self, groups: &GroupIndices) -> Self::Output {
        let mut results = BooleanBuilder::with_capacity(groups.len());

        for group in groups {
            if group.is_empty() {
                results.append_null();
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

            if all_null {
                results.append_null();
            } else {
                results.append_value(result);
            }
        }
        Ok(Self::from_builder(self.name(), results))
    }
}
