use common_error::DaftResult;

use crate::{
    array::{ops::DaftCountDistinctAggable, DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::{DaftPhysicalType, UInt64Type},
};

use super::GroupIndices;

impl<T> DaftCountDistinctAggable for &DataArray<T>
where
    T: DaftPhysicalType,
{
    type Output = DaftResult<DataArray<UInt64Type>>;

    fn count_distinct(&self) -> Self::Output {
        todo!()
    }

    fn grouped_count_distinct(&self, _: &GroupIndices) -> Self::Output {
        todo!()
    }
}

macro_rules! impl_daft_count_distinct_aggable_nested_array {
    ($arr:ident) => {
        impl DaftCountDistinctAggable for &$arr {
            type Output = DaftResult<DataArray<UInt64Type>>;

            fn count_distinct(&self) -> Self::Output {
                // let count = count_arrow_bitmap(&mode, self.validity(), self.len());
                // let result_arrow_array =
                //     Box::new(arrow2::array::PrimitiveArray::from([Some(count)]));
                // DataArray::<UInt64Type>::new(
                //     Arc::new(Field::new(self.field.name.clone(), DataType::UInt64)),
                //     result_arrow_array,
                // )
                todo!()
            }

            fn grouped_count_distinct(&self, _: &GroupIndices) -> Self::Output {
                // let counts_per_group: Vec<_> =
                //     grouped_count_arrow_bitmap(groups, &mode, self.validity());
                // Ok(DataArray::<UInt64Type>::from((
                //     self.field.name.as_ref(),
                //     counts_per_group,
                // )))
                todo!()
            }
        }
    };
}

impl_daft_count_distinct_aggable_nested_array!(FixedSizeListArray);
impl_daft_count_distinct_aggable_nested_array!(ListArray);
impl_daft_count_distinct_aggable_nested_array!(StructArray);
