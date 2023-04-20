use super::BaseArray;
use crate::{
    datatypes::{DataType, FixedSizeListArray, ListArray, UInt64Array},
    error::DaftResult,
    series::Series,
};

pub trait ExplodableArray {
    fn explode(&self) -> DaftResult<(Series, UInt64Array)>;
}

pub trait NestedArray {
    fn child_data_type(&self) -> &DataType;
}

impl NestedArray for ListArray {
    fn child_data_type(&self) -> &DataType {
        match self.data_type() {
            DataType::List(field) => &field.dtype,
            _ => panic!("Expected List type but received {:?}", self.data_type()),
        }
    }
}

impl NestedArray for FixedSizeListArray {
    fn child_data_type(&self) -> &DataType {
        match self.data_type() {
            DataType::FixedSizeList(field, _) => &field.dtype,
            _ => panic!("Expected List type but received {:?}", self.data_type()),
        }
    }
}
