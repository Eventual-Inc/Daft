use arrow2::array;

use crate::{
    array::{BaseArray, DataArray},
    datatypes::{BooleanArray, DaftNumericType, NullArray, Utf8Array},
};

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    // downcasts a DataArray<T> to an Arrow PrimitiveArray.
    pub fn downcast(&self) -> &array::PrimitiveArray<T::Native> {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl Utf8Array {
    // downcasts a DataArray<T> to an Arrow Utf8Array.
    pub fn downcast(&self) -> &array::Utf8Array<i64> {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl BooleanArray {
    // downcasts a DataArray<T> to an Arrow BooleanArray.
    pub fn downcast(&self) -> &array::BooleanArray {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl NullArray {
    // downcasts a DataArray<T> to an Arrow NullArray.
    pub fn downcast(&self) -> &array::NullArray {
        self.data().as_any().downcast_ref().unwrap()
    }
}
