use crate::datatypes::{
    BinaryArray, BooleanArray, DaftNumericType, Field, FixedSizeBinaryArray, Utf8Array,
};

use super::DataArray;

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn from_iter(
        name: &str,
        iter: impl arrow2::trusted_len::TrustedLen<Item = Option<T::Native>>,
    ) -> Self {
        let arrow_array =
            Box::new(arrow2::array::PrimitiveArray::<T::Native>::from_trusted_len_iter(iter));
        DataArray::new(Field::new(name, T::get_dtype()).into(), arrow_array).unwrap()
    }
}

impl Utf8Array {
    pub fn from_iter<S: AsRef<str>>(
        name: &str,
        iter: impl arrow2::trusted_len::TrustedLen<Item = Option<S>>,
    ) -> Self {
        let arrow_array = Box::new(arrow2::array::Utf8Array::<i64>::from_trusted_len_iter(iter));
        DataArray::new(Field::new(name, crate::DataType::Utf8).into(), arrow_array).unwrap()
    }
}

impl BinaryArray {
    pub fn from_iter<S: AsRef<[u8]>>(
        name: &str,
        iter: impl arrow2::trusted_len::TrustedLen<Item = Option<S>>,
    ) -> Self {
        let arrow_array = Box::new(arrow2::array::BinaryArray::<i64>::from_trusted_len_iter(
            iter,
        ));
        DataArray::new(
            Field::new(name, crate::DataType::Binary).into(),
            arrow_array,
        )
        .unwrap()
    }
}

impl FixedSizeBinaryArray {
    pub fn from_iter<S: AsRef<[u8]>>(
        name: &str,
        iter: impl arrow2::trusted_len::TrustedLen<Item = Option<S>>,
        size: usize,
    ) -> Self {
        let arrow_array = Box::new(arrow2::array::FixedSizeBinaryArray::from_iter(iter, size));
        DataArray::new(
            Field::new(name, crate::DataType::FixedSizeBinary(size)).into(),
            arrow_array,
        )
        .unwrap()
    }
}

impl BooleanArray {
    pub fn from_iter(
        name: &str,
        iter: impl arrow2::trusted_len::TrustedLen<Item = Option<bool>>,
    ) -> Self {
        let arrow_array = Box::new(arrow2::array::BooleanArray::from_trusted_len_iter(iter));
        DataArray::new(
            Field::new(name, crate::DataType::Boolean).into(),
            arrow_array,
        )
        .unwrap()
    }
}
