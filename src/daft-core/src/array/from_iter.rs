use std::sync::Arc;

use arrow2::{
    array::{MutablePrimitiveArray, PrimitiveArray},
    types::months_days_ns,
};

use super::DataArray;
use crate::{
    array::prelude::*,
    datatypes::{prelude::*, DaftPrimitiveType},
};

impl<T> DataArray<T>
where
    T: DaftPrimitiveType,
{
    pub fn from_iter<F: Into<Arc<Field>>>(
        field: F,
        iter: impl arrow2::trusted_len::TrustedLen<Item = Option<T::Native>>,
    ) -> Self {
        // this is a workaround to prevent overflow issues when dealing with i128 and decimal
        // typical behavior would be the result array would always be Decimal(32, 32)
        let field = field.into();
        let mut array = MutablePrimitiveArray::<T::Native>::from(field.dtype.to_arrow().unwrap());
        array.extend_trusted_len(iter);
        let data_array: PrimitiveArray<_> = array.into();
        Self::new(field, data_array.boxed()).unwrap()
    }

    pub fn from_values_iter<F: Into<Arc<Field>>>(
        field: F,
        iter: impl arrow2::trusted_len::TrustedLen<Item = T::Native>,
    ) -> Self {
        // this is a workaround to prevent overflow issues when dealing with i128 and decimal
        // typical behavior would be the result array would always be Decimal(32, 32)
        let field = field.into();
        let mut array = MutablePrimitiveArray::<T::Native>::from(field.dtype.to_arrow().unwrap());
        array.extend_trusted_len_values(iter);
        let data_array: PrimitiveArray<_> = array.into();
        Self::new(field, data_array.boxed()).unwrap()
    }
}

impl Utf8Array {
    pub fn from_iter<S: AsRef<str>>(
        name: &str,
        iter: impl arrow2::trusted_len::TrustedLen<Item = Option<S>>,
    ) -> Self {
        let arrow_array = Box::new(arrow2::array::Utf8Array::<i64>::from_trusted_len_iter(iter));
        Self::new(
            Field::new(name, crate::datatypes::DataType::Utf8).into(),
            arrow_array,
        )
        .unwrap()
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
        Self::new(
            Field::new(name, crate::datatypes::DataType::Binary).into(),
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
        Self::new(
            Field::new(name, crate::datatypes::DataType::FixedSizeBinary(size)).into(),
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
        Self::new(
            Field::new(name, crate::datatypes::DataType::Boolean).into(),
            arrow_array,
        )
        .unwrap()
    }
}

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn from_values(
        name: &str,
        iter: impl arrow2::trusted_len::TrustedLen<Item = T::Native>,
    ) -> Self {
        let arrow_array = Box::new(
            arrow2::array::PrimitiveArray::<T::Native>::from_trusted_len_values_iter(iter),
        );
        Self::new(Field::new(name, T::get_dtype()).into(), arrow_array).unwrap()
    }
}

impl Utf8Array {
    pub fn from_values<S: AsRef<str>>(
        name: &str,
        iter: impl arrow2::trusted_len::TrustedLen<Item = S>,
    ) -> Self {
        let arrow_array =
            Box::new(arrow2::array::Utf8Array::<i64>::from_trusted_len_values_iter(iter));
        Self::new(Field::new(name, DataType::Utf8).into(), arrow_array).unwrap()
    }
}

impl BinaryArray {
    pub fn from_values<S: AsRef<[u8]>>(
        name: &str,
        iter: impl arrow2::trusted_len::TrustedLen<Item = S>,
    ) -> Self {
        let arrow_array =
            Box::new(arrow2::array::BinaryArray::<i64>::from_trusted_len_values_iter(iter));
        Self::new(Field::new(name, DataType::Binary).into(), arrow_array).unwrap()
    }
}

impl BooleanArray {
    pub fn from_values(
        name: &str,
        iter: impl arrow2::trusted_len::TrustedLen<Item = bool>,
    ) -> Self {
        let arrow_array = Box::new(arrow2::array::BooleanArray::from_trusted_len_values_iter(
            iter,
        ));
        Self::new(Field::new(name, DataType::Boolean).into(), arrow_array).unwrap()
    }
}

impl IntervalArray {
    pub fn from_iter<S: Into<months_days_ns>>(
        name: &str,
        iter: impl arrow2::trusted_len::TrustedLen<Item = Option<S>>,
    ) -> Self {
        let arrow_array = Box::new(arrow2::array::MonthsDaysNsArray::from_trusted_len_iter(
            iter.map(|x| x.map(|x| x.into())),
        ));
        Self::new(Field::new(name, DataType::Interval).into(), arrow_array).unwrap()
    }
}

impl IntervalArray {
    pub fn from_values<S: Into<months_days_ns>>(
        name: &str,
        iter: impl arrow2::trusted_len::TrustedLen<Item = S>,
    ) -> Self {
        let arrow_array = Box::new(
            arrow2::array::MonthsDaysNsArray::from_trusted_len_values_iter(iter.map(|x| x.into())),
        );
        Self::new(Field::new(name, DataType::Interval).into(), arrow_array).unwrap()
    }
}
