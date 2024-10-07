use arrow2::types::{days_ms, months_days_ns};

use super::DataArray;
use crate::{
    array::prelude::*,
    datatypes::{prelude::*, IntervalDayTimeArray, IntervalMonthDayNanoArray},
};

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
        Self::new(Field::new(name, T::get_dtype()).into(), arrow_array).unwrap()
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

impl IntervalDayTimeArray {
    pub fn from_iter(
        name: &str,
        iter: impl arrow2::trusted_len::TrustedLen<Item = Option<days_ms>>,
    ) -> Self {
        let arrow_array = Box::new(arrow2::array::DaysMsArray::from_trusted_len_iter(iter));
        Self::new(
            Field::new(name, DataType::Interval(IntervalUnit::DayTime)).into(),
            arrow_array,
        )
        .unwrap()
    }
}

impl IntervalMonthDayNanoArray {
    pub fn from_iter(
        name: &str,
        iter: impl arrow2::trusted_len::TrustedLen<Item = Option<months_days_ns>>,
    ) -> Self {
        let arrow_array = Box::new(arrow2::array::MonthsDaysNsArray::from_trusted_len_iter(
            iter,
        ));
        Self::new(
            Field::new(name, DataType::Interval(IntervalUnit::MonthDayNano)).into(),
            arrow_array,
        )
        .unwrap()
    }
}
