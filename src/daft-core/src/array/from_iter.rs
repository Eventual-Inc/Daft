use std::sync::Arc;

use common_error::DaftResult;
use daft_arrow::{
    array::{MutablePrimitiveArray, PrimitiveArray},
    types::months_days_ns,
};
#[cfg(feature = "python")]
use pyo3::{Py, PyAny};

use super::DataArray;
use crate::{
    array::prelude::*,
    datatypes::{DaftPrimitiveType, prelude::*},
};

impl<T> DataArray<T>
where
    T: DaftPrimitiveType,
{
    pub fn from_iter<F: Into<Arc<Field>>>(
        field: F,
        iter: impl daft_arrow::trusted_len::TrustedLen<Item = Option<T::Native>>,
    ) -> Self {
        // this is a workaround to prevent overflow issues when dealing with i128 and decimal
        // typical behavior would be the result array would always be Decimal(32, 32)
        let field = field.into();
        let mut array = MutablePrimitiveArray::<T::Native>::from(field.dtype.to_arrow2().unwrap());
        array.extend_trusted_len(iter);
        let data_array: PrimitiveArray<_> = array.into();
        Self::new(field, data_array.boxed()).unwrap()
    }

    pub fn from_values_iter<F: Into<Arc<Field>>>(
        field: F,
        iter: impl daft_arrow::trusted_len::TrustedLen<Item = T::Native>,
    ) -> Self {
        // this is a workaround to prevent overflow issues when dealing with i128 and decimal
        // typical behavior would be the result array would always be Decimal(32, 32)
        let field = field.into();
        let mut array = MutablePrimitiveArray::<T::Native>::from(field.dtype.to_arrow2().unwrap());
        array.extend_trusted_len_values(iter);
        let data_array: PrimitiveArray<_> = array.into();
        Self::new(field, data_array.boxed()).unwrap()
    }

    pub fn from_regular_iter<F, I>(field: F, iter: I) -> DaftResult<Self>
    where
        F: Into<Arc<Field>>,
        I: Iterator<Item = Option<T::Native>>,
    {
        let field = field.into();
        let data_type = field.dtype.to_arrow2()?;
        let mut array = MutablePrimitiveArray::<T::Native>::from(data_type);
        let (_, upper_bound) = iter.size_hint();
        if let Some(upper_bound) = upper_bound {
            array.reserve(upper_bound);
        }
        array.extend(iter);
        let array = PrimitiveArray::from(array).boxed();
        Self::new(field, array)
    }
}

impl Utf8Array {
    pub fn from_iter<S: AsRef<str>>(
        name: &str,
        iter: impl daft_arrow::trusted_len::TrustedLen<Item = Option<S>>,
    ) -> Self {
        let arrow_array = Box::new(daft_arrow::array::Utf8Array::<i64>::from_trusted_len_iter(
            iter,
        ));
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
        iter: impl daft_arrow::trusted_len::TrustedLen<Item = Option<S>>,
    ) -> Self {
        let arrow_array =
            Box::new(daft_arrow::array::BinaryArray::<i64>::from_trusted_len_iter(iter));
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
        iter: impl daft_arrow::trusted_len::TrustedLen<Item = Option<S>>,
        size: usize,
    ) -> Self {
        let arrow_array = Box::new(daft_arrow::array::FixedSizeBinaryArray::from_iter(
            iter, size,
        ));
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
        iter: impl daft_arrow::trusted_len::TrustedLen<Item = Option<bool>>,
    ) -> Self {
        let arrow_array = Box::new(daft_arrow::array::BooleanArray::from_trusted_len_iter(iter));
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
        iter: impl daft_arrow::trusted_len::TrustedLen<Item = T::Native>,
    ) -> Self {
        let arrow_array = Box::new(
            daft_arrow::array::PrimitiveArray::<T::Native>::from_trusted_len_values_iter(iter),
        );
        Self::new(Field::new(name, T::get_dtype()).into(), arrow_array).unwrap()
    }
}

impl Utf8Array {
    pub fn from_values<S: AsRef<str>>(
        name: &str,
        iter: impl daft_arrow::trusted_len::TrustedLen<Item = S>,
    ) -> Self {
        let arrow_array =
            Box::new(daft_arrow::array::Utf8Array::<i64>::from_trusted_len_values_iter(iter));
        Self::new(Field::new(name, DataType::Utf8).into(), arrow_array).unwrap()
    }
}

impl BinaryArray {
    pub fn from_values<S: AsRef<[u8]>>(
        name: &str,
        iter: impl daft_arrow::trusted_len::TrustedLen<Item = S>,
    ) -> Self {
        let arrow_array =
            Box::new(daft_arrow::array::BinaryArray::<i64>::from_trusted_len_values_iter(iter));
        Self::new(Field::new(name, DataType::Binary).into(), arrow_array).unwrap()
    }
}

impl BooleanArray {
    pub fn from_values(
        name: &str,
        iter: impl daft_arrow::trusted_len::TrustedLen<Item = bool>,
    ) -> Self {
        let arrow_array =
            Box::new(daft_arrow::array::BooleanArray::from_trusted_len_values_iter(iter));
        Self::new(Field::new(name, DataType::Boolean).into(), arrow_array).unwrap()
    }
}

impl IntervalArray {
    pub fn from_iter<S: Into<months_days_ns>>(
        name: &str,
        iter: impl daft_arrow::trusted_len::TrustedLen<Item = Option<S>>,
    ) -> Self {
        let arrow_array = Box::new(daft_arrow::array::MonthsDaysNsArray::from_trusted_len_iter(
            iter.map(|x| x.map(|x| x.into())),
        ));
        Self::new(Field::new(name, DataType::Interval).into(), arrow_array).unwrap()
    }
}

impl IntervalArray {
    pub fn from_values<S: Into<months_days_ns>>(
        name: &str,
        iter: impl daft_arrow::trusted_len::TrustedLen<Item = S>,
    ) -> Self {
        let arrow_array = Box::new(
            daft_arrow::array::MonthsDaysNsArray::from_trusted_len_values_iter(
                iter.map(|x| x.into()),
            ),
        );
        Self::new(Field::new(name, DataType::Interval).into(), arrow_array).unwrap()
    }
}

#[cfg(feature = "python")]
impl PythonArray {
    /// Create a PythonArray from an iterator.
    ///
    /// Assumes that all Python objects are not None.
    pub fn from_iter(
        name: &str,
        iter: impl daft_arrow::trusted_len::TrustedLen<Item = Option<Arc<Py<PyAny>>>>,
    ) -> Self {
        use daft_arrow::buffer::NullBufferBuilder;
        use pyo3::Python;

        let (_, upper) = iter.size_hint();
        let len = upper.expect("trusted_len_unzip requires an upper limit");

        let mut values = Vec::with_capacity(len);
        let mut validity = NullBufferBuilder::new(len);

        let pynone = Arc::new(Python::attach(|py| py.None()));
        for v in iter {
            if let Some(obj) = &v {
                debug_assert!(
                    Python::attach(|py| !obj.is_none(py)),
                    "PythonArray::from_iter requires all Python objects to be not None"
                );
            }

            if let Some(obj) = v {
                values.push(obj);
                validity.append_non_null();
            } else {
                values.push(pynone.clone());
                validity.append_null();
            }
        }

        let validity = validity.finish();

        Self::new(
            Arc::new(Field::new(name, DataType::Python)),
            values.into(),
            validity,
        )
    }

    /// Create a PythonArray from an iterator of pickled values.
    ///
    /// Assumes that all Python objects are not None.
    pub fn from_iter_pickled<I, T>(name: &str, iter: I) -> DaftResult<Self>
    where
        I: daft_arrow::trusted_len::TrustedLen<Item = Option<T>>,
        T: AsRef<[u8]>,
    {
        use daft_arrow::buffer::NullBufferBuilder;
        use pyo3::Python;

        let (_, upper) = iter.size_hint();
        let len = upper.expect("trusted_len_unzip requires an upper limit");

        let mut values = Vec::with_capacity(len);
        let mut validity = NullBufferBuilder::new(len);

        Python::attach(|py| {
            use pyo3::PyErr;

            let pynone = Arc::new(py.None());
            for v in iter {
                if let Some(bytes) = v {
                    use common_py_serde::pickle_loads;
                    use pyo3::types::PyAnyMethods;

                    let obj = pickle_loads(py, bytes)?;

                    debug_assert!(
                        !obj.is_none(),
                        "PythonArray::from_iter requires all Python objects to be not None"
                    );

                    values.push(Arc::new(obj.unbind()));
                    validity.append_non_null();
                } else {
                    values.push(pynone.clone());
                    validity.append_null();
                }
            }

            Ok::<_, PyErr>(())
        })?;

        let validity = validity.finish();

        Ok(Self::new(
            Arc::new(Field::new(name, DataType::Python)),
            values.into(),
            validity,
        ))
    }
}
