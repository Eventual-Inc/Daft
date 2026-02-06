use std::sync::Arc;

use arrow::{array::ArrowPrimitiveType, buffer::ScalarBuffer};
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
    datatypes::{DaftPrimitiveType, NumericNative, prelude::*},
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
        let arrow_array = arrow::array::LargeBinaryArray::from_iter(iter);
        Self::from_arrow(
            Field::new(name, crate::datatypes::DataType::Binary),
            Arc::new(arrow_array),
        )
        .expect("Failed to create BinaryArray from nullable byte arrays")
    }
}
impl FixedSizeBinaryArray {
    pub fn from_iter<S: AsRef<[u8]>>(
        name: &str,
        iter: impl daft_arrow::trusted_len::TrustedLen<Item = Option<S>>,
        size: usize,
    ) -> Self {
        let arrow_array =
            arrow::array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(iter, size as i32)
                .expect("Failed to create FixedSizeBinaryArray from nullable byte arrays");
        Self::from_arrow(
            Field::new(name, crate::datatypes::DataType::FixedSizeBinary(size)),
            Arc::new(arrow_array),
        )
        .unwrap()
    }
}

impl BooleanArray {
    pub fn from_iter(name: &str, iter: impl Iterator<Item = Option<bool>>) -> Self {
        let arrow_array = Arc::new(arrow::array::BooleanArray::from_iter(iter));
        Self::from_arrow(
            Field::new(name, crate::datatypes::DataType::Boolean),
            arrow_array,
        )
        .unwrap()
    }
}

impl FromIterator<Option<bool>> for BooleanArray {
    fn from_iter<T: IntoIterator<Item = Option<bool>>>(iter: T) -> Self {
        let arrow_array = arrow::array::BooleanArray::from_iter(iter);
        Self::from_arrow(Field::new("", DataType::Boolean), Arc::new(arrow_array))
            .expect("Failed to create BooleanArray")
    }
}

impl<P: AsRef<str>> FromIterator<Option<P>> for Utf8Array {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        let arrow_arr = arrow::array::LargeStringArray::from_iter(iter);
        Self::from_arrow(Field::new("", DataType::Utf8), Arc::new(arrow_arr))
            .expect("Failed to create Utf8Array")
    }
}

impl<T>
    FromIterator<Option<<<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native>>
    for DataArray<T>
where
    T: DaftNumericType,
{
    #[inline]
    fn from_iter<
        I: IntoIterator<
            Item = Option<<<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native>,
        >,
    >(
        iter: I,
    ) -> Self {
        let arrow_arr =
            arrow::array::PrimitiveArray::<<T::Native as NumericNative>::ARROWTYPE>::from_iter(
                iter,
            );
        Self::from_arrow(Field::new("", T::get_dtype()), Arc::new(arrow_arr)).unwrap()
    }
}

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn from_iter_values<
        I: IntoIterator<
            Item = <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native,
        >,
    >(
        iter: I,
    ) -> Self {
        let arrow_arr =
            arrow::array::PrimitiveArray::<<T::Native as NumericNative>::ARROWTYPE>::from_iter_values(
                iter,
            );
        Self::from_arrow(Field::new("", T::get_dtype()), Arc::new(arrow_arr)).unwrap()
    }
}

impl BooleanArray {
    pub fn from_iter_values<I: IntoIterator<Item = bool>>(iter: I) -> Self {
        let buf = arrow::buffer::BooleanBuffer::from_iter(iter);
        let arrow_arr = arrow::array::BooleanArray::new(buf, None);

        Self::from_arrow(Field::new("", DataType::Boolean), Arc::new(arrow_arr)).unwrap()
    }
}
impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn from_slice(
        name: &str,
        slice: &[<<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native],
    ) -> Self {
        let b = arrow::buffer::Buffer::from_slice_ref(slice);
        let len = b.len()
            / std::mem::size_of::<
                <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native,
            >();

        let sb = ScalarBuffer::<
            <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native,
        >::new(b, 0, len);
        let arrow_arr =
            arrow::array::PrimitiveArray::<<T::Native as NumericNative>::ARROWTYPE>::new(sb, None);

        Self::from_arrow(Field::new(name, T::get_dtype()), Arc::new(arrow_arr)).unwrap()
    }

    pub fn from_vec(
        name: &str,
        values: Vec<<<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native>,
    ) -> Self {
        let arrow_arr =
            arrow::array::PrimitiveArray::<<T::Native as NumericNative>::ARROWTYPE>::from_iter_values(
                values,
            );
        Self::from_arrow(Field::new(name, T::get_dtype()), Arc::new(arrow_arr)).unwrap()
    }
}

impl Utf8Array {
    pub fn from_slice<T: AsRef<str>>(name: &str, slice: &[T]) -> Self {
        let arrow_array = Box::new(daft_arrow::array::Utf8Array::<i64>::from_slice(slice));
        Self::new(Field::new(name, DataType::Utf8).into(), arrow_array).unwrap()
    }
}

impl BinaryArray {
    pub fn from_values<S: AsRef<[u8]>>(
        name: &str,
        iter: impl daft_arrow::trusted_len::TrustedLen<Item = S>,
    ) -> Self {
        let arrow_array = arrow::array::LargeBinaryArray::from_iter_values(iter);
        Self::from_arrow(Field::new(name, DataType::Binary), Arc::new(arrow_array))
            .expect("Failed to create BinaryArray from byte arrays")
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
        let mut nulls = NullBufferBuilder::new(len);

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
                nulls.append_non_null();
            } else {
                values.push(pynone.clone());
                nulls.append_null();
            }
        }

        let nulls = nulls.finish();

        Self::new(
            Arc::new(Field::new(name, DataType::Python)),
            values.into(),
            nulls,
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
        let mut nulls_builder = NullBufferBuilder::new(len);

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
                    nulls_builder.append_non_null();
                } else {
                    values.push(pynone.clone());
                    nulls_builder.append_null();
                }
            }

            Ok::<_, PyErr>(())
        })?;

        let nulls = nulls_builder.finish();

        Ok(Self::new(
            Arc::new(Field::new(name, DataType::Python)),
            values.into(),
            nulls,
        ))
    }
}

impl IntervalArray {
    pub fn from_iter_values<I: IntoIterator<Item = arrow::datatypes::IntervalMonthDayNano>>(
        iter: I,
    ) -> Self {
        let arrow_arr = arrow::array::IntervalMonthDayNanoArray::from_iter_values(iter);
        Self::from_arrow(Field::new("", DataType::Interval), Arc::new(arrow_arr)).unwrap()
    }
}
