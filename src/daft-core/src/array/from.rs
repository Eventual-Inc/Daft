use std::sync::Arc;

use arrow::{
    array::{ArrowPrimitiveType, BooleanBuilder},
    buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer},
};
use common_error::{DaftError, DaftResult};
use daft_arrow::types::months_days_ns;
#[cfg(feature = "python")]
use pyo3::{Py, PyAny};

use super::DataArray;
use crate::{
    array::prelude::*,
    datatypes::{DaftPrimitiveType, NumericNative, prelude::*},
    prelude::*,
    series::{ArrayWrapper, SeriesLike},
};

impl ListArray {
    /// Creates a `ListArray` from pre-existing owned `Vec<Option<Vec<T>>>` data.
    ///
    /// Use this when you already have owned Vecs (e.g. deserialized data).
    /// If you're building data element-by-element, prefer using arrow builders
    /// directly to avoid double iteration.
    pub fn from_vec<T>(name: &str, data: Vec<Option<Vec<T>>>) -> Self
    where
        T: NumericNative,
        T::DAFTTYPE: DaftNumericType<Native = T>,
        T::ARROWTYPE: arrow::datatypes::ArrowPrimitiveType<Native = T>,
        ArrayWrapper<DataArray<T::DAFTTYPE>>: SeriesLike,
    {
        let flat_child_vec: Vec<T> = data.iter().flatten().flatten().copied().collect();
        let arrow_arr =
            arrow::array::PrimitiveArray::<T::ARROWTYPE>::from_iter_values(flat_child_vec);
        let flat_child = DataArray::<T::DAFTTYPE>::from_arrow(
            Field::new(name, T::DAFTTYPE::get_dtype()),
            Arc::new(arrow_arr),
        )
        .unwrap()
        .into_series();

        let lengths: ScalarBuffer<i64> = data
            .iter()
            .map(|d| d.as_ref().map_or(0, |d| d.len()))
            .map(|i| {
                i64::try_from(i).map_err(|e| DaftError::ValueError(format!("invalid length: {e}")))
            })
            .collect::<DaftResult<_>>()
            .expect("failed to convert lengths to i64");

        let offsets = OffsetBuffer::new(lengths);

        let nulls = daft_arrow::buffer::NullBuffer::from_iter(data.iter().map(Option::is_some));

        Self::new(
            flat_child.field().to_list_field().rename(name),
            flat_child,
            offsets,
            Some(nulls),
        )
    }

    /// Creates a `ListArray` from a `Vec<Option<Series>>`.
    ///
    /// Each `Some(series)` becomes a list element; `None` becomes null.
    /// The child series are concatenated into a single flat child array.
    pub fn from_series(name: &str, data: Vec<Option<Series>>) -> DaftResult<Self> {
        let lengths: ScalarBuffer<i64> = data
            .iter()
            .map(|s| s.as_ref().map_or(0, |s| s.len()))
            .map(|i| {
                i64::try_from(i).map_err(|e| DaftError::ValueError(format!("invalid length: {e}")))
            })
            .collect::<DaftResult<_>>()?;

        let offsets = OffsetBuffer::new(lengths);

        let nulls = daft_arrow::buffer::NullBuffer::from_iter(data.iter().map(Option::is_some));

        let flat_child = Series::concat(&data.iter().flatten().collect::<Vec<_>>())?;

        Ok(Self::new(
            flat_child.field().to_list_field().rename(name),
            flat_child,
            offsets,
            Some(nulls),
        ))
    }
}

impl<T> DataArray<T>
where
    T: DaftPrimitiveType,
{
    /// Creates a non-nullable `DataArray` from an iterator of values with a specific [`Field`].
    ///
    /// Unlike the `DaftNumericType` constructors which take `&str`, this takes a [`Field`]
    /// to support types like `Decimal128` where the dtype carries precision/scale.
    /// All values are treated as valid (no nulls).
    pub fn from_field_and_values<F>(field: F, iter: impl IntoIterator<Item = T::Native>) -> Self
    where
        F: Into<Arc<Field>>,
    {
        let field = field.into();
        let values: Vec<T::Native> = iter.into_iter().collect();
        let buffer = arrow::buffer::Buffer::from_vec(values);
        let len = buffer.len() / std::mem::size_of::<T::Native>();
        let scalar_buffer = arrow::buffer::ScalarBuffer::<
            <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native,
        >::new(buffer, 0, len);
        let arrow_arr =
            arrow::array::PrimitiveArray::<<T::Native as NumericNative>::ARROWTYPE>::new(
                scalar_buffer,
                None,
            )
            .with_data_type(field.dtype.to_arrow().unwrap());
        Self::from_arrow(field, Arc::new(arrow_arr)).unwrap()
    }

    /// Creates a nullable `DataArray` from an iterator of `Option<T>` with a specific [`Field`].
    ///
    /// Unlike the `DaftNumericType` constructors which take `&str`, this takes a [`Field`]
    /// to support types like `Decimal128` where the dtype carries precision/scale.
    pub fn from_iter<F, I>(field: F, iter: I) -> Self
    where
        F: Into<Arc<Field>>,
        I: IntoIterator<
            Item = Option<<<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native>,
        >,
    {
        let field = field.into();
        let arrow_arr =
            arrow::array::PrimitiveArray::<<T::Native as NumericNative>::ARROWTYPE>::from_iter(
                iter,
            )
            .with_data_type(field.dtype.to_arrow().unwrap());
        Self::from_arrow(field, Arc::new(arrow_arr)).unwrap()
    }
}

impl BinaryArray {
    /// Creates a nullable `BinaryArray` from an iterator of optional byte slices. Single-pass.
    pub fn from_iter<S: AsRef<[u8]>>(
        name: &str,
        iter: impl IntoIterator<Item = Option<S>>,
    ) -> Self {
        let arrow_array = arrow::array::LargeBinaryArray::from_iter(iter);
        Self::from_arrow(
            Field::new(name, crate::datatypes::DataType::Binary),
            Arc::new(arrow_array),
        )
        .expect("Failed to create BinaryArray from nullable byte arrays")
    }
    /// Creates a non-nullable `BinaryArray` from an iterator of byte slices. Single-pass.
    pub fn from_values<S: AsRef<[u8]>>(name: &str, iter: impl IntoIterator<Item = S>) -> Self {
        let arrow_array = arrow::array::LargeBinaryArray::from_iter_values(iter);
        Self::from_arrow(Field::new(name, DataType::Binary), Arc::new(arrow_array))
            .expect("Failed to create BinaryArray from byte arrays")
    }
}
impl FixedSizeBinaryArray {
    /// Creates a nullable `FixedSizeBinaryArray` from an iterator of optional byte slices.
    pub fn from_iter<S: AsRef<[u8]>>(
        name: &str,
        iter: impl Iterator<Item = Option<S>>,
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

/// Enables `.collect::<Utf8Array>()` from iterators of `Option<impl AsRef<str>>`.
///
/// The resulting array has an empty name `""`. Use `.rename()` to set it.
impl<P: AsRef<str>> FromIterator<Option<P>> for Utf8Array {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        let arrow_arr = arrow::array::LargeStringArray::from_iter(iter);
        Self::from_arrow(Field::new("", DataType::Utf8), Arc::new(arrow_arr))
            .expect("Failed to create Utf8Array")
    }
}

/// Enables `.collect::<DataArray<T>>()` from iterators of `Option<T::Native>`.
///
/// The resulting array has an empty name `""`. Use `.rename()` to set it.
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
    /// Creates a non-nullable `DataArray` from an iterator of non-null values. Single-pass.
    ///
    /// Prefer this over [`from_vec`](Self::from_vec) when you already have an iterator,
    /// as `from_vec` internally iterates the Vec again.
    ///
    /// # Anti-pattern
    /// ```ignore
    /// // BAD: builds a Vec, then from_vec iterates it again (double iteration)
    /// let mut v = Vec::new();
    /// for x in source { v.push(x); }
    /// Array::from_vec("col", v);
    ///
    /// // GOOD: single-pass with from_values
    /// Array::from_values("col", source);
    ///
    /// // BEST: use arrow builders directly for full control
    /// ```
    pub fn from_values<
        I: IntoIterator<
            Item = <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native,
        >,
    >(
        name: &str,
        iter: I,
    ) -> Self {
        let arrow_arr =
            arrow::array::PrimitiveArray::<<T::Native as NumericNative>::ARROWTYPE>::from_iter_values(
                iter,
            );
        Self::from_arrow(Field::new(name, T::get_dtype()), Arc::new(arrow_arr)).unwrap()
    }
    /// Creates a non-nullable `DataArray` from a slice
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

    /// Creates a non-nullable `DataArray` by taking ownership of a `Vec`.
    ///
    /// Only use this for Vecs you already own (e.g. returned from another API).
    /// If you're building a Vec element-by-element just to pass it here,
    /// use [`from_values`](Self::from_values) or arrow builders instead to
    /// avoid double iteration.
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
    /// Creates a non-nullable `Utf8Array` from a slice of string-like values.
    pub fn from_slice<T: AsRef<str>>(name: &str, slice: &[T]) -> Self {
        let arrow_array = arrow::array::LargeStringArray::from_iter_values(slice);

        Self::from_arrow(Field::new(name, DataType::Utf8), Arc::new(arrow_array)).unwrap()
    }
    /// Creates a nullable `Utf8Array` from an iterator of optional strings. Single-pass.
    pub fn from_iter<S: AsRef<str>>(name: &str, iter: impl IntoIterator<Item = Option<S>>) -> Self {
        let arrow_array = arrow::array::LargeStringArray::from_iter(iter);
        Self::from_arrow(
            Field::new(name, crate::datatypes::DataType::Utf8),
            Arc::new(arrow_array),
        )
        .unwrap()
    }
}

impl BooleanArray {
    /// Creates a nullable `BooleanArray` from an iterator of `Option<bool>`. Single-pass.
    pub fn from_iter(name: &str, iter: impl Iterator<Item = Option<bool>>) -> Self {
        let arrow_array = Arc::new(arrow::array::BooleanArray::from_iter(iter));
        Self::from_arrow(
            Field::new(name, crate::datatypes::DataType::Boolean),
            arrow_array,
        )
        .unwrap()
    }
    /// Creates a non-nullable `BooleanArray` from an iterator of `bool`. Single-pass.
    pub fn from_values(name: &str, iter: impl IntoIterator<Item = bool>) -> Self {
        let arrow_array = Arc::new(arrow::array::BooleanArray::from_iter(iter));
        Self::from_arrow(Field::new(name, DataType::Boolean), arrow_array).unwrap()
    }

    /// Creates a non-nullable `BooleanArray` by borrowing a bool slice.
    pub fn from_slice(name: &str, values: &[bool]) -> Self {
        let boolean_buffer = BooleanBuffer::from(values);
        let arrow_array = Arc::new(arrow::array::BooleanArray::new(boolean_buffer, None));
        Self::from_arrow(Field::new(name, DataType::Boolean), arrow_array).unwrap()
    }

    /// Creates a non-nullable `BooleanArray` by taking ownership of a `Vec<bool>`.
    ///
    /// Only use this for Vecs you already own. If building element-by-element,
    /// use [`from_builder`](Self::from_builder) or [`from_values`](Self::from_values) instead.
    pub fn from_vec(name: &str, values: Vec<bool>) -> Self {
        let arrow_array = Arc::new(arrow::array::BooleanArray::from(values));
        Self::from_arrow(Field::new(name, DataType::Boolean), arrow_array).unwrap()
    }

    /// Creates a `BooleanArray` from a pre-built `BooleanBuilder`.
    ///
    /// Use this when you need fine-grained control over null placement
    /// that other constructors don't provide.
    pub fn from_builder(name: &str, mut builder: BooleanBuilder) -> Self {
        let arrow_array = Arc::new(arrow::array::BooleanArray::from(builder.finish()));
        Self::from_arrow(Field::new(name, DataType::Boolean), arrow_array).unwrap()
    }

    pub fn from_null_buffer(name: &str, buf: &NullBuffer) -> common_error::DaftResult<Self> {
        Self::from_arrow(
            Field::new(name, DataType::Boolean),
            Arc::new(arrow::array::BooleanArray::new(buf.inner().clone(), None)),
        )
    }
}

/// Enables `.collect::<BooleanArray>()` from iterators of `Option<bool>`.
///
/// The resulting array has an empty name `""`. Use `.rename()` to set it.
impl FromIterator<Option<bool>> for BooleanArray {
    fn from_iter<T: IntoIterator<Item = Option<bool>>>(iter: T) -> Self {
        let arrow_array = arrow::array::BooleanArray::from_iter(iter);
        Self::from_arrow(Field::new("", DataType::Boolean), Arc::new(arrow_array))
            .expect("Failed to create BooleanArray")
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

#[cfg(feature = "python")]
impl PythonArray {
    /// Create a PythonArray from an iterator.
    ///
    /// Assumes that all Python objects are not None.
    pub fn from_iter(
        name: &str,
        iter: impl ExactSizeIterator<Item = Option<Arc<Py<PyAny>>>>,
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
    pub fn from_iter_pickled<I, T>(name: &str, iter: I) -> common_error::DaftResult<Self>
    where
        I: ExactSizeIterator<Item = Option<T>>,
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
