#![expect(
    clippy::fallible_impl_from,
    reason = "TODO(andrewgazelka/others): This should really be changed in the future"
)]

use std::{borrow::Cow, sync::Arc};

use common_error::{DaftError, DaftResult};

use crate::{
    array::DataArray,
    datatypes::{
        BinaryArray, BooleanArray, DaftNumericType, DaftPhysicalType, DataType, Field,
        FixedSizeBinaryArray, IntervalArray, NullArray, Utf8Array, Utf8Type,
    },
};

impl<T: DaftNumericType> From<(&str, Box<arrow2::array::PrimitiveArray<T::Native>>)>
    for DataArray<T>
{
    fn from(item: (&str, Box<arrow2::array::PrimitiveArray<T::Native>>)) -> Self {
        let (name, array) = item;
        Self::new(Field::new(name, T::get_dtype()).into(), array).unwrap()
    }
}

impl From<(&str, Box<arrow2::array::NullArray>)> for NullArray {
    fn from(item: (&str, Box<arrow2::array::NullArray>)) -> Self {
        let (name, array) = item;
        Self::new(Field::new(name, DataType::Null).into(), array).unwrap()
    }
}

impl From<(&str, Box<arrow2::array::Utf8Array<i64>>)> for Utf8Array {
    fn from(item: (&str, Box<arrow2::array::Utf8Array<i64>>)) -> Self {
        let (name, array) = item;
        Self::new(Field::new(name, DataType::Utf8).into(), array).unwrap()
    }
}

impl From<(&str, Box<arrow2::array::BinaryArray<i64>>)> for BinaryArray {
    fn from(item: (&str, Box<arrow2::array::BinaryArray<i64>>)) -> Self {
        let (name, array) = item;
        Self::new(Field::new(name, DataType::Binary).into(), array).unwrap()
    }
}

impl From<(&str, Box<arrow2::array::FixedSizeBinaryArray>)> for FixedSizeBinaryArray {
    fn from(item: (&str, Box<arrow2::array::FixedSizeBinaryArray>)) -> Self {
        let (name, array) = item;
        Self::new(
            Field::new(name, DataType::FixedSizeBinary(array.size())).into(),
            array,
        )
        .unwrap()
    }
}

impl<'a, I> From<(&str, I, usize)> for FixedSizeBinaryArray
where
    Cow<'a, [u8]>: From<I>,
{
    fn from((name, array, length): (&str, I, usize)) -> Self {
        let array = Cow::from(array);
        let array = array.into_owned();
        Self::new(
            Field::new(name, DataType::FixedSizeBinary(length)).into(),
            Box::new(arrow2::array::FixedSizeBinaryArray::new(
                arrow2::datatypes::DataType::FixedSizeBinary(length),
                arrow2::buffer::Buffer::from(array),
                None,
            )),
        )
        .unwrap()
    }
}

impl<T> From<(&str, &[T::Native])> for DataArray<T>
where
    T: DaftNumericType,
{
    fn from(item: (&str, &[T::Native])) -> Self {
        let (name, slice) = item;
        let arrow_array = Box::new(arrow2::array::PrimitiveArray::<T::Native>::from_slice(
            slice,
        ));
        Self::new(Field::new(name, T::get_dtype()).into(), arrow_array).unwrap()
    }
}

impl<T> From<(&str, Vec<T::Native>)> for DataArray<T>
where
    T: DaftNumericType,
{
    fn from(item: (&str, Vec<T::Native>)) -> Self {
        let (name, v) = item;
        let arrow_array = Box::new(arrow2::array::PrimitiveArray::<T::Native>::from_vec(v));
        Self::new(Field::new(name, T::get_dtype()).into(), arrow_array).unwrap()
    }
}

impl From<(&str, &[bool])> for BooleanArray {
    fn from(item: (&str, &[bool])) -> Self {
        let (name, slice) = item;
        let arrow_array = Box::new(arrow2::array::BooleanArray::from_slice(slice));
        Self::new(Field::new(name, DataType::Boolean).into(), arrow_array).unwrap()
    }
}

impl From<(&str, &[Option<bool>])> for BooleanArray {
    fn from(item: (&str, &[Option<bool>])) -> Self {
        let (name, slice) = item;
        let arrow_array = Box::new(arrow2::array::BooleanArray::from_trusted_len_iter(
            slice.iter().copied(),
        ));
        Self::new(Field::new(name, DataType::Boolean).into(), arrow_array).unwrap()
    }
}

impl From<(&str, arrow2::array::BooleanArray)> for BooleanArray {
    fn from(item: (&str, arrow2::array::BooleanArray)) -> Self {
        let (name, arrow_array) = item;
        Self::new(
            Field::new(name, DataType::Boolean).into(),
            Box::new(arrow_array),
        )
        .unwrap()
    }
}

impl From<(&str, arrow2::bitmap::Bitmap)> for BooleanArray {
    fn from(item: (&str, arrow2::bitmap::Bitmap)) -> Self {
        let (name, bitmap) = item;
        Self::new(
            Field::new(name, DataType::Boolean).into(),
            Box::new(arrow2::array::BooleanArray::new(
                arrow2::datatypes::DataType::Boolean,
                bitmap,
                None,
            )),
        )
        .unwrap()
    }
}

impl From<(&str, Box<arrow2::array::BooleanArray>)> for BooleanArray {
    fn from(item: (&str, Box<arrow2::array::BooleanArray>)) -> Self {
        let (name, arrow_array) = item;
        Self::new(Field::new(name, DataType::Boolean).into(), arrow_array).unwrap()
    }
}

impl From<(&str, Vec<arrow2::types::months_days_ns>)> for IntervalArray {
    fn from(item: (&str, Vec<arrow2::types::months_days_ns>)) -> Self {
        let (name, vec) = item;
        let arrow_array =
            Box::new(arrow2::array::PrimitiveArray::<arrow2::types::months_days_ns>::from_vec(vec));
        Self::new(Field::new(name, DataType::Interval).into(), arrow_array).unwrap()
    }
}

#[cfg(feature = "python")]
impl From<(&str, Vec<Arc<pyo3::PyObject>>)> for crate::datatypes::PythonArray {
    fn from(item: (&str, Vec<Arc<pyo3::PyObject>>)) -> Self {
        use crate::array::pseudo_arrow::PseudoArrowArray;

        let (name, vec_pyobj) = item;
        let arrow_array: Box<dyn arrow2::array::Array> =
            Box::new(PseudoArrowArray::<Arc<pyo3::PyObject>>::from_pyobj_vec(
                vec_pyobj,
            ));
        let field = Field::new(name, DataType::Python);
        Self::new(field.into(), arrow_array).unwrap()
    }
}

impl<T: AsRef<str>> From<(&str, &[T])> for DataArray<Utf8Type> {
    fn from(item: (&str, &[T])) -> Self {
        let (name, slice) = item;
        let arrow_array = Box::new(arrow2::array::Utf8Array::<i64>::from_slice(slice));
        Self::new(Field::new(name, DataType::Utf8).into(), arrow_array).unwrap()
    }
}

impl From<(&str, &[u8])> for BinaryArray {
    fn from(item: (&str, &[u8])) -> Self {
        let (name, slice) = item;
        let arrow_array = Box::new(arrow2::array::BinaryArray::<i64>::from_slice([slice]));
        Self::new(Field::new(name, DataType::Binary).into(), arrow_array).unwrap()
    }
}

impl<T: DaftPhysicalType, F: Into<Arc<Field>>> TryFrom<(F, Box<dyn arrow2::array::Array>)>
    for DataArray<T>
{
    type Error = DaftError;

    fn try_from(item: (F, Box<dyn arrow2::array::Array>)) -> DaftResult<Self> {
        let (field, array) = item;
        let field: Arc<Field> = field.into();
        Self::new(field, array)
    }
}

impl TryFrom<(&str, Vec<u8>, Vec<i64>)> for BinaryArray {
    type Error = DaftError;

    fn try_from(item: (&str, Vec<u8>, Vec<i64>)) -> DaftResult<Self> {
        let (name, data, offsets) = item;

        if offsets.is_empty() {
            return Err(DaftError::ValueError(
                "Expected non zero len offsets".to_string(),
            ));
        }
        let last_offset = *offsets.last().unwrap();
        if last_offset != data.len() as i64 {
            return Err(DaftError::ValueError(format!("Expected Last offset in offsets to be the same as the length of the data array: {last_offset} vs {}", data.len())));
        }

        assert_eq!(last_offset, data.len() as i64);
        let arrow_offsets = arrow2::offset::OffsetsBuffer::try_from(offsets)?;
        let bin_array = arrow2::array::BinaryArray::<i64>::try_new(
            arrow2::datatypes::DataType::LargeBinary,
            arrow_offsets,
            data.into(),
            None,
        )?;
        Self::new(
            Field::new(name, DataType::Binary).into(),
            Box::new(bin_array),
        )
    }
}

#[cfg(feature = "python")]
impl
    TryFrom<(
        &str,
        crate::array::pseudo_arrow::PseudoArrowArray<Arc<pyo3::PyObject>>,
    )> for crate::datatypes::PythonArray
{
    type Error = DaftError;

    fn try_from(
        item: (
            &str,
            crate::array::pseudo_arrow::PseudoArrowArray<Arc<pyo3::PyObject>>,
        ),
    ) -> DaftResult<Self> {
        let (name, array) = item;
        Self::new(Field::new(name, DataType::Python).into(), Box::new(array))
    }
}
