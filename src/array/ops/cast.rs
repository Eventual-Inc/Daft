use arrow2::compute::{
    self,
    cast::{can_cast_types, cast, CastOptions},
};

use crate::series::IntoSeries;
use crate::{
    array::DataArray,
    datatypes::logical::{
        DateArray, EmbeddingArray, FixedShapeImageArray, ImageArray, LogicalArray,
    },
    datatypes::{DaftArrowBackedType, DataType, Field, Utf8Array},
    error::{DaftError, DaftResult},
    series::Series,
    with_match_arrow_daft_types, with_match_daft_logical_types,
};

#[cfg(feature = "python")]
use crate::array::{ops::image::ImageArrayVecs, pseudo_arrow::PseudoArrowArray};
#[cfg(feature = "python")]
use crate::datatypes::{FixedSizeListArray, ImageMode, ListArray, PythonArray};
#[cfg(feature = "python")]
use crate::ffi;
#[cfg(feature = "python")]
use crate::with_match_numeric_daft_types;
#[cfg(feature = "python")]
use arrow2::array::Array;
#[cfg(feature = "python")]
use log;
#[cfg(feature = "python")]
use ndarray::IntoDimension;
#[cfg(feature = "python")]
use num_traits::{NumCast, ToPrimitive};
#[cfg(feature = "python")]
use numpy::{PyArray3, PyReadonlyArrayDyn};
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use std::iter;
#[cfg(feature = "python")]
use std::ops::Deref;

use super::as_arrow::AsArrow;
use std::sync::Arc;
fn arrow_cast<T>(to_cast: &DataArray<T>, dtype: &DataType) -> DaftResult<Series>
where
    T: DaftArrowBackedType,
{
    if !dtype.is_arrow() || !to_cast.data_type().is_arrow() {
        return Err(DaftError::TypeError(format!(
            "Can not cast {:?} to type: {:?}: not convertible to Arrow",
            to_cast.data_type(),
            dtype
        )));
    }
    let physical_type = dtype.to_physical();
    let self_arrow_type = to_cast.data_type().to_arrow()?;
    let target_arrow_type = physical_type.to_arrow()?;
    if !can_cast_types(&self_arrow_type, &target_arrow_type) {
        return Err(DaftError::TypeError(format!(
            "can not cast {:?} to type: {:?}: Arrow types not castable",
            to_cast.data_type(),
            dtype
        )));
    }

    let result_array = cast(
        to_cast.data(),
        &target_arrow_type,
        CastOptions {
            wrapped: true,
            partial: false,
        },
    )?;
    let new_field = Arc::new(Field::new(to_cast.name(), dtype.clone()));

    if dtype.is_logical() {
        with_match_daft_logical_types!(dtype, |$T| {
            let physical = DataArray::try_from((Field::new(to_cast.name(), physical_type), result_array))?;
            return Ok(LogicalArray::<$T>::new(new_field.clone(), physical).into_series());
        })
    }
    with_match_arrow_daft_types!(dtype, |$T| {
        Ok(DataArray::<$T>::try_from((new_field.clone(), result_array))?.into_series())
    })
}

impl<T> DataArray<T>
where
    T: DaftArrowBackedType,
{
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        #[cfg(feature = "python")]
        {
            use crate::python::PySeries;
            use pyo3::prelude::*;

            if dtype == &DataType::Python {
                // Convert something to Python.

                // Use the existing logic on the Python side of the PyO3 layer
                // to create a Python list out of this series.
                let old_pyseries =
                    PySeries::from(Series::try_from((self.name(), self.data.clone()))?);

                let new_pyseries: PySeries = Python::with_gil(|py| -> PyResult<PySeries> {
                    PyModule::import(py, pyo3::intern!(py, "daft.series"))?
                        .getattr(pyo3::intern!(py, "Series"))?
                        .getattr(pyo3::intern!(py, "_from_pyseries"))?
                        .call1((old_pyseries,))?
                        .call_method0(pyo3::intern!(py, "_cast_to_python"))?
                        .getattr(pyo3::intern!(py, "_series"))?
                        .extract()
                })?;

                return Ok(new_pyseries.into());
            }
        }

        arrow_cast(self, dtype)
    }
}

impl DateArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        // We need to handle casts that Arrow doesn't allow, but our type-system does

        let date_array = self
            .as_arrow()
            .clone()
            .to(arrow2::datatypes::DataType::Date32);

        match dtype {
            DataType::Utf8 => {
                // TODO: we should move this into our own strftime kernel
                let year_array = compute::temporal::year(&date_array)?;
                let month_array = compute::temporal::month(&date_array)?;
                let day_array = compute::temporal::day(&date_array)?;
                let date_str: arrow2::array::Utf8Array<i64> = year_array
                    .iter()
                    .zip(month_array.iter())
                    .zip(day_array.iter())
                    .map(|((y, m), d)| match (y, m, d) {
                        (None, _, _) | (_, None, _) | (_, _, None) => None,
                        (Some(y), Some(m), Some(d)) => Some(format!("{y}-{m}-{d}")),
                    })
                    .collect();
                Ok(Utf8Array::from((self.name(), Box::new(date_str))).into_series())
            }
            DataType::Float32 => self.cast(&DataType::Int32)?.cast(&DataType::Float32),
            DataType::Float64 => self.cast(&DataType::Int32)?.cast(&DataType::Float64),
            _ => arrow_cast(&self.physical, dtype),
        }
    }
}

#[cfg(feature = "python")]
macro_rules! pycast_then_arrowcast {
    ($self:expr, $daft_type:expr, $pytype_str:expr) => {
        {
            let old_pyseries = PySeries::from($self.clone().into_series());

            let new_pyseries = Python::with_gil(|py| -> PyResult<PySeries> {
                let old_daft_series = {
                    PyModule::import(py, pyo3::intern!(py, "daft.series"))?
                        .getattr(pyo3::intern!(py, "Series"))?
                        .getattr(pyo3::intern!(py, "_from_pyseries"))?
                        .call1((old_pyseries,))?
                };

                let py_type_fn = {
                    PyModule::import(py, pyo3::intern!(py, "builtins"))?
                        .getattr(pyo3::intern!(py, $pytype_str))?
                };

                old_daft_series
                    .call_method1(
                        pyo3::intern!(py, "_pycast_to_pynative"),
                        (py_type_fn,),
                    )?
                    .getattr(pyo3::intern!(py, "_series"))?
                    .extract()
            })?;

            let new_series: Series = new_pyseries.into();

            if new_series.data_type() == &DataType::Python {
                panic!("After casting, we expected an Arrow data type castable to {}, but got Python type again", $daft_type)
            }
            return new_series.cast(&$daft_type);
        }
    }
}

#[cfg(feature = "python")]
fn append_values_from_numpy<
    Tgt: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
>(
    pyarray: &PyAny,
    index: usize,
    from_numpy_dtype_fn: &PyAny,
    enforce_dtype: Option<&DataType>,
    values_vec: &mut Vec<Tgt>,
    shapes_vec: &mut Vec<Vec<u64>>,
) -> DaftResult<usize> {
    use crate::python::PyDataType;
    use std::num::Wrapping;

    let np_dtype = pyarray.getattr(pyo3::intern!(pyarray.py(), "dtype"))?;

    let datatype = from_numpy_dtype_fn
        .call1((np_dtype,))?
        .getattr(pyo3::intern!(pyarray.py(), "_dtype"))?
        .extract::<PyDataType>()?;
    let datatype = datatype.dtype;
    if let Some(enforce_dtype) = enforce_dtype {
        if enforce_dtype != &datatype {
            return Err(DaftError::ValueError(format!(
                "Expected Numpy array to be of type: {enforce_dtype} but is {datatype} at index: {index}",
            )));
        }
    }
    if !datatype.is_numeric() {
        return Err(DaftError::ValueError(format!(
            "Numpy array has unsupported type {} at index: {index}",
            datatype
        )));
    }
    with_match_numeric_daft_types!(datatype, |$N| {
        type Src = <$N as DaftNumericType>::Native;
        let pyarray: PyReadonlyArrayDyn<'_, Src> = pyarray.extract()?;
        // All of the conditions given in the ndarray::ArrayView::from_shape_ptr() docstring must be met in order to do
        // the pyarray.as_array conversion: https://docs.rs/ndarray/0.15.6/ndarray/type.ArrayView.html#method.from_shape_ptr
        // Relevant:
        //  1. Must have non-negative strides.

        // TODO(Clark): Double-check that we're covering all bases here for this unsafe handoff.
        if pyarray.strides().iter().any(|s| *s < 0) {
            return Err(DaftError::ValueError(format!(
                "we only support numpy arrays with non-negative strides, but got {:?} at index: {}",
                pyarray.strides(), index
            )));
        }

        let pyarray = pyarray.as_array();
        let owned_arr;
        // Create 1D slice from potentially non-contiguous and non-C-order arrays.
        // This will only create a copy if the ndarray is non-contiguous.
        let sl: &[Src] = match pyarray.as_slice_memory_order() {
            Some(sl) => sl,
            None => {
                owned_arr = pyarray.to_owned();
                owned_arr.as_slice_memory_order().unwrap()
            }
        };
        values_vec.extend(sl.iter().map(|v| <Wrapping<Tgt> as NumCast>::from(*v).unwrap().0));
        shapes_vec.push(pyarray.shape().iter().map(|v| *v as u64).collect::<Vec<u64>>());
        Ok((sl.len()))
    })
}

type ArrayPayload<Tgt> = (Vec<Tgt>, Option<Vec<i64>>, Option<Vec<Vec<u64>>>);

#[cfg(feature = "python")]
fn extract_python_to_vec<
    Tgt: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
>(
    py: Python<'_>,
    python_objects: &PythonArray,
    child_dtype: &DataType,
    enforce_dtype: Option<&DataType>,
    list_size: Option<usize>,
    shape_size: Option<usize>,
) -> DaftResult<ArrayPayload<Tgt>> {
    use std::num::Wrapping;

    let mut values_vec: Vec<Tgt> =
        Vec::with_capacity(list_size.unwrap_or(0) * python_objects.len());

    let mut offsets_vec: Vec<i64> = vec![];
    let mut shapes_vec: Vec<Vec<u64>> = vec![];

    if list_size.is_none() {
        offsets_vec.reserve(python_objects.len() + 1);
        offsets_vec.push(0);
        shapes_vec.reserve(python_objects.len() + 1);
    }

    let from_numpy_dtype = {
        PyModule::import(py, pyo3::intern!(py, "daft.datatype"))?
            .getattr(pyo3::intern!(py, "DataType"))?
            .getattr(pyo3::intern!(py, "from_numpy_dtype"))?
    };

    let pytype = match child_dtype {
        dtype if dtype.is_integer() => Ok("int"),
        dtype if dtype.is_floating() => Ok("float"),
        dtype => Err(DaftError::ValueError(format!(
            "We only support numeric types when converting to List or FixedSizeList, got {dtype}"
        ))),
    }?;

    let py_type_fn = { PyModule::import(py, pyo3::intern!(py, "builtins"))?.getattr(pytype)? };
    let py_memory_view = py
        .import("builtins")?
        .getattr(pyo3::intern!(py, "memoryview"))?;

    // TODO: use this to extract our the image mode
    // let py_pil_image_type = py
    //     .import("PIL.Image")
    //     .and_then(|m| m.getattr(pyo3::intern!(py, "Image")));

    for (i, object) in python_objects.as_arrow().iter().enumerate() {
        if let Some(object) = object {
            let object = object.into_py(py);
            let object = object.as_ref(py);

            let supports_buffer_protocol = py_memory_view.call1((object,)).is_ok();
            let supports_array_interface_protocol =
                object.hasattr(pyo3::intern!(py, "__array_interface__"))?;
            let supports_array_protocol = object.hasattr(pyo3::intern!(py, "__array__"))?;

            if supports_buffer_protocol
                || supports_array_interface_protocol
                || supports_array_protocol
            {
                // Path if object is supports buffer/array protocols.
                let np_as_array_fn = py.import("numpy")?.getattr(pyo3::intern!(py, "asarray"))?;
                let pyarray = np_as_array_fn.call1((object,))?;
                let num_values = append_values_from_numpy(
                    pyarray,
                    i,
                    from_numpy_dtype,
                    enforce_dtype,
                    &mut values_vec,
                    &mut shapes_vec,
                )?;
                if let Some(list_size) = list_size {
                    if num_values != list_size {
                        return Err(DaftError::ValueError(format!(
                                "Expected Array-like Object to have {list_size} elements but got {} at index {}",
                                num_values, i
                            )));
                    }
                } else {
                    offsets_vec.push(offsets_vec.last().unwrap() + num_values as i64);
                }
            } else {
                // Path if object does not support buffer/array protocols.
                // Try a best-effort conversion of the elements.
                let pyiter = object.iter();
                if let Ok(pyiter) = pyiter {
                    // has an iter
                    let casted_iter = pyiter.map(|v| v.and_then(|f| py_type_fn.call1((f,))));
                    let collected = if child_dtype.is_integer() {
                        let int_iter = casted_iter
                            .map(|v| v.and_then(|v| v.extract::<i64>()))
                            .map(|v| {
                                v.and_then(|v| {
                                    <Wrapping<Tgt> as NumCast>::from(v).ok_or(
                                        DaftError::ComputeError(format!(
                                            "Could not convert pyint to i64 at index {i}"
                                        ))
                                        .into(),
                                    )
                                })
                            })
                            .map(|v| v.map(|v| v.0));

                        int_iter.collect::<PyResult<Vec<_>>>()
                    } else if child_dtype.is_floating() {
                        let float_iter = casted_iter
                            .map(|v| v.and_then(|v| v.extract::<f64>()))
                            .map(|v| {
                                v.and_then(|v| {
                                    <Wrapping<Tgt> as NumCast>::from(v).ok_or(
                                        DaftError::ComputeError(
                                            "Could not convert pyfloat to f64".into(),
                                        )
                                        .into(),
                                    )
                                })
                            })
                            .map(|v| v.map(|v| v.0));
                        float_iter.collect::<PyResult<Vec<_>>>()
                    } else {
                        unreachable!(
                            "dtype should either be int or float at this point; this is a bug"
                        );
                    };

                    if collected.is_err() {
                        log::warn!("Could not convert python object to list at index: {i} for input series: {}", python_objects.name())
                    }
                    let collected: Vec<Tgt> = collected?;
                    if let Some(list_size) = list_size {
                        if collected.len() != list_size {
                            return Err(DaftError::ValueError(format!(
                                "Expected Array-like Object to have {list_size} elements but got {} at index {}",
                                collected.len(), i
                            )));
                        }
                    } else {
                        let offset = offsets_vec.last().unwrap() + collected.len() as i64;
                        offsets_vec.push(offset);
                        shapes_vec.push(vec![1]);
                    }
                    values_vec.extend_from_slice(collected.as_slice());
                } else {
                    return Err(DaftError::ValueError(format!(
                        "Python Object is neither array-like or an iterable at index {}. Can not convert to a list. object type: {}",
                        i, object.getattr(pyo3::intern!(py, "__class__"))?)));
                }
            }
        } else if let Some(list_size) = list_size {
            values_vec.extend(iter::repeat(Tgt::default()).take(list_size));
        } else {
            let offset = offsets_vec.last().unwrap();
            offsets_vec.push(*offset);
            if let Some(shape_size) = shape_size {
                shapes_vec.push(iter::repeat(1).take(shape_size).collect());
            }
        }
    }
    if list_size.is_some() {
        Ok((values_vec, None, None))
    } else {
        Ok((values_vec, Some(offsets_vec), Some(shapes_vec)))
    }
}

#[cfg(feature = "python")]
fn extract_python_like_to_fixed_size_list<
    Tgt: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
>(
    py: Python<'_>,
    python_objects: &PythonArray,
    child_field: &Field,
    list_size: usize,
) -> DaftResult<FixedSizeListArray> {
    let (values_vec, _, _) = extract_python_to_vec::<Tgt>(
        py,
        python_objects,
        &child_field.dtype,
        None,
        Some(list_size),
        None,
    )?;

    let values_array: Box<dyn arrow2::array::Array> =
        Box::new(arrow2::array::PrimitiveArray::from_vec(values_vec));

    let inner_field = child_field.to_arrow()?;

    let list_dtype = arrow2::datatypes::DataType::FixedSizeList(Box::new(inner_field), list_size);

    let daft_type = (&list_dtype).into();

    let list_array = arrow2::array::FixedSizeListArray::new(
        list_dtype,
        values_array,
        python_objects.as_arrow().validity().cloned(),
    );

    FixedSizeListArray::new(
        Field::new(python_objects.name(), daft_type).into(),
        Box::new(list_array),
    )
}

#[cfg(feature = "python")]
fn extract_python_like_to_list<
    Tgt: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
>(
    py: Python<'_>,
    python_objects: &PythonArray,
    child_field: &Field,
) -> DaftResult<ListArray> {
    let (values_vec, offsets, _) =
        extract_python_to_vec::<Tgt>(py, python_objects, &child_field.dtype, None, None, None)?;

    let offsets = offsets.expect("Offsets should but non-None for dynamic list");

    let values_array: Box<dyn arrow2::array::Array> =
        Box::new(arrow2::array::PrimitiveArray::from_vec(values_vec));

    let inner_field = child_field.to_arrow()?;

    let list_dtype = arrow2::datatypes::DataType::LargeList(Box::new(inner_field));

    let daft_type = (&list_dtype).into();

    let list_array = arrow2::array::ListArray::new(
        list_dtype,
        arrow2::offset::OffsetsBuffer::try_from(offsets)?,
        values_array,
        python_objects.as_arrow().validity().cloned(),
    );

    ListArray::new(
        Field::new(python_objects.name(), daft_type).into(),
        Box::new(list_array),
    )
}

#[cfg(feature = "python")]
fn extract_python_like_to_image_array<
    Tgt: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
>(
    py: Python<'_>,
    python_objects: &PythonArray,
    dtype: &DataType,
    child_dtype: &DataType,
    mode_from_dtype: Option<ImageMode>,
) -> DaftResult<ImageArray> {
    // 3 dimensions - height x width x channel.

    let shape_size = 3;
    let (values_vec, offsets, shapes) = extract_python_to_vec::<Tgt>(
        py,
        python_objects,
        child_dtype,
        Some(child_dtype),
        None,
        Some(shape_size),
    )?;

    let offsets = offsets.expect("Offsets should but non-None for image struct array");
    let shapes = shapes.expect("Shapes should be non-None for image struct array");

    let validity = python_objects.as_arrow().validity();

    let num_rows = shapes.len();

    let mut channels = Vec::<u16>::with_capacity(num_rows);
    let mut heights = Vec::<u32>::with_capacity(num_rows);
    let mut widths = Vec::<u32>::with_capacity(num_rows);
    let mut modes = Vec::<u8>::with_capacity(num_rows);
    for (mut shape, is_valid) in iter::zip(
        shapes.into_iter(),
        validity
            .unwrap_or(&arrow2::bitmap::Bitmap::from_iter(
                iter::repeat(true).take(num_rows),
            ))
            .iter(),
    ) {
        if !is_valid {
            // Handle invalid row by populating dummy data.
            channels.push(1);
            heights.push(1);
            widths.push(1);
            modes.push(mode_from_dtype.unwrap_or(ImageMode::L) as u8);
            continue;
        }
        if shape.len() == shape_size - 1 {
            shape.push(1);
        } else if shape.len() != shape_size {
            return Err(DaftError::ValueError(format!(
                "Image expected to have {} dimensions, but has {}. Image shape = {:?}",
                shape_size,
                shape.len(),
                shape,
            )));
        }
        assert!(shape.len() == shape_size);
        heights.push(
            shape[0]
                .try_into()
                .expect("Image height should fit into a uint16"),
        );
        widths.push(
            shape[1]
                .try_into()
                .expect("Image width should fit into a uint16"),
        );
        channels.push(
            shape[2]
                .try_into()
                .expect("Number of channels should fit into a uint8"),
        );

        modes.push(mode_from_dtype.unwrap_or(ImageMode::try_from_num_channels(
            shape[2].try_into().unwrap(),
            child_dtype,
        )?) as u8);
    }
    ImageArray::from_vecs(
        python_objects.name(),
        dtype.clone(),
        ImageArrayVecs {
            data: values_vec,
            channels,
            heights,
            widths,
            modes,
            offsets,
            validity: validity.cloned(),
        },
    )
}

#[cfg(feature = "python")]
impl PythonArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        use crate::python::PySeries;
        use pyo3::prelude::*;
        match dtype {
            DataType::Python => Ok(self.clone().into_series()),

            DataType::Null => {
                // (Follow Arrow cast behaviour: turn all elements into Null.)
                let null_array = crate::datatypes::NullArray::full_null(
                    self.name(),
                    &DataType::Null,
                    self.len(),
                );
                Ok(null_array.into_series())
            }
            DataType::Boolean => pycast_then_arrowcast!(self, DataType::Boolean, "bool"),
            DataType::Binary => pycast_then_arrowcast!(self, DataType::Binary, "bytes"),
            DataType::Utf8 => pycast_then_arrowcast!(self, DataType::Utf8, "str"),
            dt @ DataType::UInt8
            | dt @ DataType::UInt16
            | dt @ DataType::UInt32
            | dt @ DataType::UInt64
            | dt @ DataType::Int8
            | dt @ DataType::Int16
            | dt @ DataType::Int32
            | dt @ DataType::Int64 => pycast_then_arrowcast!(self, dt, "int"),
            // DataType::Float16 => todo!(),
            dt @ DataType::Float32 | dt @ DataType::Float64 => {
                pycast_then_arrowcast!(self, dt, "float")
            }
            DataType::Date => unimplemented!(),
            DataType::List(field) => {
                if !field.dtype.is_numeric() {
                    return Err(DaftError::ValueError(format!(
                        "We can only convert numeric python types to List, got {}",
                        field.dtype
                    )));
                }
                with_match_numeric_daft_types!(field.dtype, |$T| {
                    type Tgt = <$T as DaftNumericType>::Native;
                    pyo3::Python::with_gil(|py| {
                        let result = extract_python_like_to_list::<Tgt>(py, self, field)?;
                        Ok(result.into_series())
                    })
                })
            }
            DataType::FixedSizeList(field, size) => {
                if !field.dtype.is_numeric() {
                    return Err(DaftError::ValueError(format!(
                        "We can only convert numeric python types to FixedSizeList, got {}",
                        field.dtype
                    )));
                }
                with_match_numeric_daft_types!(field.dtype, |$T| {
                    type Tgt = <$T as DaftNumericType>::Native;
                    pyo3::Python::with_gil(|py| {
                        let result = extract_python_like_to_fixed_size_list::<Tgt>(py, self, field, *size)?;
                        Ok(result.into_series())
                    })
                })
            }
            DataType::Struct(_) => unimplemented!(),
            DataType::Embedding(..) => {
                let result = self.cast(&dtype.to_physical())?;
                let embedding_array = EmbeddingArray::new(
                    Field::new(self.name(), dtype.clone()),
                    result.fixed_size_list()?.clone(),
                );
                Ok(embedding_array.into_series())
            }
            DataType::Image(inner_dtype, mode) => {
                if !inner_dtype.is_numeric() {
                    panic!(
                        "Image logical type should only have numeric physical dtype, but got {}",
                        inner_dtype
                    );
                }
                with_match_numeric_daft_types!(**inner_dtype, |$T| {
                    type Tgt = <$T as DaftNumericType>::Native;
                    pyo3::Python::with_gil(|py| {
                        let result = extract_python_like_to_image_array::<Tgt>(py, self, dtype, inner_dtype, *mode)?;
                        Ok(result.into_series())
                    })
                })
            }
            DataType::FixedShapeImage(..) => {
                let result = self.cast(&dtype.to_physical())?;
                let image_array = FixedShapeImageArray::new(
                    Field::new(self.name(), dtype.clone()),
                    result.fixed_size_list()?.clone(),
                );
                Ok(image_array.into_series())
            }
            // TODO: Add implementations for these types
            // DataType::Timestamp(_, _) => $self.timestamp().unwrap().$method($($args),*),
            dt => unimplemented!("dtype {:?} not supported", dt),
        }
    }
}

impl EmbeddingArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        self.physical.cast(dtype)
    }
}

impl ImageArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match dtype {
            #[cfg(feature = "python")]
            DataType::Python => Python::with_gil(|py| {
                let mut ndarrays = Vec::with_capacity(self.len());
                let da = self.data_array();
                let ca = self.channel_array();
                let ha = self.height_array();
                let wa = self.width_array();
                let pyarrow = py.import("pyarrow")?;
                for (i, arrow_array) in da.iter().enumerate() {
                    let shape = (
                        ha.value(i) as usize,
                        wa.value(i) as usize,
                        ca.value(i) as usize,
                    );
                    let py_array = match arrow_array {
                        Some(arrow_array) => ffi::to_py_array(arrow_array, py, pyarrow)?
                            .call_method0(py, pyo3::intern!(py, "to_numpy"))?
                            .call_method1(py, pyo3::intern!(py, "reshape"), (shape,))?,
                        None => PyArray3::<u8>::zeros(py, shape.into_dimension(), false)
                            .deref()
                            .to_object(py),
                    };
                    ndarrays.push(py_array);
                }
                let values_array =
                    PseudoArrowArray::new(ndarrays.into(), self.as_arrow().validity().cloned());
                Ok(PythonArray::new(
                    Field::new(self.name(), dtype.clone()).into(),
                    values_array.to_boxed(),
                )?
                .into_series())
            }),
            _ => self.physical.cast(dtype),
        }
    }
}

impl FixedShapeImageArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match (dtype, self.logical_type()) {
            #[cfg(feature = "python")]
            (DataType::Python, DataType::FixedShapeImage(_, mode, height, width)) => {
                pyo3::Python::with_gil(|py| {
                    let shape = (
                        *height as usize,
                        *width as usize,
                        mode.num_channels() as usize,
                    );
                    let mut ndarrays = Vec::with_capacity(self.len());
                    let pyarrow = py.import("pyarrow")?;
                    for arrow_array in self.as_arrow().iter() {
                        let py_array = match arrow_array {
                            Some(arrow_array) => ffi::to_py_array(arrow_array, py, pyarrow)?
                                .call_method0(py, pyo3::intern!(py, "to_numpy"))?
                                .call_method1(py, pyo3::intern!(py, "reshape"), (shape,))?,
                            None => PyArray3::<u8>::zeros(py, shape.into_dimension(), false)
                                .deref()
                                .to_object(py),
                        };
                        ndarrays.push(py_array);
                    }
                    let values_array =
                        PseudoArrowArray::new(ndarrays.into(), self.as_arrow().validity().cloned());
                    Ok(PythonArray::new(
                        Field::new(self.name(), dtype.clone()).into(),
                        values_array.to_boxed(),
                    )?
                    .into_series())
                })
            }
            (_, _) => self.physical.cast(dtype),
        }
    }
}
