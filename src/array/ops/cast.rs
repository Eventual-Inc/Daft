use arrow2::compute::{
    self,
    cast::{can_cast_types, cast, CastOptions},
};

use crate::datatypes::FixedSizeListArray;
use crate::datatypes::ListArray;
use crate::series::IntoSeries;
use crate::{
    array::DataArray,
    datatypes::{logical::DateArray, Field},
    datatypes::{DaftArrowBackedType, DataType, Utf8Array},
    error::{DaftError, DaftResult},
    series::Series,
    with_match_arrow_daft_types, with_match_daft_logical_types,
};
use crate::{
    datatypes::logical::{EmbeddingArray, LogicalArray},
    with_match_numeric_daft_types,
};
use arrow2::array::Array;
use num_traits::{NumCast, ToPrimitive};
use std::iter;

use log;

#[cfg(feature = "python")]
use crate::datatypes::PythonArray;
#[cfg(feature = "python")]
use numpy::PyReadonlyArrayDyn;
#[cfg(feature = "python")]
use pyo3::prelude::*;

use super::as_arrow::AsArrow;
use std::sync::Arc;
fn arrow_cast<T>(to_cast: &DataArray<T>, dtype: &DataType) -> DaftResult<Series>
where
    T: DaftArrowBackedType,
{
    let _arrow_type = dtype.to_arrow();

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
    values_vec: &mut Vec<Tgt>,
) -> DaftResult<usize> {
    use crate::python::PyDataType;
    use numpy::PyArray1;
    use std::num::Wrapping;

    let np_dtype = pyarray.getattr(pyo3::intern!(pyarray.py(), "dtype"))?;

    let datatype = from_numpy_dtype_fn
        .call1((np_dtype,))?
        .getattr(pyo3::intern!(pyarray.py(), "_dtype"))?
        .extract::<PyDataType>()?;
    let datatype = datatype.dtype;

    if !datatype.is_numeric() {
        return Err(DaftError::ValueError(format!(
            "Numpy array has unsupported type {} at index: {index}",
            datatype
        )));
    }
    with_match_numeric_daft_types!(datatype, |$N| {
        type Src = <$N as DaftNumericType>::Native;
        let pyarray: PyReadonlyArrayDyn<'_, Src> = pyarray.extract()?;
        if pyarray.ndim() != 1 {
            return Err(DaftError::ValueError(format!(
                "we only support 1 dim numpy arrays, got {} at index: {}",
                pyarray.ndim(), index
            )));
        }

        let pyarray = pyarray.downcast::<PyArray1<Src>>().expect("downcasted to numpy array");
        let sl: &[Src] = unsafe { pyarray.as_slice()}.expect("convert numpy array to slice");
        values_vec.extend(sl.iter().map(|v| <Wrapping<Tgt> as NumCast>::from(*v).unwrap().0));
        Ok(sl.len())
    })
}

#[cfg(feature = "python")]
fn extract_python_to_vec<
    Tgt: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
>(
    py: Python<'_>,
    python_objects: &PythonArray,
    child_dtype: &DataType,
    list_size: Option<usize>,
) -> DaftResult<(Vec<Tgt>, Option<Vec<i64>>)> {
    use std::num::Wrapping;

    let mut values_vec: Vec<Tgt> =
        Vec::with_capacity(list_size.unwrap_or(0) * python_objects.len());

    let mut offsets_vec: Vec<i64> = vec![];

    if list_size.is_none() {
        offsets_vec.reserve(python_objects.len() + 1);
        offsets_vec.push(0);
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
            "We only support numeric types when converting to FixedSizeList, got {dtype}"
        ))),
    }?;

    let py_type_fn = { PyModule::import(py, pyo3::intern!(py, "builtins"))?.getattr(pytype)? };

    for (i, object) in python_objects.as_arrow().iter().enumerate() {
        if let Some(object) = object {
            let object = object.into_py(py);
            let object = object.as_ref(py);

            let pyarray = object.call_method0(pyo3::intern!(py, "__array__"));

            if let Ok(pyarray) = pyarray {
                // Path if object is array-like
                let num_values =
                    append_values_from_numpy(pyarray, i, from_numpy_dtype, &mut values_vec)?;
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
                // Path if object is not array-like
                // try to see if we can iterate over the object
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
                        return Err(DaftError::ValueError(format!(
                            "Python Object is neither array-like or an iterable at index {}. Can not convert to a list. object type: {}",
                            i, object.getattr(pyo3::intern!(py, "__class__"))?)));
                    };

                    if collected.is_err() {
                        log::warn!("Could not convert python object to fixed size list at index: {i} for input series: {}", python_objects.name())
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
                    }
                    values_vec.extend_from_slice(collected.as_slice());
                }
            }
        } else if let Some(list_size) = list_size {
            values_vec.extend(iter::repeat(Tgt::default()).take(list_size));
        } else {
            let offset = offsets_vec.last().unwrap();
            offsets_vec.push(*offset);
        }
    }
    if list_size.is_some() {
        Ok((values_vec, None))
    } else {
        Ok((values_vec, Some(offsets_vec)))
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
    let (values_vec, _) =
        extract_python_to_vec::<Tgt>(py, python_objects, &child_field.dtype, Some(list_size))?;

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
    let (values_vec, offsets) =
        extract_python_to_vec::<Tgt>(py, python_objects, &child_field.dtype, None)?;

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
            DataType::Embedding(..) => {
                let result = self.cast(&dtype.to_physical())?;
                let embedding_array = EmbeddingArray::new(
                    Field::new(self.name(), dtype.clone()),
                    result.fixed_size_list()?.clone(),
                );
                Ok(embedding_array.into_series())
            }
            DataType::Struct(_) => unimplemented!(),
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
