use super::as_arrow::AsArrow;
use crate::{
    array::{ops::image::ImageArraySidecarData, DataArray},
    datatypes::{
        logical::{
            DateArray, Decimal128Array, DurationArray, EmbeddingArray, FixedShapeImageArray,
            FixedShapeTensorArray, ImageArray, LogicalArray, TensorArray, TimestampArray,
        },
        DaftArrowBackedType, DaftLogicalType, DataType, Field, FixedShapeTensorType,
        FixedSizeListArray, ImageMode, StructArray, TensorType, TimeUnit, Utf8Array,
    },
    series::{IntoSeries, Series},
    with_match_arrow_daft_types, with_match_daft_logical_primitive_types,
    with_match_daft_logical_types,
};
use common_error::{DaftError, DaftResult};

use arrow2::{
    array::Array,
    compute::{
        self,
        cast::{can_cast_types, cast, CastOptions},
    },
};
use std::sync::Arc;

#[cfg(feature = "python")]
use {
    crate::array::pseudo_arrow::PseudoArrowArray,
    crate::datatypes::{ListArray, PythonArray},
    crate::ffi,
    crate::with_match_numeric_daft_types,
    log,
    ndarray::IntoDimension,
    num_traits::{NumCast, ToPrimitive},
    numpy::{PyArray3, PyReadonlyArrayDyn},
    pyo3::prelude::*,
    std::iter,
    std::ops::Deref,
};

fn arrow_logical_cast<T>(to_cast: &LogicalArray<T>, dtype: &DataType) -> DaftResult<Series>
where
    T: DaftLogicalType,
{
    // Cast from LogicalArray to the target DataType
    // using Arrow's casting mechanisms.

    // Note that Arrow Logical->Logical direct casts (what this method exposes)
    // have different behaviour than Arrow Logical->Physical->Logical casts.

    let source_dtype = to_cast.logical_type();
    let source_arrow_type = source_dtype.to_arrow()?;
    let target_arrow_type = dtype.to_arrow()?;

    // Get the result of the Arrow Logical->Target cast.
    let result_arrow_array = {
        // First, get corresponding Arrow LogicalArray of source DataArray
        use DataType::*;
        let source_arrow_array = match source_dtype {
            // Wrapped primitives
            Decimal128(..) | Date | Timestamp(..) | Duration(..) => {
                with_match_daft_logical_primitive_types!(source_dtype, |$T| {
                    use arrow2::array::Array;
                    to_cast
                        .physical
                        .data()
                        .as_any()
                        .downcast_ref::<arrow2::array::PrimitiveArray<$T>>()
                        .unwrap()
                        .clone()
                        .to(source_arrow_type)
                        .to_boxed()
                })
            }
            _ => cast(
                to_cast.physical.data(),
                &source_arrow_type,
                CastOptions {
                    wrapped: true,
                    partial: false,
                },
            )?,
        };

        // Then, cast source Arrow LogicalArray to target Arrow LogicalArray.

        cast(
            source_arrow_array.as_ref(),
            &target_arrow_type,
            CastOptions {
                wrapped: true,
                partial: false,
            },
        )?
    };

    // If the target type is also Logical, get the Arrow Physical.
    let result_arrow_physical_array = {
        if dtype.is_logical() {
            use DataType::*;
            let target_physical_type = dtype.to_physical().to_arrow()?;
            match dtype {
                // Primitive wrapper types: change the arrow2 array's type field to primitive
                Decimal128(..) | Date | Timestamp(..) | Duration(..) => {
                    with_match_daft_logical_primitive_types!(dtype, |$P| {
                        use arrow2::array::Array;
                        result_arrow_array
                            .as_any()
                            .downcast_ref::<arrow2::array::PrimitiveArray<$P>>()
                            .unwrap()
                            .clone()
                            .to(target_physical_type)
                            .to_boxed()
                    })
                }
                _ => arrow2::compute::cast::cast(
                    result_arrow_array.as_ref(),
                    &target_physical_type,
                    arrow2::compute::cast::CastOptions {
                        wrapped: true,
                        partial: false,
                    },
                )?,
            }
        } else {
            result_arrow_array
        }
    };

    let new_field = Arc::new(Field::new(to_cast.name(), dtype.clone()));

    if dtype.is_logical() {
        with_match_daft_logical_types!(dtype, |$T| {
            let physical = DataArray::try_from((Field::new(to_cast.name(), dtype.to_physical()), result_arrow_physical_array))?;
            return Ok(LogicalArray::<$T>::new(new_field.clone(), physical).into_series());
        })
    }
    with_match_arrow_daft_types!(dtype, |$T| {
        Ok(DataArray::<$T>::try_from((new_field.clone(), result_arrow_physical_array))?.into_series())
    })
}

fn arrow_cast<T>(to_cast: &DataArray<T>, dtype: &DataType) -> DaftResult<Series>
where
    T: DaftArrowBackedType,
{
    // Cast from DataArray to the target DataType
    // by using Arrow's casting mechanisms.

    if !dtype.is_arrow() || !to_cast.data_type().is_arrow() {
        return Err(DaftError::TypeError(format!(
            "Can not cast {:?} to type: {:?}: not convertible to Arrow",
            to_cast.data_type(),
            dtype
        )));
    }
    let target_physical_type = dtype.to_physical();
    let target_arrow_type = dtype.to_arrow()?;
    let target_arrow_physical_type = target_physical_type.to_arrow()?;
    let self_physical_type = to_cast.data_type().to_physical();
    let self_arrow_type = to_cast.data_type().to_arrow()?;
    let self_physical_arrow_type = self_physical_type.to_arrow()?;

    let result_array = if target_arrow_physical_type == target_arrow_type {
        if !can_cast_types(&self_arrow_type, &target_arrow_type) {
            return Err(DaftError::TypeError(format!(
                "can not cast {:?} to type: {:?}: Arrow types not castable, {:?}, {:?}",
                to_cast.data_type(),
                dtype,
                self_arrow_type,
                target_arrow_type,
            )));
        }
        cast(
            to_cast.data(),
            &target_arrow_type,
            CastOptions {
                wrapped: true,
                partial: false,
            },
        )?
    } else if can_cast_types(&self_arrow_type, &target_arrow_type) {
        // Cast from logical Arrow2 type to logical Arrow2 type.
        let arrow_logical = cast(
            to_cast.data(),
            &target_arrow_type,
            CastOptions {
                wrapped: true,
                partial: false,
            },
        )?;
        cast(
            arrow_logical.as_ref(),
            &target_arrow_physical_type,
            CastOptions {
                wrapped: true,
                partial: false,
            },
        )?
    } else if can_cast_types(&self_physical_arrow_type, &target_arrow_physical_type) {
        // Cast from physical Arrow2 type to physical Arrow2 type.
        cast(
            to_cast.data(),
            &target_arrow_physical_type,
            CastOptions {
                wrapped: true,
                partial: false,
            },
        )?
    } else {
        return Err(DaftError::TypeError(format!(
            "can not cast {:?} to type: {:?}: Arrow types not castable.\n{:?}, {:?},\nPhysical types: {:?}, {:?}",
            to_cast.data_type(),
            dtype,
            self_arrow_type,
            target_arrow_type,
            self_physical_arrow_type,
            target_arrow_physical_type,
        )));
    };

    let new_field = Arc::new(Field::new(to_cast.name(), dtype.clone()));

    if dtype.is_logical() {
        with_match_daft_logical_types!(dtype, |$T| {
            let physical = DataArray::try_from((Field::new(to_cast.name(), target_physical_type), result_array))?;
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
        match dtype {
            #[cfg(feature = "python")]
            DataType::Python => {
                use crate::python::PySeries;
                use pyo3::prelude::*;
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
                Ok(new_pyseries.into())
            }
            _ => arrow_cast(self, dtype),
        }
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
            #[cfg(feature = "python")]
            DataType::Python => cast_logical_to_python_array(self, dtype),
            _ => arrow_cast(&self.physical, dtype),
        }
    }
}

pub(super) fn decimal128_to_str(val: i128, _precision: u8, scale: i8) -> String {
    if scale < 0 {
        unimplemented!();
    } else {
        let modulus = i128::pow(10, scale as u32);
        let integral = val / modulus;
        if scale == 0 {
            format!("{}", integral)
        } else {
            let decimals = (val % modulus).abs();
            let scale = scale as usize;
            format!("{}.{:0scale$}", integral, decimals)
        }
    }
}

pub(super) fn timestamp_to_str_naive(val: i64, unit: &TimeUnit) -> String {
    let chrono_ts = {
        arrow2::temporal_conversions::timestamp_to_naive_datetime(val, unit.to_arrow().unwrap())
    };
    let format_str = match unit {
        TimeUnit::Seconds => "%Y-%m-%dT%H:%M:%S",
        TimeUnit::Milliseconds => "%Y-%m-%dT%H:%M:%S%.3f",
        TimeUnit::Microseconds => "%Y-%m-%dT%H:%M:%S%.6f",
        TimeUnit::Nanoseconds => "%Y-%m-%dT%H:%M:%S%.9f",
    };
    chrono_ts.format(format_str).to_string()
}

pub(super) fn timestamp_to_str_offset(
    val: i64,
    unit: &TimeUnit,
    offset: &chrono::FixedOffset,
) -> String {
    let seconds_format = match unit {
        TimeUnit::Seconds => chrono::SecondsFormat::Secs,
        TimeUnit::Milliseconds => chrono::SecondsFormat::Millis,
        TimeUnit::Microseconds => chrono::SecondsFormat::Micros,
        TimeUnit::Nanoseconds => chrono::SecondsFormat::Nanos,
    };
    arrow2::temporal_conversions::timestamp_to_datetime(val, unit.to_arrow().unwrap(), offset)
        .to_rfc3339_opts(seconds_format, false)
}

pub(super) fn timestamp_to_str_tz(val: i64, unit: &TimeUnit, tz: &chrono_tz::Tz) -> String {
    let seconds_format = match unit {
        TimeUnit::Seconds => chrono::SecondsFormat::Secs,
        TimeUnit::Milliseconds => chrono::SecondsFormat::Millis,
        TimeUnit::Microseconds => chrono::SecondsFormat::Micros,
        TimeUnit::Nanoseconds => chrono::SecondsFormat::Nanos,
    };
    arrow2::temporal_conversions::timestamp_to_datetime(val, unit.to_arrow().unwrap(), tz)
        .to_rfc3339_opts(seconds_format, false)
}

impl TimestampArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match dtype {
            DataType::Timestamp(..) => arrow_logical_cast(self, dtype),
            DataType::Utf8 => {
                let DataType::Timestamp(unit, timezone) = self.logical_type() else { panic!("Wrong dtype for TimestampArray: {}", self.logical_type()) };

                let str_array: arrow2::array::Utf8Array<i64> = timezone.as_ref().map_or_else(
                    || {
                        self.as_arrow()
                            .iter()
                            .map(|val| val.map(|val| timestamp_to_str_naive(*val, unit)))
                            .collect()
                    },
                    |timezone| {
                        if let Ok(offset) = arrow2::temporal_conversions::parse_offset(timezone) {
                            self.as_arrow()
                                .iter()
                                .map(|val| {
                                    val.map(|val| timestamp_to_str_offset(*val, unit, &offset))
                                })
                                .collect()
                        } else if let Ok(tz) =
                            arrow2::temporal_conversions::parse_offset_tz(timezone)
                        {
                            self.as_arrow()
                                .iter()
                                .map(|val| val.map(|val| timestamp_to_str_tz(*val, unit, &tz)))
                                .collect()
                        } else {
                            panic!("Unable to parse timezone string {}", timezone)
                        }
                    },
                );

                Ok(Utf8Array::from((self.name(), Box::new(str_array))).into_series())
            }
            DataType::Float32 => self.cast(&DataType::Int64)?.cast(&DataType::Float32),
            DataType::Float64 => self.cast(&DataType::Int64)?.cast(&DataType::Float64),
            #[cfg(feature = "python")]
            DataType::Python => cast_logical_to_python_array(self, dtype),
            _ => arrow_cast(&self.physical, dtype),
        }
    }
}

impl DurationArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match dtype {
            DataType::Duration(..) => arrow_logical_cast(self, dtype),
            DataType::Float32 => self.cast(&DataType::Int64)?.cast(&DataType::Float32),
            DataType::Float64 => self.cast(&DataType::Int64)?.cast(&DataType::Float64),
            #[cfg(feature = "python")]
            DataType::Python => cast_logical_to_python_array(self, dtype),
            _ => arrow_cast(&self.physical, dtype),
        }
    }
}

impl Decimal128Array {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match dtype {
            #[cfg(feature = "python")]
            DataType::Python => cast_logical_to_python_array(self, dtype),
            _ => arrow_logical_cast(self, dtype),
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
    shapes_vec: &mut Vec<u64>,
) -> DaftResult<(usize, usize)> {
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
        shapes_vec.extend(pyarray.shape().iter().map(|v| *v as u64));
        Ok((sl.len(), pyarray.shape().len()))
    })
}

type ArrayPayload<Tgt> = (
    Vec<Tgt>,
    Option<Vec<i64>>,
    Option<Vec<u64>>,
    Option<Vec<i64>>,
);

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
    let mut shapes_vec: Vec<u64> = vec![];
    let mut shape_offsets_vec: Vec<i64> = vec![];

    if list_size.is_none() {
        offsets_vec.reserve(python_objects.len() + 1);
        offsets_vec.push(0);
        shape_offsets_vec.reserve(python_objects.len() + 1);
        shape_offsets_vec.push(0);
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
                // Path if object supports buffer/array protocols.
                let np_as_array_fn = py.import("numpy")?.getattr(pyo3::intern!(py, "asarray"))?;
                let pyarray = np_as_array_fn.call1((object,))?;
                let (num_values, shape_size) = append_values_from_numpy(
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
                    shape_offsets_vec.push(shape_offsets_vec.last().unwrap() + shape_size as i64);
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
                        shapes_vec.extend(vec![1]);
                        shape_offsets_vec.push(1i64);
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
                shapes_vec.extend(iter::repeat(1).take(shape_size));
                shape_offsets_vec.push(shape_offsets_vec.last().unwrap() + shape_size as i64);
            } else {
                shape_offsets_vec.push(*shape_offsets_vec.last().unwrap());
            }
        }
    }
    if list_size.is_some() {
        Ok((values_vec, None, None, None))
    } else {
        Ok((
            values_vec,
            Some(offsets_vec),
            Some(shapes_vec),
            Some(shape_offsets_vec),
        ))
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
    let (values_vec, _, _, _) = extract_python_to_vec::<Tgt>(
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
    let (values_vec, offsets, _, _) =
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
    let (values_vec, offsets, shapes, shape_offsets) = extract_python_to_vec::<Tgt>(
        py,
        python_objects,
        child_dtype,
        Some(child_dtype),
        None,
        Some(shape_size),
    )?;

    let offsets = offsets.expect("Offsets should but non-None for image struct array");
    let shapes = shapes.expect("Shapes should be non-None for image struct array");
    let shape_offsets =
        shape_offsets.expect("Shape offsets should be non-None for image struct array");

    let validity = python_objects.as_arrow().validity();

    let num_rows = offsets.len() - 1;

    let mut channels = Vec::<u16>::with_capacity(num_rows);
    let mut heights = Vec::<u32>::with_capacity(num_rows);
    let mut widths = Vec::<u32>::with_capacity(num_rows);
    let mut modes = Vec::<u8>::with_capacity(num_rows);
    for i in 0..num_rows {
        let is_valid = validity.map_or(true, |v| v.get_bit(i));
        if !is_valid {
            // Handle invalid row by populating dummy data.
            channels.push(1);
            heights.push(1);
            widths.push(1);
            modes.push(mode_from_dtype.unwrap_or(ImageMode::L) as u8);
            continue;
        }
        let shape_start = shape_offsets[i] as usize;
        let shape_end = shape_offsets[i + 1] as usize;
        let shape = &mut shapes[shape_start..shape_end].to_owned();
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
        values_vec,
        offsets,
        ImageArraySidecarData {
            channels,
            heights,
            widths,
            modes,
            validity: validity.cloned(),
        },
    )
}

#[cfg(feature = "python")]
fn extract_python_like_to_tensor_array<
    Tgt: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
>(
    py: Python<'_>,
    python_objects: &PythonArray,
    dtype: &DataType,
    child_dtype: &DataType,
) -> DaftResult<TensorArray> {
    let (data, offsets, shapes, shape_offsets) = extract_python_to_vec::<Tgt>(
        py,
        python_objects,
        child_dtype,
        Some(child_dtype),
        None,
        None,
    )?;

    let offsets = offsets.expect("Offsets should but non-None for image struct array");
    let shapes = shapes.expect("Shapes should be non-None for image struct array");
    let shape_offsets =
        shape_offsets.expect("Shape offsets should be non-None for image struct array");

    let validity = python_objects.as_arrow().validity();

    let num_rows = shapes.len();
    let name = python_objects.name();
    if data.is_empty() {
        // Create an all-null array if the data array is empty.
        let physical_type = dtype.to_physical();
        let null_struct_array = arrow2::array::new_null_array(physical_type.to_arrow()?, num_rows);
        let daft_struct_array =
            StructArray::new(Field::new(name, physical_type).into(), null_struct_array)?;
        return Ok(TensorArray::new(
            Field::new(name, dtype.clone()),
            daft_struct_array,
        ));
    }
    let offsets = arrow2::offset::OffsetsBuffer::try_from(offsets)?;
    let arrow_dtype: arrow2::datatypes::DataType = Tgt::PRIMITIVE.into();

    let data_dtype = arrow2::datatypes::DataType::LargeList(Box::new(
        arrow2::datatypes::Field::new("data", arrow_dtype, true),
    ));
    let data_array = Box::new(arrow2::array::ListArray::<i64>::new(
        data_dtype,
        offsets,
        Box::new(arrow2::array::PrimitiveArray::from_vec(data)),
        validity.cloned(),
    ));
    let shapes_dtype = arrow2::datatypes::DataType::LargeList(Box::new(
        arrow2::datatypes::Field::new("shape", arrow2::datatypes::DataType::UInt64, true),
    ));
    let shape_offsets = arrow2::offset::OffsetsBuffer::try_from(shape_offsets)?;
    let shapes_array = Box::new(arrow2::array::ListArray::<i64>::new(
        shapes_dtype,
        shape_offsets,
        Box::new(arrow2::array::PrimitiveArray::from_vec(shapes)),
        validity.cloned(),
    ));

    let values: Vec<Box<dyn arrow2::array::Array>> = vec![data_array, shapes_array];
    let physical_type = dtype.to_physical();
    let struct_array = Box::new(arrow2::array::StructArray::new(
        physical_type.to_arrow()?,
        values,
        validity.cloned(),
    ));

    let daft_struct_array =
        crate::datatypes::StructArray::new(Field::new(name, physical_type).into(), struct_array)?;
    Ok(TensorArray::new(
        Field::new(name, dtype.clone()),
        daft_struct_array,
    ))
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
            DataType::Image(mode) => {
                let inner_dtype = mode.map_or(DataType::UInt8, |m| m.get_dtype());
                with_match_numeric_daft_types!(inner_dtype, |$T| {
                    type Tgt = <$T as DaftNumericType>::Native;
                    pyo3::Python::with_gil(|py| {
                        let result = extract_python_like_to_image_array::<Tgt>(py, self, dtype, &inner_dtype, *mode)?;
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
            DataType::Tensor(inner_dtype) => {
                with_match_numeric_daft_types!(**inner_dtype, |$T| {
                    type Tgt = <$T as DaftNumericType>::Native;
                    pyo3::Python::with_gil(|py| {
                        let result = extract_python_like_to_tensor_array::<Tgt>(py, self, dtype, &inner_dtype)?;
                        Ok(result.into_series())
                    })
                })
            }
            DataType::FixedShapeTensor(..) => {
                let result = self.cast(&dtype.to_physical())?;
                let tensor_array = FixedShapeTensorArray::new(
                    Field::new(self.name(), dtype.clone()),
                    result.fixed_size_list()?.clone(),
                );
                Ok(tensor_array.into_series())
            }
            // TODO: Add implementations for these types
            // DataType::Timestamp(_, _) => $self.timestamp().unwrap().$method($($args),*),
            dt => unimplemented!("dtype {:?} not supported", dt),
        }
    }
}

impl EmbeddingArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match (dtype, self.logical_type()) {
            #[cfg(feature = "python")]
            (DataType::Python, DataType::Embedding(_, size)) => Python::with_gil(|py| {
                let shape = (self.len(), *size);
                let pyarrow = py.import("pyarrow")?;
                // Only go through FFI layer once instead of for every embedding.
                // We create an ndarray view on the entire embeddings array
                // buffer sans the validity mask, and then create a subndarray view
                // for each embedding ndarray in the PythonArray.
                let py_array =
                    ffi::to_py_array(self.as_arrow().values().with_validity(None), py, pyarrow)?
                        .call_method1(py, pyo3::intern!(py, "to_numpy"), (false,))?
                        .call_method1(py, pyo3::intern!(py, "reshape"), (shape,))?;
                let ndarrays = py_array
                    .as_ref(py)
                    .iter()?
                    .map(|a| a.unwrap().to_object(py))
                    .collect::<Vec<PyObject>>();
                let values_array =
                    PseudoArrowArray::new(ndarrays.into(), self.as_arrow().validity().cloned());
                Ok(PythonArray::new(
                    Field::new(self.name(), dtype.clone()).into(),
                    values_array.to_boxed(),
                )?
                .into_series())
            }),
            (DataType::Tensor(_), DataType::Embedding(inner_dtype, size)) => {
                let image_shape = vec![*size as u64];
                let fixed_shape_tensor_dtype =
                    DataType::FixedShapeTensor(Box::new(inner_dtype.clone().dtype), image_shape);
                let fixed_shape_tensor_array = self.cast(&fixed_shape_tensor_dtype)?;
                let fixed_shape_tensor_array =
                    fixed_shape_tensor_array.downcast_logical::<FixedShapeTensorType>()?;
                fixed_shape_tensor_array.cast(dtype)
            }
            // NOTE(Clark): Casting to FixedShapeTensor is supported by the physical array cast.
            (_, _) => self.physical.cast(dtype),
        }
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
                            .call_method1(py, pyo3::intern!(py, "to_numpy"), (false,))?
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
            DataType::FixedShapeImage(mode, height, width) => {
                let num_channels = mode.num_channels();
                let image_shape = vec![*height as u64, *width as u64, num_channels as u64];
                let fixed_shape_tensor_dtype =
                    DataType::FixedShapeTensor(Box::new(mode.get_dtype()), image_shape);
                let fixed_shape_tensor_array =
                    self.cast(&fixed_shape_tensor_dtype).or_else(|e| {
                        if num_channels == 1 {
                            // Fall back to height x width shape if unit channel.
                            self.cast(&DataType::FixedShapeTensor(
                                Box::new(mode.get_dtype()),
                                vec![*height as u64, *width as u64],
                            ))
                            .or(Err(e))
                        } else {
                            Err(e)
                        }
                    })?;
                let fixed_shape_tensor_array =
                    fixed_shape_tensor_array.downcast_logical::<FixedShapeTensorType>()?;
                fixed_shape_tensor_array.cast(dtype)
            }
            DataType::Tensor(_) => {
                let ndim = 3;
                let mut shapes = Vec::with_capacity(ndim * self.len());
                let shape_offsets = (0..=ndim * self.len())
                    .step_by(ndim)
                    .map(|v| v as i64)
                    .collect::<Vec<i64>>();
                let validity = self.as_arrow().validity();
                let data_array = self.data_array();
                let ca = self.channel_array();
                let ha = self.height_array();
                let wa = self.width_array();
                for i in 0..self.len() {
                    shapes.push(ha.value(i) as u64);
                    shapes.push(wa.value(i) as u64);
                    shapes.push(ca.value(i) as u64);
                }
                let shapes_dtype = arrow2::datatypes::DataType::LargeList(Box::new(
                    arrow2::datatypes::Field::new(
                        "shape",
                        arrow2::datatypes::DataType::UInt64,
                        true,
                    ),
                ));
                let shape_offsets = arrow2::offset::OffsetsBuffer::try_from(shape_offsets)?;
                let shapes_array = Box::new(arrow2::array::ListArray::<i64>::new(
                    shapes_dtype,
                    shape_offsets,
                    Box::new(arrow2::array::PrimitiveArray::from_vec(shapes)),
                    validity.cloned(),
                ));

                let values: Vec<Box<dyn arrow2::array::Array>> =
                    vec![data_array.to_boxed(), shapes_array];
                let physical_type = dtype.to_physical();
                let struct_array = Box::new(arrow2::array::StructArray::new(
                    physical_type.to_arrow()?,
                    values,
                    validity.cloned(),
                ));

                let daft_struct_array = crate::datatypes::StructArray::new(
                    Field::new(self.name(), physical_type).into(),
                    struct_array,
                )?;
                Ok(
                    TensorArray::new(Field::new(self.name(), dtype.clone()), daft_struct_array)
                        .into_series(),
                )
            }
            DataType::FixedShapeTensor(inner_dtype, _) => {
                let tensor_dtype = DataType::Tensor(inner_dtype.clone());
                let tensor_array = self.cast(&tensor_dtype)?;
                let tensor_array = tensor_array.downcast_logical::<TensorType>()?;
                tensor_array.cast(dtype)
            }
            _ => self.physical.cast(dtype),
        }
    }
}

impl FixedShapeImageArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match (dtype, self.logical_type()) {
            #[cfg(feature = "python")]
            (DataType::Python, DataType::FixedShapeImage(mode, height, width)) => {
                pyo3::Python::with_gil(|py| {
                    let shape = (
                        self.len(),
                        *height as usize,
                        *width as usize,
                        mode.num_channels() as usize,
                    );
                    let pyarrow = py.import("pyarrow")?;
                    // Only go through FFI layer once instead of for every image.
                    // We create an (N, H, W, C) ndarray view on the entire image array
                    // buffer sans the validity mask, and then create a subndarray view
                    // for each image ndarray in the PythonArray.
                    let py_array = ffi::to_py_array(
                        self.as_arrow().values().with_validity(None),
                        py,
                        pyarrow,
                    )?
                    .call_method1(py, pyo3::intern!(py, "to_numpy"), (false,))?
                    .call_method1(
                        py,
                        pyo3::intern!(py, "reshape"),
                        (shape,),
                    )?;
                    let ndarrays = py_array
                        .as_ref(py)
                        .iter()?
                        .map(|a| a.unwrap().to_object(py))
                        .collect::<Vec<PyObject>>();
                    let values_array =
                        PseudoArrowArray::new(ndarrays.into(), self.as_arrow().validity().cloned());
                    Ok(PythonArray::new(
                        Field::new(self.name(), dtype.clone()).into(),
                        values_array.to_boxed(),
                    )?
                    .into_series())
                })
            }
            (DataType::Tensor(_), DataType::FixedShapeImage(mode, height, width)) => {
                let num_channels = mode.num_channels();
                let image_shape = vec![*height as u64, *width as u64, num_channels as u64];
                let fixed_shape_tensor_dtype =
                    DataType::FixedShapeTensor(Box::new(mode.get_dtype()), image_shape);
                let fixed_shape_tensor_array =
                    self.cast(&fixed_shape_tensor_dtype).or_else(|e| {
                        if num_channels == 1 {
                            // Fall back to height x width shape if unit channel.
                            self.cast(&DataType::FixedShapeTensor(
                                Box::new(mode.get_dtype()),
                                vec![*height as u64, *width as u64],
                            ))
                            .or(Err(e))
                        } else {
                            Err(e)
                        }
                    })?;
                let fixed_shape_tensor_array =
                    fixed_shape_tensor_array.downcast_logical::<FixedShapeTensorType>()?;
                fixed_shape_tensor_array.cast(dtype)
            }
            (DataType::Image(_), DataType::FixedShapeImage(mode, _, _)) => {
                let tensor_dtype = DataType::Tensor(Box::new(mode.get_dtype()));
                let tensor_array = self.cast(&tensor_dtype)?;
                let tensor_array = tensor_array.downcast_logical::<TensorType>()?;
                tensor_array.cast(dtype)
            }
            // NOTE(Clark): Casting to FixedShapeTensor is supported by the physical array cast.
            (_, _) => self.physical.cast(dtype),
        }
    }
}

impl TensorArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match dtype {
            #[cfg(feature = "python")]
            DataType::Python => Python::with_gil(|py| {
                let mut ndarrays = Vec::with_capacity(self.len());
                let da = self.data_array();
                let sa = self.shape_array();
                let pyarrow = py.import("pyarrow")?;
                for (arrow_array, shape_array) in da.iter().zip(sa.iter()) {
                    if let (Some(arrow_array), Some(shape_array)) = (arrow_array, shape_array) {
                        let shape_array = shape_array
                            .as_any()
                            .downcast_ref::<arrow2::array::UInt64Array>()
                            .unwrap();
                        let shape = shape_array.values().to_vec();
                        let py_array = ffi::to_py_array(arrow_array, py, pyarrow)?
                            .call_method1(py, pyo3::intern!(py, "to_numpy"), (false,))?
                            .call_method1(py, pyo3::intern!(py, "reshape"), (shape,))?;
                        ndarrays.push(py_array);
                    } else {
                        ndarrays.push(py.None())
                    }
                }
                let values_array =
                    PseudoArrowArray::new(ndarrays.into(), self.as_arrow().validity().cloned());
                Ok(PythonArray::new(
                    Field::new(self.name(), dtype.clone()).into(),
                    values_array.to_boxed(),
                )?
                .into_series())
            }),
            DataType::FixedShapeTensor(inner_dtype, shape) => {
                let da = self.data_array();
                let sa = self.shape_array();
                if !sa.iter().all(|s| {
                    s.map_or(true, |s| {
                        s.as_any()
                            .downcast_ref::<arrow2::array::PrimitiveArray<u64>>()
                            .unwrap()
                            .iter()
                            .eq(shape.iter().map(Some))
                    })
                }) {
                    return Err(DaftError::TypeError(format!(
                        "Can not cast Tensor array to FixedShapeTensor array with type {:?}: Tensor array has shapes different than {:?}; shapes: {:?}",
                        dtype,
                        shape,
                        sa,
                    )));
                }
                let size = shape.iter().product::<u64>() as usize;
                let new_da = arrow2::compute::cast::cast(
                    da,
                    &arrow2::datatypes::DataType::FixedSizeList(
                        Box::new(arrow2::datatypes::Field::new(
                            "data",
                            inner_dtype.to_arrow()?,
                            true,
                        )),
                        size,
                    ),
                    Default::default(),
                )?;
                let inner_field = Box::new(Field::new("data", *inner_dtype.clone()));
                let new_field = Arc::new(Field::new(
                    "data",
                    DataType::FixedSizeList(inner_field, size),
                ));
                let result = FixedSizeListArray::new(new_field, new_da)?;
                let tensor_array =
                    FixedShapeTensorArray::new(Field::new(self.name(), dtype.clone()), result);
                Ok(tensor_array.into_series())
            }
            DataType::Image(mode) => {
                let sa = self.shape_array();
                if !sa.iter().all(|s| {
                    s.map_or(true, |s| {
                        if s.len() != 3 && s.len() != 2 {
                            // Images must have 2 or 3 dimensions: height x width or height x width x channel.
                            // If image is 2 dimensions, 8-bit grayscale is assumed.
                            return false;
                        }
                        if let Some(mode) = mode && s.as_any()
                            .downcast_ref::<arrow2::array::PrimitiveArray<u64>>()
                            .unwrap()
                            .get(s.len() - 1)
                            .unwrap() != mode.num_channels() as u64
                        {
                            // If type-level mode is defined, each image must have the implied number of channels.
                            return false;
                        }
                        true
                    })
                }) {
                    return Err(DaftError::TypeError(format!(
                        "Can not cast Tensor array to Image array with type {:?}: Tensor array shapes are not compatible: {:?}",
                        dtype,
                        sa,
                    )));
                }
                let num_rows = self.len();
                let mut channels = Vec::<u16>::with_capacity(num_rows);
                let mut heights = Vec::<u32>::with_capacity(num_rows);
                let mut widths = Vec::<u32>::with_capacity(num_rows);
                let mut modes = Vec::<u8>::with_capacity(num_rows);
                let da = self.data_array();
                let validity = da.validity();
                for i in 0..num_rows {
                    let is_valid = validity.map_or(true, |v| v.get_bit(i));
                    if !is_valid {
                        // Handle invalid row by populating dummy data.
                        channels.push(1);
                        heights.push(1);
                        widths.push(1);
                        modes.push(mode.unwrap_or(ImageMode::L) as u8);
                        continue;
                    }
                    let shape = sa.value(i);
                    let shape = shape
                        .as_any()
                        .downcast_ref::<arrow2::array::PrimitiveArray<u64>>()
                        .unwrap();
                    assert!(shape.validity().map_or(true, |v| v.iter().all(|b| b)));
                    let mut shape = shape.values().to_vec();
                    if shape.len() == 2 {
                        // Add unit channel dimension to grayscale height x width image.
                        shape.push(1);
                    }
                    if shape.len() != 3 {
                        return Err(DaftError::ValueError(format!(
                            "Image expected to have {} dimensions, but has {}. Image shape = {:?}",
                            3,
                            shape.len(),
                            shape,
                        )));
                    }
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

                    modes.push(mode.unwrap_or(ImageMode::try_from_num_channels(
                        shape[2].try_into().unwrap(),
                        &DataType::UInt8,
                    )?) as u8);
                }
                Ok(ImageArray::from_list_array(
                    self.name(),
                    dtype.clone(),
                    Box::new(da.clone()),
                    ImageArraySidecarData {
                        channels,
                        heights,
                        widths,
                        modes,
                        validity: validity.cloned(),
                    },
                )?
                .into_series())
            }
            DataType::FixedShapeImage(mode, height, width) => {
                let num_channels = mode.num_channels();
                let image_shape = vec![*height as u64, *width as u64, num_channels as u64];
                let fixed_shape_tensor_dtype =
                    DataType::FixedShapeTensor(Box::new(mode.get_dtype()), image_shape);
                let fixed_shape_tensor_array =
                    self.cast(&fixed_shape_tensor_dtype).or_else(|e| {
                        if num_channels == 1 {
                            // Fall back to height x width shape if unit channel.
                            self.cast(&DataType::FixedShapeTensor(
                                Box::new(mode.get_dtype()),
                                vec![*height as u64, *width as u64],
                            ))
                            .or(Err(e))
                        } else {
                            Err(e)
                        }
                    })?;
                let fixed_shape_tensor_array =
                    fixed_shape_tensor_array.downcast_logical::<FixedShapeTensorType>()?;
                fixed_shape_tensor_array.cast(dtype)
            }
            _ => self.physical.cast(dtype),
        }
    }
}

impl FixedShapeTensorArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match (dtype, self.logical_type()) {
            #[cfg(feature = "python")]
            (DataType::Python, DataType::FixedShapeTensor(_, shape)) => {
                pyo3::Python::with_gil(|py| {
                    let pyarrow = py.import("pyarrow")?;
                    let mut np_shape: Vec<u64> = vec![self.len() as u64];
                    np_shape.extend(shape);
                    // Only go through FFI layer once instead of for every tensor element.
                    // We create an (N, [shape..]) ndarray view on the entire tensor array buffer
                    // sans the validity mask, and then create a subndarray view for each ndarray
                    // element in the PythonArray.
                    let py_array = ffi::to_py_array(
                        self.as_arrow().values().with_validity(None),
                        py,
                        pyarrow,
                    )?
                    .call_method1(py, pyo3::intern!(py, "to_numpy"), (false,))?
                    .call_method1(
                        py,
                        pyo3::intern!(py, "reshape"),
                        (np_shape,),
                    )?;
                    let ndarrays = py_array
                        .as_ref(py)
                        .iter()?
                        .map(|a| a.unwrap().to_object(py))
                        .collect::<Vec<PyObject>>();
                    let values_array =
                        PseudoArrowArray::new(ndarrays.into(), self.as_arrow().validity().cloned());
                    Ok(PythonArray::new(
                        Field::new(self.name(), dtype.clone()).into(),
                        values_array.to_boxed(),
                    )?
                    .into_series())
                })
            }
            (DataType::Tensor(_), DataType::FixedShapeTensor(inner_dtype, tensor_shape)) => {
                let ndim = tensor_shape.len();
                let shapes = tensor_shape
                    .iter()
                    .cycle()
                    .copied()
                    .take(ndim * self.len())
                    .collect();
                let shape_offsets = (0..=ndim * self.len())
                    .step_by(ndim)
                    .map(|v| v as i64)
                    .collect::<Vec<i64>>();
                let physical_arr = self.as_arrow();
                let list_dtype = arrow2::datatypes::DataType::LargeList(Box::new(
                    arrow2::datatypes::Field::new("data", inner_dtype.to_arrow()?, true),
                ));
                let list_arr = cast(
                    physical_arr,
                    &list_dtype,
                    CastOptions {
                        wrapped: true,
                        partial: false,
                    },
                )?;
                let validity = self.as_arrow().validity();
                let shapes_dtype = arrow2::datatypes::DataType::LargeList(Box::new(
                    arrow2::datatypes::Field::new(
                        "shape",
                        arrow2::datatypes::DataType::UInt64,
                        true,
                    ),
                ));
                let shape_offsets = arrow2::offset::OffsetsBuffer::try_from(shape_offsets)?;
                let shapes_array = Box::new(arrow2::array::ListArray::<i64>::new(
                    shapes_dtype,
                    shape_offsets,
                    Box::new(arrow2::array::PrimitiveArray::from_vec(shapes)),
                    validity.cloned(),
                ));

                let values: Vec<Box<dyn arrow2::array::Array>> = vec![list_arr, shapes_array];
                let physical_type = dtype.to_physical();
                let struct_array = Box::new(arrow2::array::StructArray::new(
                    physical_type.to_arrow()?,
                    values,
                    validity.cloned(),
                ));

                let daft_struct_array =
                    StructArray::new(Field::new(self.name(), physical_type).into(), struct_array)?;
                Ok(
                    TensorArray::new(Field::new(self.name(), dtype.clone()), daft_struct_array)
                        .into_series(),
                )
            }
            // NOTE(Clark): Casting to FixedShapeImage is supported by the physical array cast.
            (_, _) => self.physical.cast(dtype),
        }
    }
}

#[cfg(feature = "python")]
fn cast_logical_to_python_array<T>(array: &LogicalArray<T>, dtype: &DataType) -> DaftResult<Series>
where
    T: DaftLogicalType,
    LogicalArray<T>: AsArrow,
    <LogicalArray<T> as AsArrow>::Output: arrow2::array::Array,
{
    Python::with_gil(|py| {
        let arrow_dtype = array.logical_type().to_arrow()?;
        let arrow_array = array.as_arrow().to_type(arrow_dtype).with_validity(None);
        let pyarrow = py.import("pyarrow")?;
        let py_array: Vec<PyObject> = ffi::to_py_array(arrow_array.to_boxed(), py, pyarrow)?
            .call_method0(py, pyo3::intern!(py, "to_pylist"))?
            .extract(py)?;
        let values_array =
            PseudoArrowArray::new(py_array.into(), array.as_arrow().validity().cloned());
        Ok(PythonArray::new(
            Field::new(array.name(), dtype.clone()).into(),
            values_array.to_boxed(),
        )?
        .into_series())
    })
}
