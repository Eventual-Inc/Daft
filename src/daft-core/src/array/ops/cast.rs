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
use pyo3::types::PyIterator;
use std::sync::Arc;

#[cfg(feature = "python")]
use {
    crate::array::pseudo_arrow::PseudoArrowArray,
    crate::datatypes::{ListArray, PythonArray},
    crate::ffi,
    crate::python::PyDataType,
    crate::with_match_numeric_daft_types,
    log,
    ndarray::{ArrayView, IntoDimension},
    num_traits::{NumCast, ToPrimitive},
    numpy::{IxDyn, PyArray3, PyReadonlyArrayDyn},
    pyo3::prelude::*,
    std::iter,
    std::num::Wrapping,
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

// #[cfg(feature = "python")]
// fn append_values_from_numpy<
//     'a,
//     Tgt: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
// >(
//     pyarray: &'a PyAny,
//     index: usize,
//     from_numpy_dtype_fn: &PyAny,
//     enforce_dtype: Option<&DataType>,
// ) -> DaftResult<(
//     std::slice::Iter<'a, Tgt::Bytes>,
//     usize,
//     std::slice::Iter<'a, u64>,
//     usize,
// )> {
//     let np_dtype = pyarray.getattr(pyo3::intern!(pyarray.py(), "dtype"))?;

//     let datatype = from_numpy_dtype_fn
//         .call1((np_dtype,))?
//         .getattr(pyo3::intern!(pyarray.py(), "_dtype"))?
//         .extract::<PyDataType>()?;
//     let datatype = datatype.dtype;
//     if let Some(enforce_dtype) = enforce_dtype {
//         if enforce_dtype != &datatype {
//             return Err(DaftError::ValueError(format!(
//                 "Expected Numpy array to be of type: {enforce_dtype} but is {datatype} at index: {index}",
//             )));
//         }
//     }
//     if !datatype.is_numeric() {
//         return Err(DaftError::ValueError(format!(
//             "Numpy array has unsupported type {} at index: {index}",
//             datatype
//         )));
//     }
//     with_match_numeric_daft_types!(datatype, |$N| {
//         type Src = <$N as DaftNumericType>::Native;
//         let pyarray: PyReadonlyArrayDyn<'_, Src> = pyarray.extract()?;
//         // All of the conditions given in the ndarray::ArrayView::from_shape_ptr() docstring must be met in order to do
//         // the pyarray.as_array conversion: https://docs.rs/ndarray/0.15.6/ndarray/type.ArrayView.html#method.from_shape_ptr
//         // Relevant:
//         //  1. Must have non-negative strides.

//         // TODO(Clark): Double-check that we're covering all bases here for this unsafe handoff.
//         if pyarray.strides().iter().any(|s| *s < 0) {
//             return Err(DaftError::ValueError(format!(
//                 "we only support numpy arrays with non-negative strides, but got {:?} at index: {}",
//                 pyarray.strides(), index
//             )));
//         }

//         let pyarray = pyarray.as_array();
//         let owned_arr;
//         // Create 1D slice from potentially non-contiguous and non-C-order arrays.
//         // This will only create a copy if the ndarray is non-contiguous.
//         let sl: &[Src] = match pyarray.as_slice_memory_order() {
//             Some(sl) => sl,
//             None => {
//                 owned_arr = pyarray.to_owned();
//                 owned_arr.as_slice_memory_order().unwrap()
//             }
//         };
//         Ok((sl.iter().map(|v| <Wrapping<Tgt> as NumCast>::from(*v).unwrap().0), sl.len(), pyarray.shape().iter().map(|v| *v as u64), pyarray.shape().len()))
//     })
// }

// #[cfg(feature = "python")]
// fn append_values_from_numpy<
//     Tgt: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
// >(
//     pyarray: &PyAny,
//     index: usize,
//     from_numpy_dtype_fn: &PyAny,
//     enforce_dtype: Option<&DataType>,
//     values_vec: &mut Vec<Tgt>,
//     shapes_vec: &mut Vec<u64>,
// ) -> DaftResult<(usize, usize)> {
//     use crate::python::PyDataType;
//     use std::num::Wrapping;

//     let np_dtype = pyarray.getattr(pyo3::intern!(pyarray.py(), "dtype"))?;

//     let datatype = from_numpy_dtype_fn
//         .call1((np_dtype,))?
//         .getattr(pyo3::intern!(pyarray.py(), "_dtype"))?
//         .extract::<PyDataType>()?;
//     let datatype = datatype.dtype;
//     if let Some(enforce_dtype) = enforce_dtype {
//         if enforce_dtype != &datatype {
//             return Err(DaftError::ValueError(format!(
//                 "Expected Numpy array to be of type: {enforce_dtype} but is {datatype} at index: {index}",
//             )));
//         }
//     }
//     if !datatype.is_numeric() {
//         return Err(DaftError::ValueError(format!(
//             "Numpy array has unsupported type {} at index: {index}",
//             datatype
//         )));
//     }
//     with_match_numeric_daft_types!(datatype, |$N| {
//         type Src = <$N as DaftNumericType>::Native;
//         let pyarray: PyReadonlyArrayDyn<'_, Src> = pyarray.extract()?;
//         // All of the conditions given in the ndarray::ArrayView::from_shape_ptr() docstring must be met in order to do
//         // the pyarray.as_array conversion: https://docs.rs/ndarray/0.15.6/ndarray/type.ArrayView.html#method.from_shape_ptr
//         // Relevant:
//         //  1. Must have non-negative strides.

//         // TODO(Clark): Double-check that we're covering all bases here for this unsafe handoff.
//         if pyarray.strides().iter().any(|s| *s < 0) {
//             return Err(DaftError::ValueError(format!(
//                 "we only support numpy arrays with non-negative strides, but got {:?} at index: {}",
//                 pyarray.strides(), index
//             )));
//         }

//         let pyarray = pyarray.as_array();
//         let owned_arr;
//         // Create 1D slice from potentially non-contiguous and non-C-order arrays.
//         // This will only create a copy if the ndarray is non-contiguous.
//         let sl: &[Src] = match pyarray.as_slice_memory_order() {
//             Some(sl) => sl,
//             None => {
//                 owned_arr = pyarray.to_owned();
//                 owned_arr.as_slice_memory_order().unwrap()
//             }
//         };
//         values_vec.extend(sl.iter().map(|v| <Wrapping<Tgt> as NumCast>::from(*v).unwrap().0));
//         shapes_vec.extend(pyarray.shape().iter().map(|v| *v as u64));
//         Ok((sl.len(), pyarray.shape().len()))
//     })
// }

// struct TensorBuilder<T: TensorBufferBuilder> {
//     tensor_buffer_builder: T,
//     np_as_array_fn: PyAny,
//     from_numpy_dtype_fn: PyAny,
// }

// impl TensorBuilder {
//     fn add_ndarray(&self, pyarray: &PyAny) -> DaftResult<()> {
//         let pyarray = self.np_as_array_fn.call1((pyarray,))?;
//         let np_dtype = pyarray.getattr(pyo3::intern!(pyarray.py(), "dtype"))?;

//         let datatype = self
//             .from_numpy_dtype_fn
//             .call1((np_dtype,))?
//             .getattr(pyo3::intern!(pyarray.py(), "_dtype"))?
//             .extract::<PyDataType>()?;
//         let datatype = datatype.dtype;
//         if !datatype.is_numeric() {
//             return Err(DaftError::ValueError(format!(
//                 "Numpy array has unsupported type {}",
//                 datatype
//             )));
//         }
//         with_match_numeric_daft_types!(datatype, |$N| {
//             type Src = <$N as DaftNumericType>::Native;
//             let pyarray: PyReadonlyArrayDyn<'_, Src> = pyarray.extract()?;
//             // All of the conditions given in the ndarray::ArrayView::from_shape_ptr() docstring must be met in order to do
//             // the pyarray.as_array conversion: https://docs.rs/ndarray/0.15.6/ndarray/type.ArrayView.html#method.from_shape_ptr
//             // Relevant:
//             //  1. Must have non-negative strides.

//             // TODO(Clark): Double-check that we're covering all bases here for this unsafe handoff.
//             if pyarray.strides().iter().any(|s| *s < 0) {
//                 return Err(DaftError::ValueError(format!(
//                     "we only support numpy arrays with non-negative strides, but got {:?}",
//                     pyarray.strides()
//                 )));
//             }

//             self.tensor_buffer_builder.extend_with_ndarray(pyarray.as_array());
//             Ok(())
//         })
//     }
// }

// struct ImageBuilder<T: ImageBufferBuilder> {
//     image_buffer_builder: T,
// }

// impl ImageBuilder {
//     fn add_image(&self, image: &PyAny) -> DaftResult<()> {
//         todo!("todo");
//     }
// }

// enum TypeBuilder {
//     Tensor(TensorBuilder),
//     Image(ImageBuilder),
//     List(ListBuilder),
// }

// struct Builder {
//     type_builder: TypeBuilder,
//     py_obj_type_classifier: PyObjTypeClassifier,
// }

// impl Builder {
//     fn add(&self, py_obj: &PyAny) -> DaftResult<()> {
//         match (
//             self.py_obj_type_classifier.classify(py_obj),
//             self.type_builder,
//         ) {
//             (PyObjType::Ndarray | PyObjType::Image, TypeBuilder::Tensor(tensor_builder)) => {
//                 tensor_builder.add_ndarray(py_obj)
//             }
//             (PyObjType::Image, TypeBuilder::Image(image_builder)) => {
//                 image_builder.add_image(py_obj)
//             }
//             (PyObjType::Ndarray, TypeBuilder::Image(image_builder)) => {
//                 image_builder.add_ndarray(py_obj)
//             }
//             (PyObjType::Iterator, TypeBuilder::List(list_builder)) => list_builder.add_iter(py_obj),
//             (PyObjType::Ndarray, TypeBuilder::List(list_builder)) => {
//                 list_builder.add_ndarray(py_obj)
//             }
//             (py_obj_type, type_builder) => unimplemented!("not implemented"),
//         }
//     }
// }

// impl<T> BufferBuilder for T
// where
//     T: TensorBufferBuilder + ImageBufferBuilder + ListBufferBuilder,
// {
//     fn extend<'a>(&'a self, py_obj: &'a PyAny) -> DaftResult<()> {
//         match self.py_obj_type_classifier.classify(py_obj) {
//             PyObjType::Image | PyObjType::Ndarray => {
//                 let ndarray = self.np_as_array_fn.call1((object,))?;
//                 self.extend_with_ndarray(ndarray)
//             }
//             PyObjType::Iterator => todo!("not implemented"),
//             PyObjType::Unknown(py_type) => {
//                 unimplemented!(format!("not implemented for {}", py_type))
//             }
//         }
//     }

//     fn push_null(&self) {
//         self.offsets.push(self.offsets.last());
//         self.shape_offsets.push(self.shape_offsets.last());
//     }
// }

// struct PrimitiveTensorBufferBuilder<T> {
//     data: Vec<T>,
//     offsets: Vec<i64>,
//     shapes: Vec<u64>,
//     shape_offsets: Vec<i64>,
// }

// impl<T> PrimitiveTensorBufferBuilder<T> {
//     fn new(num_tensors: usize) -> Self {
//         Self {
//             data: Vec::with_capacity(num_tensors),
//             offsets: Vec::with_capacity(num_tensors + 1),
//             shapes: Vec::with_capacity(num_tensors),
//             shape_offsets: Vec::with_capacity(num_tensors + 1),
//         }
//     }
// }

// fn ndarray_to_slice<'a, T>(ndarray: &'a ArrayView<'a, T, IxDyn>) -> &'a [T] {
//     let owned_arr;
//     // Create 1D slice from potentially non-contiguous and non-C-order arrays.
//     // This will only create a copy if the ndarray is non-contiguous.
//     match ndarray.as_slice_memory_order() {
//         Some(sl) => sl,
//         None => {
//             owned_arr = ndarray.to_owned();
//             owned_arr.as_slice_memory_order().unwrap()
//         }
//     }
// }

// impl<T> TensorBufferBuilder for PrimitiveTensorBufferBuilder<T>
// where
//     T: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
// {
//     fn extend<'a, U>(&'a self, ndarray: &'a ArrayView<'a, U, IxDyn>)
//     where
//         U: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
//     {
//         let data = ndarray_to_slice(ndarray);
//         self.data.extend(
//             data.iter()
//                 .map(|v| <Wrapping<T> as NumCast>::from(*v).unwrap().0),
//         );
//         self.offsets.push(self.offsets.last() + data.len() as u64);
//         let shape = ndarray.shape();
//         self.shapes.extend(shape.iter());
//         self.shape_offsets
//             .push(self.shape_offsets.last() + shape.len() as u64);
//     }

//     fn push_null(&self) {
//         self.offsets.push(self.offsets.last());
//         self.shape_offsets.push(self.shape_offsets.last());
//     }
// }

// struct FixedShapePrimitiveTensorBufferBuilder<T> {
//     data: Vec<T>,
//     shape: Vec<u64>,
// }

// impl<T> FixedShapePrimitiveTensorBufferBuilder<T> {
//     fn new(num_tensors: u64, shape: Vec<u64>) -> Self {
//         Self {
//             data: Vec::with_capacity(num_tensors),
//             shape,
//         }
//     }
// }

// impl<T> TensorBufferBuilder for FixedShapePrimitiveTensorBufferBuilder<T>
// where
//     T: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
// {
//     fn extend<'a, U>(&'a self, ndarray: &'a ArrayView<'a, U, IxDyn>)
//     where
//         U: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
//     {
//         let data = ndarray_to_slice(ndarray);
//         self.data.extend(
//             data.iter()
//                 .map(|v| <Wrapping<T> as NumCast>::from(*v).unwrap().0),
//         );
//     }

//     fn push_null(&self) {
//         self.offsets
//             .push(self.offsets.last() + self.shape.iter().product() as u64);
//     }
// }

// struct BinaryTensorBufferBuilder {
//     data: Vec<u8>,
//     offsets: Vec<i64>,
//     dtypes: Vec<DataType>,
// }

// impl BinaryTensorBufferBuilder {
//     fn new(num_tensors: usize) -> Self {
//         Self {
//             data: Vec::with_capacity(num_tensors),
//             offsets: Vec::with_capacity(num_tensors + 1),
//             dtypes: Vec::with_capacity(num_tensors),
//         }
//     }
// }

// impl TensorBufferBuilder for BinaryTensorBufferBuilder {
//     fn extend<'a, U>(&'a self, ndarray: &'a ArrayView<'a, U, IxDyn>)
//     where
//         U: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
//     {
//         let data = ndarray_to_slice(ndarray);
//         self.data
//             .extend(data.iter().flat_map(|v| v.to_le_bytes().as_ref().iter()));
//         self.offsets.push(self.offsets.last() + data.len() as u64);
//         let shape = ndarray.shape();
//         self.shapes.extend(shape.iter());
//         self.shape_offsets
//             .push(self.shape_offsets.last() + shape.len() as u64);
//         self.dtypes
//             .push(arrow2::datatypes::DataType::from(U::PRIMITIVE).into());
//     }

//     fn push_null(&self) {
//         self.offsets.push(self.offsets.last());
//         self.shape_offsets.push(self.shape_offsets.last());
//     }
// }

// struct TensorData<T> {
//     data: Vec<T>,
//     data_offsets: Vec<i64>,
//     shapes: Vec<u64>,
//     shape_offsets: Vec<i64>,
// }

// struct HeterogeneousTypedTensorData {
//     tensor_data: TensorData<u8>,
//     dtypes: Vec<DataType>,
// }

// #[cfg(feature = "python")]
// fn extract_tensor_from_ndarray<T>(
//     py: Python<'_>,
//     pyarray: PyAny,
//     tensor_buffer_builder: &T,
//     index: usize,
//     from_numpy_dtype_fn: &PyAny,
//     enforce_dtype: Option<&DataType>,
// ) -> DaftResult<()>
// where
//     T: TensorBufferBuilder,
// {
//     let np_dtype = pyarray.getattr(pyo3::intern!(pyarray.py(), "dtype"))?;

//     let datatype = from_numpy_dtype_fn
//         .call1((np_dtype,))?
//         .getattr(pyo3::intern!(pyarray.py(), "_dtype"))?
//         .extract::<PyDataType>()?;
//     let datatype = datatype.dtype;
//     if let Some(enforce_dtype) = enforce_dtype {
//         if enforce_dtype != &datatype {
//             return Err(DaftError::ValueError(format!(
//                 "Expected Numpy array to be of type: {enforce_dtype} but is {datatype} at index: {index}",
//             )));
//         }
//     }
//     if !datatype.is_numeric() {
//         return Err(DaftError::ValueError(format!(
//             "Numpy array has unsupported type {} at index: {index}",
//             datatype
//         )));
//     }
//     with_match_numeric_daft_types!(datatype, |$N| {
//         type Src = <$N as DaftNumericType>::Native;
//         let pyarray: PyReadonlyArrayDyn<'_, Src> = pyarray.extract()?;
//         // All of the conditions given in the ndarray::ArrayView::from_shape_ptr() docstring must be met in order to do
//         // the pyarray.as_array conversion: https://docs.rs/ndarray/0.15.6/ndarray/type.ArrayView.html#method.from_shape_ptr
//         // Relevant:
//         //  1. Must have non-negative strides.

//         // TODO(Clark): Double-check that we're covering all bases here for this unsafe handoff.
//         if pyarray.strides().iter().any(|s| *s < 0) {
//             return Err(DaftError::ValueError(format!(
//                 "we only support numpy arrays with non-negative strides, but got {:?} at index: {}",
//                 pyarray.strides(), index
//             )));
//         }

//         let pyarray = pyarray.as_array();
//         tensor_buffer_builder.extend(pyarray);
//         Ok(())
//         // let owned_arr;
//         // // Create 1D slice from potentially non-contiguous and non-C-order arrays.
//         // // This will only create a copy if the ndarray is non-contiguous.
//         // let sl: &[Src] = match pyarray.as_slice_memory_order() {
//         //     Some(sl) => sl,
//         //     None => {
//         //         owned_arr = pyarray.to_owned();
//         //         owned_arr.as_slice_memory_order().unwrap()
//         //     }
//         // };
//         // values_vec.extend(sl.iter().map(|v| <Wrapping<Tgt> as NumCast>::from(*v).unwrap().0));
//         // shapes_vec.extend(pyarray.shape().iter().map(|v| *v as u64));
//         // Ok((sl.len(), pyarray.shape().len()))
//     })
//     // let pyarray: PyReadonlyArrayDyn<'_, T> = pyarray.extract()?;
//     // // All of the conditions given in the ndarray::ArrayView::from_shape_ptr() docstring must be met in order to do
//     // // the pyarray.as_array conversion: https://docs.rs/ndarray/0.15.6/ndarray/type.ArrayView.html#method.from_shape_ptr
//     // // Relevant:
//     // //  1. Must have non-negative strides.

//     // // TODO(Clark): Double-check that we're covering all bases here for this unsafe handoff.
//     // if pyarray.strides().iter().any(|s| *s < 0) {
//     //     return Err(DaftError::ValueError(format!(
//     //         "we only support numpy arrays with non-negative strides, but got {:?} at index: {}",
//     //         pyarray.strides(),
//     //         index
//     //     )));
//     // }

//     // let pyarray = pyarray.as_array();
//     // let owned_arr;
//     // // Create 1D slice from potentially non-contiguous and non-C-order arrays.
//     // // This will only create a copy if the ndarray is non-contiguous.
//     // let sl: &[T] = match pyarray.as_slice_memory_order() {
//     //     Some(sl) => sl,
//     //     None => {
//     //         owned_arr = pyarray.to_owned();
//     //         owned_arr.as_slice_memory_order().unwrap()
//     //     }
//     // };
//     // Ok((sl, pyarray.shape(), datatype))
// }

// #[cfg(feature = "python")]
// fn extract_tensors<'a, T>(
//     py: Python<'_>,
//     python_objects: &'a PythonArray,
//     tensor_buffer_builder: &T,
//     child_dtype: &DataType,
//     enforce_dtype: Option<&DataType>,
// ) -> DaftResult<()>
// where
//     T: TensorBufferBuilder,
// {
//     let from_numpy_dtype = {
//         PyModule::import(py, pyo3::intern!(py, "daft.datatype"))?
//             .getattr(pyo3::intern!(py, "DataType"))?
//             .getattr(pyo3::intern!(py, "from_numpy_dtype"))?
//     };

//     let pytype = match child_dtype {
//         dtype if dtype.is_integer() => Ok("int"),
//         dtype if dtype.is_floating() => Ok("float"),
//         dtype => Err(DaftError::ValueError(format!(
//             "We only support numeric types when converting to List or FixedSizeList, got {dtype}"
//         ))),
//     }?;

//     let py_type_fn = { PyModule::import(py, pyo3::intern!(py, "builtins"))?.getattr(pytype)? };
//     let py_memory_view = py
//         .import("builtins")?
//         .getattr(pyo3::intern!(py, "memoryview"))?;
//     for (i, object) in python_objects.as_arrow().iter().enumerate() {
//         if let Some(object) = object {
//             let object = object.into_py(py);
//             let object = object.as_ref(py);

//             let supports_buffer_protocol = py_memory_view.call1((object,)).is_ok();
//             let supports_array_interface_protocol =
//                 object.hasattr(pyo3::intern!(py, "__array_interface__"))?;
//             let supports_array_protocol = object.hasattr(pyo3::intern!(py, "__array__"))?;

//             if supports_buffer_protocol
//                 || supports_array_interface_protocol
//                 || supports_array_protocol
//             {
//                 // Path if object supports buffer/array protocols.
//                 let np_as_array_fn = py.import("numpy")?.getattr(pyo3::intern!(py, "asarray"))?;
//                 let pyarray = np_as_array_fn.call1((object,))?;
//                 extract_tensor_from_ndarray(
//                     py,
//                     pyarray,
//                     tensor_buffer_builder,
//                     i,
//                     from_numpy_dtype,
//                     enforce_dtype,
//                 )?;
//                 // let (array_values, num_values, array_shape, shape_size) =
//                 //     append_values_from_numpy(pyarray, i, from_numpy_dtype, enforce_dtype)?;
//                 if let Some(list_size) = list_size {
//                     if ndarray_data.len() != list_size {
//                         return Err(DaftError::ValueError(format!(
//                                 "Expected Array-like Object to have {list_size} elements but got {} at index {}",
//                                 ndarray_data.len(), i
//                             )));
//                     }
//                 } else {
//                     offsets_vec.push(offsets_vec.last().unwrap() + num_values as i64);
//                     shape_offsets_vec.push(shape_offsets_vec.last().unwrap() + shape_size as i64);
//                 }
//             } else {
//                 // Path if object does not support buffer/array protocols.
//                 // Try a best-effort conversion of the elements.
//                 let pyiter = object.iter();
//                 if let Ok(pyiter) = pyiter {
//                     // has an iter
//                     let casted_iter = pyiter.map(|v| v.and_then(|f| py_type_fn.call1((f,))));
//                     let collected = if child_dtype.is_integer() {
//                         let int_iter = casted_iter
//                             .map(|v| v.and_then(|v| v.extract::<i64>()))
//                             .map(|v| {
//                                 v.and_then(|v| {
//                                     <Wrapping<Tgt> as NumCast>::from(v).ok_or(
//                                         DaftError::ComputeError(format!(
//                                             "Could not convert pyint to i64 at index {i}"
//                                         ))
//                                         .into(),
//                                     )
//                                 })
//                             })
//                             .map(|v| v.map(|v| v.0));

//                         int_iter.collect::<PyResult<Vec<_>>>()
//                     } else if child_dtype.is_floating() {
//                         let float_iter = casted_iter
//                             .map(|v| v.and_then(|v| v.extract::<f64>()))
//                             .map(|v| {
//                                 v.and_then(|v| {
//                                     <Wrapping<Tgt> as NumCast>::from(v).ok_or(
//                                         DaftError::ComputeError(
//                                             "Could not convert pyfloat to f64".into(),
//                                         )
//                                         .into(),
//                                     )
//                                 })
//                             })
//                             .map(|v| v.map(|v| v.0));
//                         float_iter.collect::<PyResult<Vec<_>>>()
//                     } else {
//                         unreachable!(
//                             "dtype should either be int or float at this point; this is a bug"
//                         );
//                     };

//                     if collected.is_err() {
//                         log::warn!("Could not convert python object to list at index: {i} for input series: {}", python_objects.name())
//                     }
//                     let collected: Vec<Tgt> = collected?;
//                     if let Some(list_size) = list_size {
//                         if collected.len() != list_size {
//                             return Err(DaftError::ValueError(format!(
//                                 "Expected Array-like Object to have {list_size} elements but got {} at index {}",
//                                 collected.len(), i
//                             )));
//                         }
//                     } else {
//                         let offset = offsets_vec.last().unwrap() + collected.len() as i64;
//                         offsets_vec.push(offset);
//                         shapes_vec.extend(vec![1]);
//                         shape_offsets_vec.push(1i64);
//                     }
//                     values_vec.extend_from_slice(collected.as_slice());
//                 } else {
//                     return Err(DaftError::ValueError(format!(
//                         "Python Object is neither array-like or an iterable at index {}. Can not convert to a list. object type: {}",
//                         i, object.getattr(pyo3::intern!(py, "__class__"))?)));
//                 }
//             }
//         } else if let Some(list_size) = list_size {
//             values_vec.extend(iter::repeat(Tgt::default()).take(list_size));
//         } else {
//             let offset = offsets_vec.last().unwrap();
//             offsets_vec.push(*offset);
//             if let Some(shape_size) = shape_size {
//                 shapes_vec.extend(iter::repeat(1).take(shape_size));
//                 shape_offsets_vec.push(shape_offsets_vec.last().unwrap() + shape_size as i64);
//             } else {
//                 shape_offsets_vec.push(*shape_offsets_vec.last().unwrap());
//             }
//         }
//     }
//     if list_size.is_some() {
//         Ok((values_vec, None, None, None))
//     } else {
//         Ok((
//             values_vec,
//             Some(offsets_vec),
//             Some(shapes_vec),
//             Some(shape_offsets_vec),
//         ))
//     }
// }

// trait TensorBufferBuilder {
//     fn extend_with_ndarray<'a, U>(&'a self, ndarray: ArrayView<'a, U, IxDyn>)
//     where
//         U: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType;
// }

// trait NdarrayBufferBuilder {
//     fn extend_with_ndarray<'a, U>(&'a self, ndarray: ArrayView<'a, U, IxDyn>)
//     where
//         U: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType;
// }

// trait TensorBufferBuilder: NdarrayBufferBuilder {
//     fn extend_with_pyarray(&self, py_array: &PyAny);
// }

// trait ImageBufferBuilder {
//     fn extend_with_image(&self, py_img: &PyAny) -> DaftResult<()>;
// }

// trait ListBufferBuilder {
//     fn extend_with_iter(&self, py_iter: &PyIterator) -> DaftResult<()>;
// }

// trait ArrayBuilder {
//     fn extend(&self, py_obj: &PyAny) -> DaftResult<()>;

//     fn push_null(&self);
// }

// struct TensorAdder<T: TensorBufferBuilder> {
//     buffer_builder: &T,
//     np_as_array_fn: PyAny,
//     from_numpy_dtype_fn: PyAny,
// }

// impl<T> TensorAdder<T> {
//     fn add_pyarray(&self, py_array: &PyAny) -> DaftResult<()> {
//         let pyarray = self.np_as_array_fn.call1((pyarray,))?;
//         let np_dtype = pyarray.getattr(pyo3::intern!(pyarray.py(), "dtype"))?;

//         let datatype = self
//             .from_numpy_dtype_fn
//             .call1((np_dtype,))?
//             .getattr(pyo3::intern!(pyarray.py(), "_dtype"))?
//             .extract::<PyDataType>()?;
//         let datatype = datatype.dtype;
//         if !datatype.is_numeric() {
//             return Err(DaftError::ValueError(format!(
//                 "Numpy array has unsupported type {}",
//                 datatype
//             )));
//         }
//         with_match_numeric_daft_types!(datatype, |$N| {
//             type Src = <$N as DaftNumericType>::Native;
//             let pyarray: PyReadonlyArrayDyn<'_, Src> = pyarray.extract()?;
//             // All of the conditions given in the ndarray::ArrayView::from_shape_ptr() docstring must be met in order to do
//             // the pyarray.as_array conversion: https://docs.rs/ndarray/0.15.6/ndarray/type.ArrayView.html#method.from_shape_ptr
//             // Relevant:
//             //  1. Must have non-negative strides.

//             // TODO(Clark): Double-check that we're covering all bases here for this unsafe handoff.
//             if pyarray.strides().iter().any(|s| *s < 0) {
//                 return Err(DaftError::ValueError(format!(
//                     "we only support numpy arrays with non-negative strides, but got {:?}",
//                     pyarray.strides()
//                 )));
//             }

//             self.buffer_builder.extend_with_ndarray(pyarray.as_array());
//             Ok(())
//         })
//     }
// }

// struct ImageAdder<T: ImageBufferBuilder> {
//     buffer_builder: &T,
// }

// impl<T> ImageAdder<T> {
//     fn add_image(&self, py_img: &PyAny) -> DaftResult<()> {
//         self.buffer_builder.extend_with_image(py_img)
//     }
// }

// struct TensorArrayBuilder<'a, T> {
//     array_adder: TensorAdder<PrimitiveTensorBufferBuilder<T>>,
//     py_obj_type_classifier: PyObjTypeClassifier<'a>,
// }

// impl<'a, T> ArrayBuilder for TensorArrayBuilder<'a, T> {
//     fn extend(&self, py_obj: &PyAny) -> DaftResult<()> {
//         match self.py_obj_type_classifier.classify(py_obj) {
//             PyObjType::Ndarray(..) | PyObjType::Image(..) => {
//                 Ok(self.array_adder.add_pyarray(py_obj))
//             }
//             _ => unimplemented!("not implemented"),
//         }
//     }
// }

// struct FixedShapeTensorArrayBuilder<'a, T> {
//     array_adder: TensorAdder<FixedShapePrimitiveTensorBufferBuilder<T>>,
//     shape: Vec<u64>,
//     py_obj_type_classifier: PyObjTypeClassifier<'a>,
// }

// impl<'a, T> ArrayBuilder for TensorArrayBuilder<'a, T> {
//     fn extend(&self, py_obj: &PyAny) -> DaftResult<()> {
//         match self.py_obj_type_classifier.classify(py_obj) {
//             PyObjType::Ndarray(_, shape) => {
//                 if shape != self.shape {
//                     return Err(DaftError::ValueError(format!("Shape of ndarray doesn't match the shape on the fixed-shape tensor type: expected {}, got {}", self.shape, shape)));
//                 }
//                 Ok(self.array_adder.add_pyarray(py_obj))
//             }
//             PyObjType::Image(mode, height, width) => {
//                 let shape = vec![height, width, mode.num_channels() as u64];
//                 if shape != self.shape {
//                     return Err(DaftError::ValueError(format!("Shape of image doesn't match the shape on the fixed-shape tensor type: expected {}, got {}", self.shape, shape)));
//                 }
//                 Ok(self.array_adder.add_pyarray(py_obj))
//             }
//             _ => unimplemented!("not implemented"),
//         }
//     }
// }

// struct Builder<'a, T> {
//     buffer_builder: T,
//     py_obj_type_classifier: PyObjTypeClassifier<'a>,
//     np_as_array_fn: PyAny,
//     from_numpy_dtype_fn: PyAny,
// }

// trait PushNull {
//     fn push_null(&self);
// }

// impl<'a, T> PushNull for Builder<'a, T>
// where
//     T: BufferBuilder,
// {
//     fn push_null(&self) {
//         self.buffer_builder.push_null();
//     }
// }

// trait ExtendBuffer {
//     fn extend(&self, py_obj: &PyAny) -> DaftResult<()>;
// }

// impl<'a, T> Builder<'a, T>
// where
//     T: TensorBufferBuilder,
// {
//     fn add_pyarray(&self, pyarray: &PyAny) -> DaftResult<()> {
//         let pyarray = self.np_as_array_fn.call1((pyarray,))?;
//         let np_dtype = pyarray.getattr(pyo3::intern!(pyarray.py(), "dtype"))?;

//         let datatype = self
//             .from_numpy_dtype_fn
//             .call1((np_dtype,))?
//             .getattr(pyo3::intern!(pyarray.py(), "_dtype"))?
//             .extract::<PyDataType>()?;
//         let datatype = datatype.dtype;
//         if !datatype.is_numeric() {
//             return Err(DaftError::ValueError(format!(
//                 "Numpy array has unsupported type {}",
//                 datatype
//             )));
//         }
//         with_match_numeric_daft_types!(datatype, |$N| {
//             type Src = <$N as DaftNumericType>::Native;
//             let pyarray: PyReadonlyArrayDyn<'_, Src> = pyarray.extract()?;
//             // All of the conditions given in the ndarray::ArrayView::from_shape_ptr() docstring must be met in order to do
//             // the pyarray.as_array conversion: https://docs.rs/ndarray/0.15.6/ndarray/type.ArrayView.html#method.from_shape_ptr
//             // Relevant:
//             //  1. Must have non-negative strides.

//             // TODO(Clark): Double-check that we're covering all bases here for this unsafe handoff.
//             if pyarray.strides().iter().any(|s| *s < 0) {
//                 return Err(DaftError::ValueError(format!(
//                     "we only support numpy arrays with non-negative strides, but got {:?}",
//                     pyarray.strides()
//                 )));
//             }

//             self.buffer_builder.extend_with_ndarray(pyarray.as_array());
//             Ok(())
//         })
//     }
// }

// impl<'a, T> ExtendBuffer for Builder<'a, T>
// where
//     T: TensorBufferBuilder,
// {
//     fn extend(&self, py_obj: &PyAny) -> DaftResult<()> {
//         match self.py_obj_type_classifier.classify(py_obj) {
//             PyObjType::Ndarray | PyObjType::Image => Ok(self.add_pyarray(py_obj)),
//             _ => unimplemented!("not implemented"),
//         }
//     }
// }

// impl<'a, T> Builder<'a, T>
// where
//     T: ImageBufferBuilder,
// {
//     fn add_image(&self, py_image: &PyAny) -> DaftResult<()> {
//         todo!("not implemented yet");
//     }
// }

// impl<'a, T> ExtendBuffer for Builder<'a, T>
// where
//     T: ImageBufferBuilder,
// {
//     fn extend(&self, py_obj: &PyAny) -> DaftResult<()> {
//         match self.py_obj_type_classifier.classify(py_obj) {
//             PyObjType::Image => Ok(self.add_image(py_obj)),
//             PyObjType::Ndarray => Ok(self.add_pyarray(py_obj)),
//             _ => unimplemented!("not implemented"),
//         }
//     }
// }

// impl<'a, T> Builder<'a, T>
// where
//     T: ListBufferBuilder,
// {
//     fn add_iter(&self, py_iter: &PyAny) -> DaftResult<()> {
//         todo!("not implemented yet");
//     }
// }

// impl<'a, T> ExtendBuffer for Builder<'a, T>
// where
//     T: ListBufferBuilder,
// {
//     fn extend(&self, py_obj: &PyAny) -> DaftResult<()> {
//         match self.py_obj_type_classifier.classify(py_obj) {
//             PyObjType::Iterator => Ok(self.add_iter(py_obj)),
//             PyObjType::Ndarray => Ok(self.add_pyarray(py_obj)),
//             _ => unimplemented!("not implemented"),
//         }
//     }
// }

// struct HeterogeneousImageBufferBuilder {
//     tensor_adder: TensorAdder<BinaryTensorBufferBuilder>,
//     modes: Vec<ImageMode>,
// }

// impl ImageBufferBufferBuilder for HeterogeneousImageBufferBuilder {
//     fn extend_with_image(&self, py_img: &PyAny) {
//         let pil_mode = py_img
//             .getattr(pyo3::intern!(py_img.py(), "mode"))?
//             .extract::<&str>()?;
//         let image_mode = ImageMode::from_pil_mode_str(pil_mode)?;
//         self.modes.push(image_mode);
//         self.tensor_adder.add_pyarray(py_img);
//     }
// }

enum PyObjType {
    Image,
    Ndarray,
    Iterator,
    Unknown(String),
}

// enum PyObjType {
//     Image(ImageMode, u64, u64),
//     Ndarray(DataType, Vec<u64>),
//     Iterator,
//     Unknown(String),
// }

struct PyObjTypeClassifier<'a> {
    py_memory_view: &'a PyAny,
    from_numpy_dtype_fn: &'a PyAny,
    py_pil_image_type: Option<&'a PyAny>,
}

impl<'a> PyObjTypeClassifier<'a> {
    fn new(py: Python<'a>) -> DaftResult<Self> {
        Ok(Self {
            py_memory_view: py
                .import("builtins")?
                .getattr(pyo3::intern!(py, "memoryview"))?,
            from_numpy_dtype_fn: py
                .import(pyo3::intern!(py, "daft.datatype"))?
                .getattr(pyo3::intern!(py, "DataType"))?
                .getattr(pyo3::intern!(py, "from_numpy_dtype"))?,
            py_pil_image_type: py
                .import("PIL.Image")
                .and_then(|m| m.getattr(pyo3::intern!(py, "Image")))
                .ok(),
        })
    }

    fn classify(&self, py_obj: &PyAny) -> DaftResult<PyObjType> {
        let py = py_obj.py();
        let supports_buffer_protocol = self.py_memory_view.call1((py_obj,)).is_ok();
        let supports_array_interface_protocol =
            py_obj.hasattr(pyo3::intern!(py, "__array_interface__"))?;
        let supports_array_protocol = py_obj.hasattr(pyo3::intern!(py, "__array__"))?;
        let supports_iter_protocol = py_obj.hasattr(pyo3::intern!(py, "__iter__"))?;
        let supports_sequence_protocol = py_obj.hasattr(pyo3::intern!(py, "__getitem__"))?;

        if supports_buffer_protocol || supports_array_interface_protocol || supports_array_protocol
        {
            return Ok(PyObjType::Ndarray)
            // let np_dtype = py_obj.getattr(pyo3::intern!(py, "dtype"))?;
            // let datatype = self
            //     .from_numpy_dtype_fn
            //     .call1((np_dtype,))?
            //     .getattr(pyo3::intern!(py, "_dtype"))?
            //     .extract::<PyDataType>()?;
            // let dtype = datatype.dtype;
            // let shape = py_obj
            //     .getattr(pyo3::intern!(py, "shape"))?
            //     .extract::<Vec<u64>>()?;
            // return Ok(PyObjType::Ndarray(dtype, shape));
        } else if let Some(py_pil_image_type) = self.py_pil_image_type && py_obj.is_instance(py_pil_image_type)? {
            return Ok(PyObjType::Image)
            // let pil_mode = py_obj.getattr(pyo3::intern!(py, "mode"))?.extract::<&str>()?;
            // let image_mode = ImageMode::from_pil_mode_str(pil_mode)?;
            // let (width, height) = py_obj.getattr(pyo3::intern!(py, "size"))?.extract::<(u64, u64)>()?;
            // return Ok(PyObjType::Image(image_mode, height, width));
        } else if supports_iter_protocol || supports_sequence_protocol {
            return Ok(PyObjType::Iterator);
        } else {
            return Ok(PyObjType::Unknown(py_obj.get_type().to_string()));
        }
    }
}

// trait BufferBuilder {
//     type Output;

//     fn finalize(&self) -> Self::Output;

//     fn push_null(&self);
// }

trait Finalizable {
    type Output;

    fn finalize(self) -> Self::Output;
}

trait PushNull {
    fn push_null(&mut self);
}

trait ExtendWithNdarray {
    fn extend_with_ndarray<'a, U>(&mut self, ndarray: ArrayView<'a, U, IxDyn>) -> DaftResult<()>
    where
        U: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType;
}

trait ExtendWithIter {
    fn extend_with_iter(&mut self, py_iter: &PyIterator) -> DaftResult<()>;
}

trait AddPyArray {
    fn add_py_array(&mut self, py_array: &PyAny) -> DaftResult<()>;
}

trait AddPyIter {
    fn add_py_iter(&mut self, py_iter: &PyAny) -> DaftResult<()>;
}

trait AddImage {
    fn add_image(&mut self, py_img: &PyAny) -> DaftResult<()>;
}

trait ExtendWithPyObj {
    fn extend(&mut self, py_obj: &PyAny) -> DaftResult<()>;
}

// trait ArrayBuilder {
//     type Output;

//     fn extend(&self, py_obj: &PyAny) -> DaftResult<()>;

//     fn push_null(&self);

//     fn finalize(&self) -> Self::Output;
// }

struct TensorBufferBuilder<'a, T> {
    buffer_extender: T,
    np_as_array_fn: &'a PyAny,
    from_numpy_dtype_fn: &'a PyAny,
    py_obj_type_classifier: PyObjTypeClassifier<'a>,
}

impl<'a, T> TensorBufferBuilder<'a, T> {
    fn new(buffer_extender: T, py: Python<'a>) -> DaftResult<Self> {
        Ok(Self {
            buffer_extender,
            np_as_array_fn: py.import("numpy")?.getattr(pyo3::intern!(py, "asarray"))?,
            from_numpy_dtype_fn: py
                .import(pyo3::intern!(py, "daft.datatype"))?
                .getattr(pyo3::intern!(py, "DataType"))?
                .getattr(pyo3::intern!(py, "from_numpy_dtype"))?,
            py_obj_type_classifier: PyObjTypeClassifier::new(py)?,
        })
    }
}

impl<'a, T> ExtendWithPyObj for TensorBufferBuilder<'a, T>
where
    T: ExtendWithNdarray + ExtendWithIter,
{
    fn extend(&mut self, py_obj: &PyAny) -> DaftResult<()> {
        match self.py_obj_type_classifier.classify(py_obj)? {
            PyObjType::Ndarray | PyObjType::Image => self.add_py_array(py_obj),
            PyObjType::Iterator => self.add_py_iter(py_obj),
            _ => unimplemented!("not implemented"),
        }
    }
}

impl<'a, T> PushNull for TensorBufferBuilder<'a, T>
where
    T: PushNull,
{
    fn push_null(&mut self) {
        self.buffer_extender.push_null();
    }
}

impl<'a, T> Finalizable for TensorBufferBuilder<'a, T>
where
    T: Finalizable,
{
    type Output = T::Output;

    fn finalize(self) -> Self::Output {
        self.buffer_extender.finalize()
    }
}

impl<'a, T> AddPyArray for TensorBufferBuilder<'a, T>
where
    T: ExtendWithNdarray,
{
    fn add_py_array(&mut self, py_array: &PyAny) -> DaftResult<()> {
        let py_array = self.np_as_array_fn.call1((py_array,))?;
        let np_dtype = py_array.getattr(pyo3::intern!(py_array.py(), "dtype"))?;

        let datatype = self
            .from_numpy_dtype_fn
            .call1((np_dtype,))?
            .getattr(pyo3::intern!(py_array.py(), "_dtype"))?
            .extract::<PyDataType>()?;
        let datatype = datatype.dtype;
        if !datatype.is_numeric() {
            return Err(DaftError::ValueError(format!(
                "Numpy array has unsupported type {}",
                datatype
            )));
        }
        with_match_numeric_daft_types!(datatype, |$N| {
            type Src = <$N as DaftNumericType>::Native;
            let py_array: PyReadonlyArrayDyn<'_, Src> = py_array.extract()?;
            // All of the conditions given in the ndarray::ArrayView::from_shape_ptr() docstring must be met in order to do
            // the pyarray.as_array conversion: https://docs.rs/ndarray/0.15.6/ndarray/type.ArrayView.html#method.from_shape_ptr
            // Relevant:
            //  1. Must have non-negative strides.

            // TODO(Clark): Double-check that we're covering all bases here for this unsafe handoff.
            if py_array.strides().iter().any(|s| *s < 0) {
                return Err(DaftError::ValueError(format!(
                    "we only support numpy arrays with non-negative strides, but got {:?}",
                    py_array.strides()
                )));
            }

            self.buffer_extender.extend_with_ndarray(py_array.as_array())
        })
    }
}

impl<'a, T> AddPyIter for TensorBufferBuilder<'a, T>
where
    T: ExtendWithIter,
{
    fn add_py_iter(&mut self, py_iter: &PyAny) -> DaftResult<()> {
        self.buffer_extender.extend_with_iter(py_iter.iter()?)
    }
}

struct ImageBufferBuilder<'a, T> {
    tensor_buffer_builder: TensorBufferBuilder<'a, T>,
    modes: Vec<ImageMode>,
    from_numpy_dtype_fn: &'a PyAny,
    py_obj_type_classifier: PyObjTypeClassifier<'a>,
}

impl<'a, T> ImageBufferBuilder<'a, T> {
    fn new(
        tensor_buffer_builder: TensorBufferBuilder<'a, T>,
        num_tensors: usize,
        py: Python<'a>,
    ) -> DaftResult<Self> {
        Ok(Self {
            tensor_buffer_builder,
            modes: Vec::with_capacity(num_tensors),
            from_numpy_dtype_fn: py
                .import(pyo3::intern!(py, "daft.datatype"))?
                .getattr(pyo3::intern!(py, "DataType"))?
                .getattr(pyo3::intern!(py, "from_numpy_dtype"))?,
            py_obj_type_classifier: PyObjTypeClassifier::new(py)?,
        })
    }
}

impl<'a, T> ExtendWithPyObj for ImageBufferBuilder<'a, T>
where
    T: ExtendWithNdarray,
{
    fn extend(&mut self, py_obj: &PyAny) -> DaftResult<()> {
        match self.py_obj_type_classifier.classify(py_obj)? {
            PyObjType::Image => self.add_image(py_obj),
            PyObjType::Ndarray => self.add_py_array(py_obj),
            _ => unimplemented!("not implemented"),
        }
    }
}

impl<'a, T> PushNull for ImageBufferBuilder<'a, T>
where
    T: PushNull,
{
    fn push_null(&mut self) {
        self.tensor_buffer_builder.push_null();
    }
}

struct ImageBuffers<T> {
    tensor_buffers: T,
    image_modes: Vec<ImageMode>,
}

impl<'a, T> Finalizable for ImageBufferBuilder<'a, T>
where
    T: Finalizable,
{
    type Output = ImageBuffers<T::Output>;

    fn finalize(self) -> Self::Output {
        Self::Output {
            tensor_buffers: self.tensor_buffer_builder.finalize(),
            image_modes: self.modes,
        }
    }
}

impl<'a, T: ExtendWithNdarray> AddImage for ImageBufferBuilder<'a, T> {
    fn add_image(&mut self, py_img: &PyAny) -> DaftResult<()> {
        let pil_mode = py_img
            .getattr(pyo3::intern!(py_img.py(), "mode"))?
            .extract::<&str>()?;
        let image_mode = ImageMode::from_pil_mode_str(pil_mode)?;
        self.modes.push(image_mode);
        self.tensor_buffer_builder.add_py_array(py_img)
    }
}

impl<'a, T: ExtendWithNdarray> AddPyArray for ImageBufferBuilder<'a, T> {
    fn add_py_array(&mut self, py_array: &PyAny) -> DaftResult<()> {
        let py = py_array.py();
        let shape = py_array
            .getattr(pyo3::intern!(py, "shape"))?
            .extract::<Vec<u64>>()?;
        if shape.len() != 3 && shape.len() != 2 {
            return Err(DaftError::ValueError(format!(
                "Image shapes must be 2D or 3D, but got: {:?}",
                shape
            )));
        }
        let num_channels = if shape.len() == 2 {
            Ok(1u16)
        } else {
            u16::try_from(shape[2])
        }
        .map_err(|e| {
            DaftError::ValueError(format!(
                "Failed to interpret tensor channel dimension size as u16: {}",
                e.to_string()
            ))
        })?;
        let np_dtype = py_array.getattr(pyo3::intern!(py, "dtype"))?;
        let datatype = self
            .from_numpy_dtype_fn
            .call1((np_dtype,))?
            .getattr(pyo3::intern!(py, "_dtype"))?
            .extract::<PyDataType>()?;
        let dtype = datatype.dtype;
        let image_mode = ImageMode::try_from_num_channels(num_channels, &dtype)?;
        self.modes.push(image_mode);
        self.tensor_buffer_builder.add_py_array(py_array)
    }
}

struct PrimitiveTensorBufferExtender<T> {
    data: Vec<T>,
    offsets: Vec<i64>,
    shapes: Vec<u64>,
    shape_offsets: Vec<i64>,
}

impl<T> PrimitiveTensorBufferExtender<T> {
    fn new(num_tensors: usize) -> Self {
        Self {
            data: Vec::with_capacity(num_tensors),
            offsets: Vec::with_capacity(num_tensors + 1),
            shapes: Vec::with_capacity(num_tensors),
            shape_offsets: Vec::with_capacity(num_tensors + 1),
        }
    }
}

fn ndarray_to_slice<'a, T>(ndarray: ArrayView<'a, T, IxDyn>) -> &[T]
where
    T: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
{
    // Create 1D slice from potentially non-contiguous and non-C-order arrays.
    // This will only create a copy if the ndarray is non-contiguous.
    ndarray.to_slice_memory_order().unwrap()
    // let owned_arr;
    // // Create 1D slice from potentially non-contiguous and non-C-order arrays.
    // // This will only create a copy if the ndarray is non-contiguous.
    // match ndarray.as_slice_memory_order() {
    //     Some(sl) => sl,
    //     None => {
    //         owned_arr = ndarray.to_owned();
    //         owned_arr.as_slice_memory_order().unwrap()
    //     }
    // }
}

impl<T> ExtendWithNdarray for PrimitiveTensorBufferExtender<T>
where
    T: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
{
    fn extend_with_ndarray<'a, U>(&mut self, ndarray: ArrayView<'a, U, IxDyn>) -> DaftResult<()>
    where
        U: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
    {
        let shape = ndarray.shape();
        self.shapes.extend(shape.iter().map(|v| *v as u64));
        self.shape_offsets
            .push(self.shape_offsets.last().unwrap() + shape.len() as i64);
        let data = ndarray_to_slice(ndarray);
        self.data.extend(
            data.iter()
                .map(|v| <Wrapping<T> as NumCast>::from(*v).unwrap().0),
        );
        self.offsets
            .push(self.offsets.last().unwrap() + data.len() as i64);
        Ok(())
    }
}

impl<T> ExtendWithIter for PrimitiveTensorBufferExtender<T>
where
    T: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
    T: for<'p> FromPyObject<'p>,
{
    fn extend_with_iter(&mut self, py_iter: &PyIterator) -> DaftResult<()> {
        let collected = py_iter.extract::<Vec<T>>()?;
        self.data.extend(collected.iter());
        self.offsets
            .push(self.offsets.last().unwrap() + collected.len() as i64);
        self.shapes.push(collected.len() as u64);
        self.shape_offsets
            .push(self.shape_offsets.last().unwrap() + 1i64);
        Ok(())
    }
}

impl<T> PushNull for PrimitiveTensorBufferExtender<T>
where
    T: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
{
    fn push_null(&mut self) {
        self.offsets.push(*self.offsets.last().unwrap());
        self.shape_offsets.push(*self.shape_offsets.last().unwrap());
    }
}

impl<T> Finalizable for PrimitiveTensorBufferExtender<T>
where
    T: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
{
    type Output = Self;

    fn finalize(self) -> Self::Output {
        self
    }
}

struct FixedShapePrimitiveTensorBufferExtender<T> {
    data: Vec<T>,
    shape: Vec<u64>,
}

impl<T> FixedShapePrimitiveTensorBufferExtender<T> {
    fn new(num_tensors: usize, shape: Vec<u64>) -> Self {
        Self {
            data: Vec::with_capacity(num_tensors),
            shape,
        }
    }
}

impl<T> ExtendWithNdarray for FixedShapePrimitiveTensorBufferExtender<T>
where
    T: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
{
    fn extend_with_ndarray<'a, U>(&mut self, ndarray: ArrayView<'a, U, IxDyn>) -> DaftResult<()>
    where
        U: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
    {
        if !ndarray
            .shape()
            .iter()
            .map(|v| *v as u64)
            .eq(self.shape.iter().map(|v| *v))
        {
            return Err(DaftError::ValueError(format!(
                "Expected fixed-shape tensor of shape {:?}, got {:?}",
                self.shape,
                ndarray.shape()
            )));
        }
        let data = ndarray_to_slice(ndarray);
        self.data.extend(
            data.iter()
                .map(|v| <Wrapping<T> as NumCast>::from(*v).unwrap().0),
        );
        Ok(())
    }
}

impl<T> ExtendWithIter for FixedShapePrimitiveTensorBufferExtender<T>
where
    T: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
    T: for<'p> FromPyObject<'p>,
{
    fn extend_with_iter(&mut self, py_iter: &PyIterator) -> DaftResult<()> {
        let collected = py_iter.extract::<Vec<T>>()?;
        if self.shape.len() != 1 || self.shape[0] != collected.len() as u64 {
            return Err(DaftError::ValueError(format!(
                "Expected fixed-shape tensor of shape {:?}, got {:?}",
                self.shape,
                vec![collected.len()]
            )));
        }
        self.data.extend(collected.iter());
        Ok(())
    }
}

impl<T> PushNull for FixedShapePrimitiveTensorBufferExtender<T>
where
    T: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
{
    fn push_null(&mut self) {
        self.data
            .extend(iter::repeat(T::default()).take(self.shape.iter().product::<u64>() as usize));
    }
}

impl<T> Finalizable for FixedShapePrimitiveTensorBufferExtender<T>
where
    T: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
{
    type Output = Self;

    fn finalize(self) -> Self::Output {
        self
    }
}

struct BinaryTensorBufferExtender {
    data: Vec<u8>,
    offsets: Vec<i64>,
    shapes: Vec<u64>,
    shape_offsets: Vec<i64>,
    dtypes: Vec<DataType>,
}

impl BinaryTensorBufferExtender {
    fn new(num_tensors: usize) -> Self {
        Self {
            data: Vec::with_capacity(num_tensors),
            offsets: Vec::with_capacity(num_tensors + 1),
            shapes: Vec::with_capacity(num_tensors),
            shape_offsets: Vec::with_capacity(num_tensors + 1),
            dtypes: Vec::with_capacity(num_tensors),
        }
    }
}

impl ExtendWithNdarray for BinaryTensorBufferExtender {
    fn extend_with_ndarray<'a, U>(&mut self, ndarray: ArrayView<'a, U, IxDyn>) -> DaftResult<()>
    where
        U: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
    {
        let shape = ndarray.shape();
        self.shapes.extend(shape.iter().map(|v| *v as u64));
        self.shape_offsets
            .push(self.shape_offsets.last().unwrap() + shape.len() as i64);
        let data = ndarray_to_slice(ndarray);
        for v in data.iter() {
            self.data.extend_from_slice(v.to_le_bytes().as_ref());
        }
        self.offsets
            .push(self.offsets.last().unwrap() + data.len() as i64);
        self.dtypes
            .push((&arrow2::datatypes::DataType::from(U::PRIMITIVE)).into());
        Ok(())
    }
}

impl PushNull for BinaryTensorBufferExtender {
    fn push_null(&mut self) {
        self.offsets.push(*self.offsets.last().unwrap());
        self.shape_offsets.push(*self.shape_offsets.last().unwrap());
    }
}

impl Finalizable for BinaryTensorBufferExtender {
    type Output = Self;

    fn finalize(self) -> Self::Output {
        self
    }
}

#[cfg(feature = "python")]
fn extract_python_to_buffers<T>(
    mut builder: T,
    py: Python<'_>,
    python_objects: &PythonArray,
) -> DaftResult<T::Output>
where
    T: ExtendWithPyObj + PushNull + Finalizable,
{
    for object in python_objects.as_arrow().iter() {
        if let Some(object) = object {
            let object = object.into_py(py);
            builder.extend(object.as_ref(py))?;
        } else {
            builder.push_null();
        }
    }
    Ok(builder.finalize())
}

#[cfg(feature = "python")]
fn extract_python_like_to_fixed_size_list<Tgt>(
    py: Python<'_>,
    python_objects: &PythonArray,
    child_field: &Field,
    list_size: usize,
) -> DaftResult<FixedSizeListArray>
where
    Tgt: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
    Tgt: for<'p> FromPyObject<'p>,
{
    let n_rows = python_objects.len();
    let builder = TensorBufferBuilder::new(
        FixedShapePrimitiveTensorBufferExtender::<Tgt>::new(n_rows, vec![list_size as u64]),
        py,
    )?;
    let buffer = extract_python_to_buffers(builder, py, python_objects)?;
    let values_vec = buffer.data;

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
fn extract_python_like_to_list<Tgt>(
    py: Python<'_>,
    python_objects: &PythonArray,
    child_field: &Field,
) -> DaftResult<ListArray>
where
    Tgt: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
    Tgt: for<'p> FromPyObject<'p>,
{
    let n_rows = python_objects.len();
    let builder = TensorBufferBuilder::new(PrimitiveTensorBufferExtender::<Tgt>::new(n_rows), py)?;
    let buffers = extract_python_to_buffers(builder, py, python_objects)?;
    let values_vec = buffers.data;
    let offsets = buffers.offsets;

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
fn extract_python_like_to_image_array<Tgt>(
    py: Python<'_>,
    python_objects: &PythonArray,
    dtype: &DataType,
    child_dtype: &DataType,
    mode_from_dtype: Option<ImageMode>,
) -> DaftResult<ImageArray>
where
    Tgt: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
    Tgt: for<'p> FromPyObject<'p>,
{
    // 3 dimensions - height x width x channel.

    let shape_size = 3;
    let num_rows = python_objects.len();
    let builder = ImageBufferBuilder::new(
        TensorBufferBuilder::new(BinaryTensorBufferExtender::new(num_rows), py)?,
        num_rows,
        py,
    )?;
    let buffers = extract_python_to_buffers(builder, py, python_objects)?;
    let values_vec = buffers.tensor_buffers.data;
    let offsets = buffers.tensor_buffers.offsets;
    let shapes = buffers.tensor_buffers.shapes;
    let shape_offsets = buffers.tensor_buffers.shape_offsets;
    let image_modes = buffers.image_modes;

    let validity = python_objects.as_arrow().validity();

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
        assert!(shape_end > shape_start);
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

        if let Some(mode_from_dtype) = mode_from_dtype && mode_from_dtype != image_modes[i] {
            return Err(DaftError::ValueError(format!(
                "Expected type-level image mode {}, but got {} for index {}",
                mode_from_dtype, image_modes[i], i
            )));
        }
        modes.push(mode_from_dtype.unwrap_or(image_modes[i]) as u8);
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
fn extract_python_like_to_tensor_array<Tgt>(
    py: Python<'_>,
    python_objects: &PythonArray,
    dtype: &DataType,
    child_dtype: &DataType,
) -> DaftResult<TensorArray>
where
    Tgt: numpy::Element + NumCast + ToPrimitive + arrow2::types::NativeType,
    Tgt: for<'p> FromPyObject<'p>,
{
    let n_rows = python_objects.len();
    let builder = TensorBufferBuilder::new(PrimitiveTensorBufferExtender::<Tgt>::new(n_rows), py)?;
    let buffers = extract_python_to_buffers(builder, py, python_objects)?;
    let data = buffers.data;
    let offsets = buffers.offsets;
    let shapes = buffers.shapes;
    let shape_offsets = buffers.shape_offsets;

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
