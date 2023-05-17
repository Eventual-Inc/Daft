use arrow2::compute::{
    self,
    cast::{can_cast_types, cast, CastOptions},
};

use crate::datatypes::FixedSizeListArray;
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

use std::iter;

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
use crate::datatypes::DaftNumericType;

#[cfg(feature = "python")]
fn extract_numpy_array_to_fixed_size_list<T: DaftNumericType>(
    py: Python<'_>,
    python_objects: &PythonArray,
    list_size: usize,
) -> DaftResult<FixedSizeListArray>
where
    T::Native: numpy::Element,
{
    use arrow2::array::Array;
    use numpy::PyArray1;

    let mut values_vec = Vec::with_capacity(list_size * python_objects.len());

    for object in python_objects.as_arrow().iter() {
        if let Some(object) = object {
            let pyarray = object.call_method0(py, pyo3::intern!(py, "__array__"));
            if pyarray.is_err() {
                return Err(DaftError::ValueError(format!(
                    "An error occurred when extracting array out of type: {:?}",
                    object.getattr(py, pyo3::intern!(py, "__class__"))
                )));
            }

            let pyarray = pyarray?.call_method0(py, "__array__")?;
            let pyarray: PyReadonlyArrayDyn<'_, T::Native> = pyarray.extract(py)?;
            if pyarray.ndim() != 1 {
                return Err(DaftError::ValueError(format!(
                    "we only support 1 dim numpy arrays, got {}",
                    pyarray.ndim()
                )));
            }

            let pyarray = pyarray.downcast::<PyArray1<T::Native>>().unwrap();
            if pyarray.len() != list_size {
                return Err(DaftError::ValueError(format!(
                    "Expected Array-like Object to have {list_size} elements but got {}",
                    pyarray.len()
                )));
            }
            values_vec.extend_from_slice(unsafe { pyarray.as_slice() }.unwrap())
        } else {
            values_vec.extend(iter::repeat(T::Native::default()).take(list_size));
        }
    }

    let values_array: Box<dyn arrow2::array::Array> =
        Box::new(arrow2::array::PrimitiveArray::from_vec(values_vec));

    let inner_field =
        arrow2::datatypes::Field::new(python_objects.name(), T::get_dtype().to_arrow()?, true);

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
            DataType::List(_) => unimplemented!(),
            DataType::FixedSizeList(field, size) => {
                with_match_numeric_daft_types!(field.dtype, |$T| {
                    pyo3::Python::with_gil(|py| {
                        let result = extract_numpy_array_to_fixed_size_list::<$T>(py, self, *size)?;
                        Ok(result.into_series())
                    })
                })
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
