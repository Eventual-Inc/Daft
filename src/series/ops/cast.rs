use crate::{datatypes::DataType, error::DaftResult, series::Series};

#[macro_export]
macro_rules! apply_method_all_arrow_series {
    ($self:expr, $method:ident, $($args:expr),*) => {
        match $self.data_type() {
            DataType::Null => $self.null().unwrap().$method($($args),*),
            DataType::Boolean => $self.bool().unwrap().$method($($args),*),
            DataType::Binary => $self.binary().unwrap().$method($($args),*),
            DataType::Utf8 => $self.utf8().unwrap().$method($($args),*),
            DataType::UInt8 => $self.u8().unwrap().$method($($args),*),
            DataType::UInt16 => $self.u16().unwrap().$method($($args),*),
            DataType::UInt32 => $self.u32().unwrap().$method($($args),*),
            DataType::UInt64 => $self.u64().unwrap().$method($($args),*),
            DataType::Int8 => $self.i8().unwrap().$method($($args),*),
            DataType::Int16 => $self.i16().unwrap().$method($($args),*),
            DataType::Int32 => $self.i32().unwrap().$method($($args),*),
            DataType::Int64 => $self.i64().unwrap().$method($($args),*),
            // DataType::Float16 => $self.f16().unwrap().$method($($args),*),
            DataType::Float32 => $self.f32().unwrap().$method($($args),*),
            DataType::Float64 => $self.f64().unwrap().$method($($args),*),
            DataType::Date => $self.date().unwrap().$method($($args),*),
            DataType::List(_) => $self.list().unwrap().$method($($args),*),
            DataType::FixedSizeList(..) => $self.fixed_size_list().unwrap().$method($($args),*),
            DataType::Struct(_) => $self.struct_().unwrap().$method($($args),*),
            // TODO: Add implementations for these types
            // DataType::Timestamp(_, _) => $self.timestamp().unwrap().$method($($args),*),
            dt => panic!("dtype {:?} not supported", dt)
        }
    }
}

#[cfg(feature = "python")]
macro_rules! pycast_then_arrowcast {
    ($self:expr, $daft_type:expr, $pytype_str:expr) => {
        {
            let old_pyseries = PySeries::from($self.clone());

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

            let new_series: Self = new_pyseries.into();

            if new_series.data_type() == &DataType::Python {
                panic!("After casting, we expected an Arrow data type castable to {}, but got Python type again", $daft_type)
            }
            return new_series.cast(&$daft_type);
        }
    }
}

impl Series {
    pub fn cast(&self, datatype: &DataType) -> DaftResult<Series> {
        self.inner.cast(datatype)

        // if self.data_type() == datatype {
        //     return Ok(self.clone());
        // }
        // #[cfg(feature = "python")]
        // {
        //     use crate::python::PySeries;
        //     use pyo3::prelude::*;

        //     if datatype == &DataType::Python {
        //         // Convert something to Python.

        //         // Use the existing logic on the Python side of the PyO3 layer
        //         // to create a Python list out of this series.
        //         let old_pyseries = PySeries::from(self.clone());

        //         let new_pyseries: PySeries = Python::with_gil(|py| -> PyResult<PySeries> {
        //             PyModule::import(py, pyo3::intern!(py, "daft.series"))?
        //                 .getattr(pyo3::intern!(py, "Series"))?
        //                 .getattr(pyo3::intern!(py, "_from_pyseries"))?
        //                 .call1((old_pyseries,))?
        //                 .call_method0(pyo3::intern!(py, "_cast_to_python"))?
        //                 .getattr(pyo3::intern!(py, "_series"))?
        //                 .extract()
        //         })?;

        //         return Ok(new_pyseries.into());
        //     } else if self.data_type() == &DataType::Python {
        //         // Convert Python type to an Arrow type.
        //         // The semantics of this cast is:
        //         // we will call the appropriate Python cast function on the Python object,
        //         // then call the Arrow cast function.
        //         match datatype {
        //             DataType::Null => {
        //                 // (Follow Arrow cast behaviour: turn all elements into Null.)
        //                 let null_array = crate::datatypes::NullArray::full_null(
        //                     self.name(),
        //                     &DataType::Null,
        //                     self.len(),
        //                 );
        //                 return Ok(null_array.into_series());
        //             }
        //             DataType::Boolean => pycast_then_arrowcast!(self, DataType::Boolean, "bool"),
        //             DataType::Binary => pycast_then_arrowcast!(self, DataType::Binary, "bytes"),
        //             DataType::Utf8 => pycast_then_arrowcast!(self, DataType::Utf8, "str"),
        //             dt @ DataType::UInt8
        //             | dt @ DataType::UInt16
        //             | dt @ DataType::UInt32
        //             | dt @ DataType::UInt64
        //             | dt @ DataType::Int8
        //             | dt @ DataType::Int16
        //             | dt @ DataType::Int32
        //             | dt @ DataType::Int64 => pycast_then_arrowcast!(self, dt, "int"),
        //             // DataType::Float16 => todo!(),
        //             dt @ DataType::Float32 | dt @ DataType::Float64 => {
        //                 pycast_then_arrowcast!(self, dt, "float")
        //             }
        //             DataType::Date => unimplemented!(),
        //             DataType::List(_) => unimplemented!(),
        //             DataType::FixedSizeList(..) => unimplemented!(),
        //             DataType::Struct(_) => unimplemented!(),
        //             // TODO: Add implementations for these types
        //             // DataType::Timestamp(_, _) => $self.timestamp().unwrap().$method($($args),*),
        //             dt => unimplemented!("dtype {:?} not supported", dt),
        //         }
        //     }
        // }
        // apply_method_all_arrow_series!(self, cast, datatype)
    }
}
