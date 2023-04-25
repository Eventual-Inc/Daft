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

impl Series {
    pub fn cast(&self, datatype: &DataType) -> DaftResult<Series> {
        if self.data_type() == datatype {
            return Ok(self.clone());
        }
        #[cfg(feature = "python")]
        {
            use crate::python::PySeries;
            use pyo3::prelude::*;

            if datatype == &DataType::Python {
                // Convert something to Python.

                // Use the existing logic on the Python side of the PyO3 layer
                // to create a Python list out of this series.
                let old_pyseries = PySeries::from(self.clone());

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
            } else if self.data_type() == &DataType::Python {
                // Convert something from Python to Arrow.
                // Complex. Need to apply a Python-side cast to a relevant Python native type,
                // and then use our existing Python native -> Arrow import logic.
                match datatype {
                    DataType::Null => todo!(),
                    DataType::Boolean => {
                        let old_pyseries = PySeries::from(self.clone());

                        let new_pyseries = Python::with_gil(|py| -> PyResult<PySeries> {
                            let old_daft_series = {
                                PyModule::import(py, pyo3::intern!(py, "daft.series"))?
                                    .getattr(pyo3::intern!(py, "Series"))?
                                    .getattr(pyo3::intern!(py, "_from_pyseries"))?
                                    .call1((old_pyseries,))?
                            };

                            let py_type_fn = {
                                PyModule::import(py, pyo3::intern!(py, "builtins"))?
                                    .getattr(pyo3::intern!(py, "bool"))?
                            };

                            old_daft_series
                                .call_method1(
                                    pyo3::intern!(py, "_pycast_to_pynative"),
                                    (py_type_fn,),
                                )?
                                .getattr(pyo3::intern!(py, "_series"))?
                                .extract()
                        })?;

                        return Ok(new_pyseries.into());
                    }
                    DataType::Binary => todo!(),
                    DataType::Utf8 => todo!(),
                    DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64 => todo!(),
                    // DataType::Float16 => todo!(),
                    DataType::Float32 | DataType::Float64 => todo!(),
                    DataType::Date => todo!(),
                    DataType::List(_) => todo!(),
                    DataType::FixedSizeList(..) => todo!(),
                    DataType::Struct(_) => todo!(),
                    // TODO: Add implementations for these types
                    // DataType::Timestamp(_, _) => $self.timestamp().unwrap().$method($($args),*),
                    dt => unimplemented!("dtype {:?} not supported", dt),
                }
            }
        }
        apply_method_all_arrow_series!(self, cast, datatype)
    }
}
