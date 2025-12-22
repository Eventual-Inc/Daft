use common_error::{DaftError, DaftResult};
use daft_core::series::Series;
use daft_schema::schema::Schema;
#[cfg(feature = "python")]
use pyo3::Python;
#[cfg(feature = "python")]
use pyo3::types::PyAnyMethods;
use serde::{Deserialize, Serialize};

use crate::functions::{
    ExprRef, FunctionArgs, ScalarUDF, function_args::FunctionArgs as FunctionArgsDerive,
};

#[derive(FunctionArgsDerive)]
struct KVGetArgs<T> {
    keys: T,
    store_name: T,
    #[arg(optional)]
    on_error: Option<T>,
    #[arg(optional)]
    columns: Option<T>,
}

#[derive(FunctionArgsDerive)]
struct KVBatchGetArgs<T> {
    keys: T,
    store_name: T,
    batch_size: T,
    #[arg(optional)]
    on_error: Option<T>,
    #[arg(optional)]
    columns: Option<T>,
}

#[derive(FunctionArgsDerive)]
struct KVExistsArgs<T> {
    keys: T,
    store_name: T,
}

#[derive(FunctionArgsDerive)]
struct KVPutArgs<T> {
    key: T,
    value: T,
    store_name: T,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct KVGetWithStoreName;

#[typetag::serde]
impl ScalarUDF for KVGetWithStoreName {
    fn name(&self) -> &'static str {
        "kv_get_with_name"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        #[cfg_attr(not(feature = "python"), allow(unused_variables))]
        let KVGetArgs {
            keys,
            store_name: store_name_series,
            on_error: on_error_series_opt,
            columns: columns_series_opt,
        } = args.try_into()?;

        if store_name_series.len() != 1 {
            return Err(DaftError::ValueError(
                "Store name series must contain exactly one element".to_string(),
            ));
        }

        #[cfg(feature = "python")]
        {
            use daft_core::prelude::*;

            let name = store_name_series
                .utf8()?
                .get(0)
                .ok_or_else(|| DaftError::ValueError("Missing store name".to_string()))?;

            let on_error = if let Some(on_error_series) = on_error_series_opt.as_ref() {
                if on_error_series.len() != 1 {
                    return Err(DaftError::ValueError(
                        "on_error series must contain exactly one element".to_string(),
                    ));
                }
                on_error_series
                    .utf8()?
                    .get(0)
                    .ok_or_else(|| DaftError::ValueError("Missing on_error".to_string()))?
                    .to_string()
            } else {
                "raise".to_string()
            };

            // Parse columns if provided
            let requested: Option<Vec<String>> = if let Some(cols_ser) = columns_series_opt.as_ref()
            {
                match cols_ser.data_type() {
                    DataType::List(_) => {
                        let lit = cols_ser.get_lit(0);
                        match lit {
                            Literal::List(inner) => {
                                let inner_utf8 = inner.utf8()?;
                                let mut cols = Vec::with_capacity(inner_utf8.len());
                                for i in 0..inner_utf8.len() {
                                    cols.push(
                                        inner_utf8
                                            .get(i)
                                            .ok_or_else(|| {
                                                DaftError::ValueError(
                                                    "Null column name".to_string(),
                                                )
                                            })?
                                            .to_string(),
                                    );
                                }
                                Some(cols)
                            }
                            _ => {
                                return Err(DaftError::ValueError(
                                    "Expected List literal for columns".to_string(),
                                ));
                            }
                        }
                    }
                    DataType::Binary => {
                        let arr = cols_ser.binary()?;
                        let bytes = arr.get(0).ok_or_else(|| {
                            DaftError::ValueError(
                                "columns must contain exactly one element".to_string(),
                            )
                        })?;
                        let cols: Vec<String> = serde_json::from_slice(bytes).map_err(|e| {
                            DaftError::ValueError(format!("Failed to deserialize columns: {}", e))
                        })?;
                        Some(cols)
                    }
                    _ => {
                        return Err(DaftError::TypeError(format!(
                            "Invalid type for columns: {}",
                            cols_ser.data_type()
                        )));
                    }
                }
            } else {
                None
            };

            let k_py = keys.cast(&DataType::Python)?;
            let _k_arr = k_py.downcast::<PythonArray>()?;
            let s = Python::attach(|py| -> DaftResult<Series> {
                let daft_mod = py.import("daft.daft")?;
                let py_keys = pyo3::Py::new(
                    py,
                    daft_core::python::PySeries {
                        series: keys.clone(),
                    },
                )?;
                let func = daft_mod.getattr("kv_get_direct_series")?;
                let py_res = match requested.clone() {
                    Some(cols) => func.call1((name, py_keys, on_error.as_str(), cols))?,
                    None => func.call1((name, py_keys, on_error.as_str(), None::<Vec<String>>))?,
                };
                let py_series: pyo3::PyRef<daft_core::python::PySeries> = py_res.extract()?;
                let mut s = py_series.series.clone();
                s = s.rename(keys.name());
                Ok(s)
            })?;
            Ok(s)
        }

        #[cfg(not(feature = "python"))]
        {
            panic!("KVGetWithStoreName is only supported with the 'python' feature enabled");
        }
    }

    fn get_return_field(
        &self,
        args: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<daft_core::datatypes::Field> {
        #[cfg_attr(not(feature = "python"), allow(unused_variables))]
        let KVGetArgs {
            keys,
            store_name: _store_name,
            on_error: _,
            columns: _columns,
        } = args.try_into()?;

        #[cfg_attr(not(feature = "python"), allow(unused_variables))]
        let keys = keys.to_field(schema)?;
        #[cfg(feature = "python")]
        {
            Ok(daft_core::datatypes::Field::new(
                keys.name,
                daft_core::datatypes::DataType::Python,
            ))
        }

        #[cfg(not(feature = "python"))]
        {
            panic!("KVGetWithStoreName is only supported with the 'python' feature enabled");
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct KVBatchGetWithStoreName;

#[typetag::serde]
impl ScalarUDF for KVBatchGetWithStoreName {
    fn name(&self) -> &'static str {
        "kv_batch_get_with_name"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        #[cfg_attr(not(feature = "python"), allow(unused_variables))]
        let KVBatchGetArgs {
            keys,
            store_name: store_name_series,
            batch_size: batch_size_series,
            on_error: on_error_series_opt,
            columns: columns_series_opt,
        } = args.try_into()?;

        if store_name_series.len() != 1 {
            return Err(DaftError::ValueError(
                "Store name series must contain exactly one element".to_string(),
            ));
        }
        if batch_size_series.len() != 1 {
            return Err(DaftError::ValueError(
                "Batch size series must contain exactly one element".to_string(),
            ));
        }

        #[cfg(feature = "python")]
        {
            use daft_core::prelude::*;

            let name = store_name_series
                .utf8()?
                .get(0)
                .ok_or_else(|| DaftError::ValueError("Missing store name".to_string()))?;

            let batch_size = batch_size_series
                .i64()?
                .get(0)
                .ok_or_else(|| DaftError::ValueError("Missing batch size".to_string()))?;
            if batch_size <= 0 {
                return Err(DaftError::ValueError(
                    "batch_size must be a positive integer".to_string(),
                ));
            }
            let batch_size_usize = batch_size as usize;

            let on_error = if let Some(on_error_series) = on_error_series_opt.as_ref() {
                if on_error_series.len() != 1 {
                    return Err(DaftError::ValueError(
                        "on_error series must contain exactly one element".to_string(),
                    ));
                }
                on_error_series
                    .utf8()?
                    .get(0)
                    .ok_or_else(|| DaftError::ValueError("Missing on_error".to_string()))?
                    .to_string()
            } else {
                "raise".to_string()
            };

            let requested: Option<Vec<String>> = if let Some(cols_ser) = columns_series_opt.as_ref()
            {
                match cols_ser.data_type() {
                    DataType::List(_) => {
                        let lit = cols_ser.get_lit(0);
                        match lit {
                            Literal::List(inner) => {
                                let inner_utf8 = inner.utf8()?;
                                let mut cols = Vec::with_capacity(inner_utf8.len());
                                for i in 0..inner_utf8.len() {
                                    cols.push(
                                        inner_utf8
                                            .get(i)
                                            .ok_or_else(|| {
                                                DaftError::ValueError(
                                                    "Null column name".to_string(),
                                                )
                                            })?
                                            .to_string(),
                                    );
                                }
                                Some(cols)
                            }
                            _ => {
                                return Err(DaftError::ValueError(
                                    "Expected List literal for columns".to_string(),
                                ));
                            }
                        }
                    }
                    DataType::Binary => {
                        let arr = cols_ser.binary()?;
                        let bytes = arr.get(0).ok_or_else(|| {
                            DaftError::ValueError(
                                "columns must contain exactly one element".to_string(),
                            )
                        })?;
                        let cols: Vec<String> = serde_json::from_slice(bytes).map_err(|e| {
                            DaftError::ValueError(format!("Failed to deserialize columns: {}", e))
                        })?;
                        Some(cols)
                    }
                    _ => {
                        return Err(DaftError::TypeError(format!(
                            "Invalid type for columns: {}",
                            cols_ser.data_type()
                        )));
                    }
                }
            } else {
                None
            };

            let k_py = keys.cast(&DataType::Python)?;
            let _k_arr = k_py.downcast::<PythonArray>()?;

            let s = Python::attach(|py| -> DaftResult<Series> {
                let daft_mod = py.import("daft.daft")?;
                let py_keys = pyo3::Py::new(
                    py,
                    daft_core::python::PySeries {
                        series: keys.clone(),
                    },
                )?;
                let func = daft_mod.getattr("kv_batch_get_direct_series")?;
                let py_res = match requested.clone() {
                    Some(cols) => {
                        func.call1((name, py_keys, batch_size_usize, on_error.as_str(), cols))?
                    }
                    None => func.call1((
                        name,
                        py_keys,
                        batch_size_usize,
                        on_error.as_str(),
                        None::<Vec<String>>,
                    ))?,
                };
                let py_series: pyo3::PyRef<daft_core::python::PySeries> = py_res.extract()?;
                Ok(py_series.series.clone().rename(keys.name()))
            })?;
            Ok(s)
        }

        #[cfg(not(feature = "python"))]
        {
            panic!("KVBatchGetWithStoreName is only supported with the 'python' feature enabled");
        }
    }

    fn get_return_field(
        &self,
        args: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<daft_core::datatypes::Field> {
        let KVBatchGetArgs {
            keys,
            store_name: _store_name,
            batch_size: _batch_size,
            on_error: _,
            columns: _columns,
        } = args.try_into()?;

        #[cfg_attr(not(feature = "python"), allow(unused_variables))]
        let keys = keys.to_field(schema)?;
        #[cfg(feature = "python")]
        {
            Ok(daft_core::datatypes::Field::new(
                keys.name,
                daft_core::datatypes::DataType::Python,
            ))
        }

        #[cfg(not(feature = "python"))]
        {
            panic!("KVBatchGetWithStoreName is only supported with the 'python' feature enabled");
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct KVExistsWithStoreName;

#[typetag::serde]
impl ScalarUDF for KVExistsWithStoreName {
    fn name(&self) -> &'static str {
        "kv_exists_with_name"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        #[cfg_attr(not(feature = "python"), allow(unused_variables))]
        let KVExistsArgs {
            keys,
            store_name: store_name_series,
        } = args.try_into()?;

        if store_name_series.len() != 1 {
            return Err(DaftError::ValueError(
                "Store name series must contain exactly one element".to_string(),
            ));
        }

        #[cfg(feature = "python")]
        {
            use daft_core::prelude::*;

            let name = store_name_series
                .utf8()?
                .get(0)
                .ok_or_else(|| DaftError::ValueError("Missing store name".to_string()))?;

            let k_py = keys.cast(&DataType::Python)?;
            let _k_arr = k_py.downcast::<PythonArray>()?;
            let s = Python::attach(|py| -> DaftResult<Series> {
                let daft_mod = py.import("daft.daft")?;
                let py_keys = pyo3::Py::new(
                    py,
                    daft_core::python::PySeries {
                        series: keys.clone(),
                    },
                )?;
                let func = daft_mod.getattr("kv_exists_direct_series")?;
                let py_res = func.call1((name, py_keys))?;
                let py_series: pyo3::PyRef<daft_core::python::PySeries> = py_res.extract()?;
                Ok(py_series.series.clone().rename(keys.name()))
            })?;
            Ok(s)
        }

        #[cfg(not(feature = "python"))]
        {
            panic!("KVExistsWithStoreName is only supported with the 'python' feature enabled");
        }
    }

    fn get_return_field(
        &self,
        args: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<daft_core::datatypes::Field> {
        let KVExistsArgs {
            keys,
            store_name: _store_name,
        } = args.try_into()?;

        #[cfg_attr(not(feature = "python"), allow(unused_variables))]
        let keys = keys.to_field(schema)?;

        #[cfg(feature = "python")]
        {
            Ok(daft_core::datatypes::Field::new(
                keys.name,
                daft_core::datatypes::DataType::Boolean,
            ))
        }

        #[cfg(not(feature = "python"))]
        {
            panic!("KVExistsWithStoreName is only supported with the 'python' feature enabled");
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct KVPutWithStoreName;

#[typetag::serde]
impl ScalarUDF for KVPutWithStoreName {
    fn name(&self) -> &'static str {
        "kv_put_with_name"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        #[cfg_attr(not(feature = "python"), allow(unused_variables))]
        let KVPutArgs {
            key,
            value,
            store_name: store_name_series,
        } = args.try_into()?;

        if store_name_series.len() != 1 {
            return Err(DaftError::ValueError(
                "Store name series must contain exactly one element".to_string(),
            ));
        }
        #[cfg(feature = "python")]
        {
            use daft_core::prelude::*;

            let name = store_name_series
                .utf8()?
                .get(0)
                .ok_or_else(|| DaftError::ValueError("Missing store name".to_string()))?;

            // Build PySeries for key and value
            let s = Python::attach(|py| -> DaftResult<Series> {
                let daft_mod = py.import("daft.daft")?;
                let py_key = pyo3::Py::new(
                    py,
                    daft_core::python::PySeries {
                        series: key.clone(),
                    },
                )?;
                let py_value = pyo3::Py::new(
                    py,
                    daft_core::python::PySeries {
                        series: value.clone(),
                    },
                )?;
                let func = daft_mod.getattr("kv_put_direct_series")?;
                let py_res = func.call1((name, py_key, py_value))?;
                let py_series: pyo3::PyRef<daft_core::python::PySeries> = py_res.extract()?;
                let mut s = py_series.series.clone();
                s = s.rename("result");
                Ok(s)
            })?;
            Ok(s)
        }

        #[cfg(not(feature = "python"))]
        {
            panic!("KVPutWithStoreName is only supported with the 'python' feature enabled");
        }
    }

    fn get_return_field(
        &self,
        _args: FunctionArgs<ExprRef>,
        _schema: &Schema,
    ) -> DaftResult<daft_core::datatypes::Field> {
        let fields = vec![
            daft_core::datatypes::Field::new("ok", daft_core::datatypes::DataType::Boolean),
            daft_core::datatypes::Field::new("key", daft_core::datatypes::DataType::Utf8),
        ];
        Ok(daft_core::datatypes::Field::new(
            "result",
            daft_core::datatypes::DataType::Struct(fields),
        ))
    }
}

pub fn kv_get_with_name(
    keys: ExprRef,
    name: ExprRef,
    on_error: ExprRef,
    columns: Option<ExprRef>,
) -> ExprRef {
    use crate::functions::scalar::ScalarFn;
    let mut args = vec![keys, name, on_error];
    if let Some(c) = columns {
        args.push(c);
    }
    ScalarFn::builtin(KVGetWithStoreName, args).into()
}

pub fn kv_batch_get_with_name(
    keys: ExprRef,
    name: ExprRef,
    batch_size: ExprRef,
    on_error: ExprRef,
    columns: Option<ExprRef>,
) -> ExprRef {
    use crate::functions::scalar::ScalarFn;
    let mut args = vec![keys, name, batch_size, on_error];
    if let Some(c) = columns {
        args.push(c);
    }
    ScalarFn::builtin(KVBatchGetWithStoreName, args).into()
}

pub fn kv_exists_with_name(keys: ExprRef, name: ExprRef) -> ExprRef {
    use crate::functions::scalar::ScalarFn;
    ScalarFn::builtin(KVExistsWithStoreName, vec![keys, name]).into()
}

pub fn kv_put_with_name(key: ExprRef, value: ExprRef, name: ExprRef) -> ExprRef {
    use crate::functions::scalar::ScalarFn;
    ScalarFn::builtin(KVPutWithStoreName, vec![key, value, name]).into()
}
