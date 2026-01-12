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
    store_name: T,
    keys: T,
    #[arg(optional)]
    on_error: Option<T>,
    #[arg(optional)]
    columns: Option<T>,
}

#[derive(FunctionArgsDerive)]
struct KVBatchGetArgs<T> {
    store_name: T,
    keys: T,
    batch_size: T,
    #[arg(optional)]
    on_error: Option<T>,
    #[arg(optional)]
    columns: Option<T>,
}

#[derive(FunctionArgsDerive)]
struct KVExistsArgs<T> {
    store_name: T,
    keys: T,
}

#[derive(FunctionArgsDerive)]
struct KVPutArgs<T> {
    store_name: T,
    key: T,
    value: T,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct KVGetWithStoreName;

// -----------------------------------------------------------------------------------------
// Helper Functions
// -----------------------------------------------------------------------------------------

#[cfg(feature = "python")]
fn call_python_kv_helper<F>(
    func_name: &str,
    result_name: &str,
    args_builder: F,
) -> DaftResult<Series>
where
    F: FnOnce(Python) -> DaftResult<pyo3::Py<pyo3::types::PyTuple>>,
{
    use daft_core::python::PySeries;
    Python::attach(|py| -> DaftResult<Series> {
        let daft_mod = py.import("daft.daft")?;
        let func = daft_mod.getattr(func_name)?;
        let args = args_builder(py)?;
        let py_res = func.call1(args)?;
        let py_series: pyo3::PyRef<PySeries> = py_res.extract()?;
        Ok(py_series.series.clone().rename(result_name))
    })
}

#[cfg(feature = "python")]
fn parse_on_error(on_error_series_opt: Option<&daft_core::series::Series>) -> DaftResult<String> {
    if let Some(on_error_series) = on_error_series_opt {
        if on_error_series.len() != 1 {
            return Err(DaftError::ValueError(
                "on_error series must contain exactly one element".to_string(),
            ));
        }
        Ok(on_error_series
            .utf8()?
            .get(0)
            .ok_or_else(|| DaftError::ValueError("Missing on_error".to_string()))?
            .to_string())
    } else {
        Ok("raise".to_string())
    }
}

#[cfg(feature = "python")]
fn parse_requested_columns(columns_series_opt: Option<&Series>) -> DaftResult<Option<Vec<String>>> {
    use daft_core::prelude::{DataType, Literal};
    if let Some(cols_ser) = columns_series_opt {
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
                                        DaftError::ValueError("Null column name".to_string())
                                    })?
                                    .to_string(),
                            );
                        }
                        Ok(Some(cols))
                    }
                    _ => Err(DaftError::ValueError(
                        "Expected List literal for columns".to_string(),
                    )),
                }
            }
            DataType::Binary => {
                let arr = cols_ser.binary()?;
                let bytes = arr.get(0).ok_or_else(|| {
                    DaftError::ValueError("columns must contain exactly one element".to_string())
                })?;
                let cols: Vec<String> = serde_json::from_slice(bytes).map_err(|e| {
                    DaftError::ValueError(format!("Failed to deserialize columns: {}", e))
                })?;
                Ok(Some(cols))
            }
            _ => Err(DaftError::TypeError(format!(
                "Invalid type for columns: {}",
                cols_ser.data_type()
            ))),
        }
    } else {
        Ok(None)
    }
}

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

        // Ensure store name is a scalar (single element)
        if store_name_series.len() != 1 {
            return Err(DaftError::ValueError(
                "Store name series must contain exactly one element".to_string(),
            ));
        }

        #[cfg(feature = "python")]
        {
            use daft_core::prelude::*;

            // Extract store name
            let name = store_name_series
                .utf8()?
                .get(0)
                .ok_or_else(|| DaftError::ValueError("Missing store name".to_string()))?;

            // 1. Parse Arguments
            let on_error = parse_on_error(on_error_series_opt.as_ref())?;
            let requested = parse_requested_columns(columns_series_opt.as_ref())?;

            // 2. Execute via Python bridge
            // Note: KVGetWithStoreName delegates to daft.daft.kv_get_direct_series which resolves the store from session by name
            let k_py = keys.cast(&DataType::Python)?;
            let _k_arr = k_py.downcast::<PythonArray>()?;

            call_python_kv_helper("kv_get_direct_series", keys.name(), |py| {
                use pyo3::IntoPyObject;
                let py_keys = pyo3::Py::new(
                    py,
                    daft_core::python::PySeries {
                        series: keys.clone(),
                    },
                )?;
                let args = match requested {
                    Some(cols) => (name, py_keys, on_error.as_str(), cols)
                        .into_pyobject(py)?
                        .unbind(),
                    None => (name, py_keys, on_error.as_str(), None::<Vec<String>>)
                        .into_pyobject(py)?
                        .unbind(),
                };
                Ok(args)
            })
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

// -----------------------------------------------------------------------------------------
// KVGetWithConfig Implementation
// -----------------------------------------------------------------------------------------
use crate::functions::kv::KVConfig;
#[cfg(feature = "python")]
use crate::functions::kv::lance::LanceKVStore;

#[derive(FunctionArgsDerive)]
struct KVGetWithConfigArgs<T> {
    keys: T,
    #[arg(optional)]
    on_error: Option<T>,
    #[arg(optional)]
    columns: Option<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct KVGetWithConfig {
    pub config: KVConfig,
}

#[typetag::serde]
impl ScalarUDF for KVGetWithConfig {
    fn name(&self) -> &'static str {
        "kv_get_with_config"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        #[cfg_attr(not(feature = "python"), allow(unused_variables))]
        let KVGetWithConfigArgs {
            keys,
            on_error: on_error_series_opt,
            columns: columns_series_opt,
        } = args.try_into()?;

        #[cfg(feature = "python")]
        {
            use common_py_serde::PyObjectWrapper;
            use daft_core::prelude::*;

            // 1. Parse Arguments
            let on_error = parse_on_error(on_error_series_opt.as_ref())?;
            let requested = parse_requested_columns(columns_series_opt.as_ref())?;

            match &self.config.lance {
                Some(cfg) => {
                    let store = LanceKVStore::new(
                        "worker_lance".to_string(),
                        cfg.uri.clone(),
                        cfg.key_column.clone().unwrap_or_else(|| "id".to_string()),
                        cfg.batch_size.unwrap_or(100),
                        cfg.io_config.clone(),
                    );

                    let k_py = keys.cast(&DataType::Python)?;
                    let k_arr = k_py.downcast::<PythonArray>()?;
                    let mut out: Vec<Literal> = Vec::with_capacity(k_arr.len());

                    Python::attach(|py| -> DaftResult<()> {
                        for i in 0..k_arr.len() {
                            let key_str = k_arr.str_value(i).unwrap_or_default();
                            let obj = store.get(py, &key_str)?;

                            // Convert to Dict/Value based on requested columns
                            let final_obj = if let Some(cols) = &requested {
                                let pydict = pyo3::types::PyDict::new(py);
                                let obj_bound = obj.bind(py);
                                if obj_bound.is_none() {
                                    if on_error == "null" {
                                        for f in cols {
                                            pydict.set_item(f, py.None())?;
                                        }
                                    } else {
                                        return Err(DaftError::ValueError(format!(
                                            "Key not found: {}",
                                            key_str
                                        )));
                                    }
                                } else if let Ok(dict) = obj_bound.cast::<pyo3::types::PyDict>() {
                                    for f in cols {
                                        let val = dict.call_method1("get", (f,))?;
                                        pydict.set_item(f, val)?;
                                    }
                                } else if cols.len() == 1 {
                                    pydict.set_item(&cols[0], obj.clone_ref(py))?;
                                }
                                pydict.into()
                            } else {
                                obj
                            };

                            out.push(Literal::Python(PyObjectWrapper(std::sync::Arc::new(
                                final_obj,
                            ))));
                        }
                        Ok(())
                    })?;

                    let mut s = Series::from_literals(out)?;
                    s = s.rename(keys.name());
                    Ok(s)
                }
                None => Err(DaftError::ValueError(
                    "KVGetWithConfig requires a Lance config".to_string(),
                )),
            }
        }
        #[cfg(not(feature = "python"))]
        {
            panic!("KVGetWithConfig requires python feature");
        }
    }

    fn get_return_field(
        &self,
        args: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<daft_core::datatypes::Field> {
        let KVGetWithConfigArgs { keys, .. } = args.try_into()?;
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
            panic!("KVGetWithConfig requires python feature");
        }
    }
}

#[derive(FunctionArgsDerive)]
struct KVBatchGetWithConfigArgs<T> {
    keys: T,
    batch_size: T,
    #[arg(optional)]
    on_error: Option<T>,
    #[arg(optional)]
    columns: Option<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct KVBatchGetWithConfig {
    pub config: KVConfig,
}

#[typetag::serde]
impl ScalarUDF for KVBatchGetWithConfig {
    fn name(&self) -> &'static str {
        "kv_batch_get_with_config"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        #[cfg_attr(not(feature = "python"), allow(unused_variables))]
        let KVBatchGetWithConfigArgs {
            keys,
            batch_size: batch_size_series,
            on_error: on_error_series_opt,
            columns: columns_series_opt,
        } = args.try_into()?;

        #[cfg(feature = "python")]
        {
            use common_py_serde::PyObjectWrapper;
            use daft_core::prelude::*;

            // 1. Parse Arguments
            let batch_size = batch_size_series
                .i64()?
                .get(0)
                .ok_or(DaftError::ValueError("Missing batch size".into()))?;
            let batch_size_usize = batch_size as usize;
            if batch_size <= 0 {
                return Err(DaftError::ValueError("batch_size must be positive".into()));
            }

            let on_error = parse_on_error(on_error_series_opt.as_ref())?;
            let requested = parse_requested_columns(columns_series_opt.as_ref())?;

            match &self.config.lance {
                Some(cfg) => {
                    // Use the configured batch_size from config for the store,
                    // but the UDF argument batch_size controls the outer loop.
                    let store = LanceKVStore::new(
                        "worker_lance".to_string(),
                        cfg.uri.clone(),
                        cfg.key_column.clone().unwrap_or_else(|| "id".to_string()),
                        cfg.batch_size.unwrap_or(100),
                        cfg.io_config.clone(),
                    );

                    let k_py = keys.cast(&DataType::Python)?;
                    let k_arr = k_py.downcast::<PythonArray>()?;
                    let total_len = k_arr.len();
                    let mut out: Vec<Literal> = Vec::with_capacity(total_len);

                    Python::attach(|py| -> DaftResult<()> {
                        // Iterate in chunks
                        for chunk_start in (0..total_len).step_by(batch_size_usize) {
                            let chunk_end =
                                std::cmp::min(chunk_start + batch_size_usize, total_len);
                            for i in chunk_start..chunk_end {
                                let key_str = k_arr.str_value(i).unwrap_or_default();
                                let obj = store.get(py, &key_str)?;

                                let final_obj = if let Some(cols) = &requested {
                                    let pydict = pyo3::types::PyDict::new(py);
                                    let obj_bound = obj.bind(py);
                                    if obj_bound.is_none() {
                                        if on_error == "null" {
                                            for f in cols {
                                                pydict.set_item(f, py.None())?;
                                            }
                                        } else {
                                            return Err(DaftError::ValueError(format!(
                                                "Key not found: {}",
                                                key_str
                                            )));
                                        }
                                    } else if let Ok(dict) = obj_bound.cast::<pyo3::types::PyDict>()
                                    {
                                        for f in cols {
                                            let val = dict.call_method1("get", (f,))?;
                                            pydict.set_item(f, val)?;
                                        }
                                    } else if cols.len() == 1 {
                                        pydict.set_item(&cols[0], obj.clone_ref(py))?;
                                    }
                                    pydict.into()
                                } else {
                                    obj
                                };
                                out.push(Literal::Python(PyObjectWrapper(std::sync::Arc::new(
                                    final_obj,
                                ))));
                            }
                        }
                        Ok(())
                    })?;

                    let mut s = Series::from_literals(out)?;
                    s = s.rename(keys.name());
                    Ok(s)
                }
                None => Err(DaftError::ValueError(
                    "KVBatchGetWithConfig requires a Lance config".to_string(),
                )),
            }
        }
        #[cfg(not(feature = "python"))]
        {
            panic!("KVBatchGetWithConfig requires python feature");
        }
    }

    fn get_return_field(
        &self,
        args: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<daft_core::datatypes::Field> {
        let KVBatchGetWithConfigArgs { keys, .. } = args.try_into()?;
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
            panic!("KVBatchGetWithConfig requires python feature");
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

            let on_error = parse_on_error(on_error_series_opt.as_ref())?;
            let requested = parse_requested_columns(columns_series_opt.as_ref())?;

            let k_py = keys.cast(&DataType::Python)?;
            let _k_arr = k_py.downcast::<PythonArray>()?;

            call_python_kv_helper("kv_batch_get_direct_series", keys.name(), |py| {
                use pyo3::IntoPyObject;
                let py_keys = pyo3::Py::new(
                    py,
                    daft_core::python::PySeries {
                        series: keys.clone(),
                    },
                )?;
                let args = match requested {
                    Some(cols) => (name, py_keys, batch_size_usize, on_error.as_str(), cols)
                        .into_pyobject(py)?
                        .unbind(),
                    None => (
                        name,
                        py_keys,
                        batch_size_usize,
                        on_error.as_str(),
                        None::<Vec<String>>,
                    )
                        .into_pyobject(py)?
                        .unbind(),
                };
                Ok(args)
            })
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
        #[cfg_attr(not(feature = "python"), allow(unused_variables))]
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
            call_python_kv_helper("kv_exists_direct_series", keys.name(), |py| {
                use pyo3::IntoPyObject;
                let py_keys = pyo3::Py::new(
                    py,
                    daft_core::python::PySeries {
                        series: keys.clone(),
                    },
                )?;
                Ok((name, py_keys).into_pyobject(py)?.unbind())
            })
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

        let keys = keys.to_field(schema)?;

        Ok(daft_core::datatypes::Field::new(
            keys.name,
            daft_core::datatypes::DataType::Boolean,
        ))
    }
}

#[derive(FunctionArgsDerive)]
struct KVExistsWithConfigArgs<T> {
    keys: T,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct KVExistsWithConfig {
    pub config: KVConfig,
}

#[typetag::serde]
impl ScalarUDF for KVExistsWithConfig {
    fn name(&self) -> &'static str {
        "kv_exists_with_config"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        #[cfg_attr(not(feature = "python"), allow(unused_variables))]
        let KVExistsWithConfigArgs { keys } = args.try_into()?;

        #[cfg(feature = "python")]
        {
            use daft_core::prelude::*;

            match &self.config.lance {
                Some(cfg) => {
                    let store = LanceKVStore::new(
                        "worker_lance".to_string(),
                        cfg.uri.clone(),
                        cfg.key_column.clone().unwrap_or_else(|| "id".to_string()),
                        cfg.batch_size.unwrap_or(100),
                        cfg.io_config.clone(),
                    );

                    let k_py = keys.cast(&DataType::Python)?;
                    let k_arr = k_py.downcast::<PythonArray>()?;
                    let mut out: Vec<Literal> = Vec::with_capacity(k_arr.len());

                    Python::attach(|py| -> DaftResult<()> {
                        for i in 0..k_arr.len() {
                            let key_str = k_arr.str_value(i).unwrap_or_default();
                            let obj = store.get(py, &key_str)?;
                            let exists = !obj.is_none(py);
                            out.push(Literal::Boolean(exists));
                        }
                        Ok(())
                    })?;

                    let mut s = Series::from_literals(out)?;
                    s = s.rename(keys.name());
                    Ok(s)
                }
                None => Err(DaftError::ValueError(
                    "KVExistsWithConfig requires a Lance config".to_string(),
                )),
            }
        }
        #[cfg(not(feature = "python"))]
        {
            panic!("KVExistsWithConfig requires python feature");
        }
    }

    fn get_return_field(
        &self,
        args: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<daft_core::datatypes::Field> {
        let KVExistsWithConfigArgs { keys } = args.try_into()?;
        let keys = keys.to_field(schema)?;
        Ok(daft_core::datatypes::Field::new(
            keys.name,
            daft_core::datatypes::DataType::Boolean,
        ))
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
            let name = store_name_series
                .utf8()?
                .get(0)
                .ok_or_else(|| DaftError::ValueError("Missing store name".to_string()))?;

            // Build PySeries for key and value
            call_python_kv_helper("kv_put_direct_series", "result", |py| {
                use pyo3::IntoPyObject;
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
                Ok((name, py_key, py_value).into_pyobject(py)?.unbind())
            })
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

pub fn kv_get_with_config(
    config: KVConfig,
    keys: ExprRef,
    on_error: ExprRef,
    columns: Option<ExprRef>,
) -> ExprRef {
    use crate::functions::scalar::ScalarFn;
    let udf = KVGetWithConfig { config };
    let mut args = vec![keys, on_error];
    if let Some(c) = columns {
        args.push(c);
    }
    ScalarFn::builtin(udf, args).into()
}

pub fn kv_batch_get_with_config(
    config: KVConfig,
    keys: ExprRef,
    batch_size: ExprRef,
    on_error: ExprRef,
    columns: Option<ExprRef>,
) -> ExprRef {
    use crate::functions::scalar::ScalarFn;
    let udf = KVBatchGetWithConfig { config };
    let mut args = vec![keys, batch_size, on_error];
    if let Some(c) = columns {
        args.push(c);
    }
    ScalarFn::builtin(udf, args).into()
}

pub fn kv_get_with_name(
    name: ExprRef,
    keys: ExprRef,
    on_error: ExprRef,
    columns: Option<ExprRef>,
) -> ExprRef {
    use crate::functions::scalar::ScalarFn;
    let mut args = vec![name, keys, on_error];
    if let Some(c) = columns {
        args.push(c);
    }
    ScalarFn::builtin(KVGetWithStoreName, args).into()
}

pub fn kv_batch_get_with_name(
    name: ExprRef,
    keys: ExprRef,
    batch_size: ExprRef,
    on_error: ExprRef,
    columns: Option<ExprRef>,
) -> ExprRef {
    use crate::functions::scalar::ScalarFn;
    let mut args = vec![name, keys, batch_size, on_error];
    if let Some(c) = columns {
        args.push(c);
    }
    ScalarFn::builtin(KVBatchGetWithStoreName, args).into()
}

pub fn kv_exists_with_config(config: KVConfig, keys: ExprRef) -> ExprRef {
    use crate::functions::scalar::ScalarFn;
    let udf = KVExistsWithConfig { config };
    ScalarFn::builtin(udf, vec![keys]).into()
}

pub fn kv_exists_with_name(name: ExprRef, keys: ExprRef) -> ExprRef {
    use crate::functions::scalar::ScalarFn;
    ScalarFn::builtin(KVExistsWithStoreName, vec![name, keys]).into()
}

pub fn kv_put_with_name(name: ExprRef, key: ExprRef, value: ExprRef) -> ExprRef {
    use crate::functions::scalar::ScalarFn;
    ScalarFn::builtin(KVPutWithStoreName, vec![name, key, value]).into()
}
